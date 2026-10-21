#include "async_channel.h"
#include <assert.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>

/*
 * The elements are stored in the slots of a linked list of blocks.
 *
 * A producer claims a slot by advancing tail_state, fills it and then marks it
 * as written. The consumer walks the slots in order and reads a slot only
 * after it is marked as written, so it parks on the first slot which is not.
 *
 * The slots are numbered across the blocks. tail_state is the number of the
 * next slot to claim shifted left by two, the lowest bit is set when the
 * channel is closed and the next one while the producer which claimed the last
 * slot of a block is installing the next block. The consumer keeps the index
 * in its block and the number of the consumed slots apart, the index reaches
 * BLOCK_CAPACITY after the last slot of the block is consumed and the block is
 * released on the next call.
 *
 * Backpressure: the number of elements in the channel is the number of the
 * claimed slots minus the number of the consumed ones. A producer which takes
 * it to the high watermark switches the channel to the backpressured mode, the
 * consumer which takes it down to the low watermark switches it back and
 * resumes the waiting producers. The mode and the waiting producers share one
 * word, so a producer can not get queued after the waiters were resumed.
 * Both sides re-check the count of the other side after the switch
 * (store -> load, all seq_cst), so the mode can not get stuck.
 */

/* has to be 2^N, so the index is masked out of the number of the slot */
#if !defined(BLOCK_CAPACITY)
#define BLOCK_CAPACITY 32
#endif
_Static_assert((BLOCK_CAPACITY & (BLOCK_CAPACITY - 1)) == 0, "BLOCK_CAPACITY has to be 2^N");

#define TAIL_CLOSED ((uint64_t) 1)
#define TAIL_LOCKED ((uint64_t) 2)
#define TAIL_STEP ((uint64_t) 4)

/* tail_state is 64-bit on every platform, so it never comes back to a value a
   stalled producer loaded and its CAS can not succeed on a stale tail_block */
_Static_assert(ATOMIC_LLONG_LOCK_FREE == 2, "64-bit atomics have to be lock-free");

#define SLOT_EMPTY 0
#define SLOT_CONSUMER_PARKED 1
#define SLOT_WRITTEN 2
#define SLOT_CLOSED 3

/* waiters word: one of these or a pointer to the stack of the waiters */
#define WAITERS_NONE ((uintptr_t) 0)    /* not backpressured */
#define WAITERS_EMPTY ((uintptr_t) 1)   /* backpressured, nobody waits */
#define WAITERS_CLOSED ((uintptr_t) 2)  /* closed, nobody waits any more */

#define WAITER_INIT 0
#define WAITER_WAITING 1
#define WAITER_RESUMED 2
#define WAITER_CANCELLED 3

#define CACHE_LINE 128

static inline void spin_wait(void) {
#if defined(__aarch64__) || defined(__arm__)
    __asm__ __volatile__("yield");
#elif defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#endif
}

struct block_s {
    struct block_s * _Atomic next;
    /* the slots follows at async_channel->slots_offset */
};


struct slot_s {
    atomic_uchar state;
    /* the element follows at async_channel->data_offset */
};


/* the data of the user follows, the alignment keeps it aligned as malloc does */
struct producer_waiter_s {
    _Alignas(max_align_t) size_t _Atomic ref_count;
    struct producer_waiter_s * next;
    atomic_uchar state;
};


struct async_channel_s {
    size_t data_offset;
    size_t slot_size;
    size_t slots_offset;
    size_t block_size;
    size_t block_alignment;
    void (*resume_consumer)(void * consumer_data);
    uint64_t low_watermark;
    uint64_t high_watermark;
    void (*resume_producer)(void * thunk, int outcome);

    /* producers */
    _Alignas(CACHE_LINE) _Atomic uint64_t tail_state;
    struct block_s * _Atomic tail_block;

    /* consumer */
    _Alignas(CACHE_LINE) struct block_s * head_block;
    size_t head_index;
    uint64_t head_count;

    /* backpressure: consumed is written by the consumer for every element and
       read by the producers, waiters is changed only when the mode switches */
    _Alignas(CACHE_LINE) _Atomic uint64_t consumed;
    _Alignas(CACHE_LINE) _Atomic uintptr_t waiters;

    /* the consumer data follows on its own cache line, the size of the
       structure is a multiple of its alignment */
};

_Static_assert(_Alignof(struct async_channel_s) >= _Alignof(max_align_t), "the consumer data has to be aligned as malloc does");


static inline size_t round_up(size_t value, size_t alignment) {
    return (value + alignment - 1) & ~(alignment - 1);
}


/* the number of the slots claimed before the state */
static inline uint64_t tail_get_count(uint64_t tail_state) {
    return (tail_state / TAIL_STEP);
}


static inline size_t tail_get_index(uint64_t tail_state) {
    return (size_t) (tail_get_count(tail_state) & (BLOCK_CAPACITY - 1));
}


static inline struct slot_s * get_slot(
    async_channel_t * async_channel,
    struct block_s * block,
    size_t index
) {
    char * ptr = (char*) block;
    ptr += async_channel->slots_offset;
    ptr += (index * async_channel->slot_size);
    return (struct slot_s*) ptr;
}


static inline struct slot_s * get_slot_by_data(async_channel_t * async_channel, void * ptr) {
    return (struct slot_s*) (((char*) ptr) - async_channel->data_offset);
}


static inline void * slot_get_data(async_channel_t * async_channel, struct slot_s * slot) {
    return ((char*) slot) + async_channel->data_offset;
}


static inline struct producer_waiter_s * get_producer_waiter_by_data(void * ptr) {
    ptr = (((char*)ptr) - sizeof(struct producer_waiter_s));
    return ptr;
}


static struct block_s * block_create(async_channel_t * async_channel) {
    struct block_s * block;
    if (async_channel->block_alignment <= _Alignof(max_align_t)) {
        block = malloc(async_channel->block_size);
    } else {
        block = aligned_alloc(async_channel->block_alignment, async_channel->block_size);
    }
    if (block == NULL) {
        /* there is no way to report it once the slot is about to be claimed */
        abort();
    }
    /* only the slot states are initialized, the elements are written by the producers */
    atomic_init(&block->next, NULL);
    for (size_t idx=0; idx<BLOCK_CAPACITY; idx++) {
        struct slot_s * slot = get_slot(async_channel, block, idx);
        atomic_init(&slot->state, SLOT_EMPTY);
    }
    return block;
}


/* high_watermark == 0 disables the backpressure. */
async_channel_t * async_channel_create(
    size_t element_size,
    size_t element_alignment,
    size_t consumer_data_size,
    void ** consumer_data,
    void (*resume_consumer)(void * consumer_data),
    uint64_t low_watermark,
    uint64_t high_watermark,
    void (*resume_producer)(void * waiter_thunk, int outcome)
) {
    assert(element_alignment != 0 && (element_alignment & (element_alignment - 1)) == 0);
    assert(high_watermark == 0 || low_watermark < high_watermark);
    struct async_channel_s * async_channel = aligned_alloc(
        _Alignof(struct async_channel_s),
        round_up(sizeof(struct async_channel_s) + consumer_data_size, _Alignof(struct async_channel_s))
    );
    if (async_channel == NULL) {
        return NULL;
    }
    size_t block_alignment = element_alignment > _Alignof(struct block_s)
        ? element_alignment
        : _Alignof(struct block_s);
    size_t data_offset = round_up(sizeof(struct slot_s), element_alignment);
    size_t slot_size = round_up(data_offset + element_size, element_alignment);
    size_t slots_offset = round_up(sizeof(struct block_s), element_alignment);
    async_channel->data_offset = data_offset;
    async_channel->slot_size = slot_size;
    async_channel->slots_offset = slots_offset;
    async_channel->block_size = round_up(slots_offset + BLOCK_CAPACITY * slot_size, block_alignment);
    async_channel->block_alignment = block_alignment;
    async_channel->resume_consumer = resume_consumer;
    async_channel->low_watermark = low_watermark;
    async_channel->high_watermark = high_watermark;
    async_channel->resume_producer = resume_producer;

    struct block_s * block = block_create(async_channel);
    atomic_init(&async_channel->tail_state, 0);
    atomic_init(&async_channel->tail_block, block);
    async_channel->head_block = block;
    async_channel->head_index = 0;
    async_channel->head_count = 0;
    atomic_init(&async_channel->consumed, 0);
    atomic_init(&async_channel->waiters, WAITERS_NONE);
    *consumer_data = (async_channel + 1);
    return async_channel;
}


/* Switches the waiters word to new_state (WAITERS_NONE or WAITERS_CLOSED)
   unless it is closed already, and resumes the waiters which were queued. */
static void resume_waiters(async_channel_t * async_channel, uintptr_t new_state) {
    uintptr_t waiters = atomic_load(&async_channel->waiters);
    for (;;) {
        if ((waiters == WAITERS_CLOSED) || (waiters == new_state)) {
            return;
        }
        if (atomic_compare_exchange_weak(&async_channel->waiters, &waiters, new_state)) {
            break;
        }
    }
    int outcome = (new_state == WAITERS_CLOSED) ? ASYNC_CHANNEL_WAITER_CLOSED : ASYNC_CHANNEL_WAITER_DEMAND;
    struct producer_waiter_s * waiter = (waiters == WAITERS_NONE || waiters == WAITERS_EMPTY)
        ? NULL
        : (struct producer_waiter_s*) waiters;
    while (waiter != NULL) {
        struct producer_waiter_s * next = waiter->next;
        unsigned char expected = WAITER_WAITING;
        if (atomic_compare_exchange_strong(&waiter->state, &expected, WAITER_RESUMED)) {
            async_channel->resume_producer(waiter + 1, outcome);
        }
        size_t ref_count = (atomic_fetch_sub(&waiter->ref_count, 1) - 1);
        if (ref_count == 0) {
            free(waiter);
        }
        waiter = next;
    }
}


/* Returns NULL if the channel is closed.
   position receives the number of elements claimed so far including this one. */
void * async_channel_reserve(async_channel_t * async_channel, uint64_t * position) {
    struct block_s * next_block = NULL;
    /* tail_block is always loaded after tail_state: it is the block of that
       state as long as the state is not changed, what is checked by the CAS */
    uint64_t tail_state = atomic_load(&async_channel->tail_state);
    struct block_s * block = atomic_load(&async_channel->tail_block);
    for (;;) {
        if (tail_state & TAIL_CLOSED) {
            free(next_block);
            return NULL;
        }
        if (tail_state & TAIL_LOCKED) {
            /* the next block is being installed */
            spin_wait();
            tail_state = atomic_load(&async_channel->tail_state);
            block = atomic_load(&async_channel->tail_block);
            continue;
        }
        size_t index = tail_get_index(tail_state);
        uint64_t new_tail_state = (tail_state + TAIL_STEP);
        int last = (index + 1 == BLOCK_CAPACITY);
        if (last) {
            new_tail_state |= TAIL_LOCKED;
            if (next_block == NULL) {
                next_block = block_create(async_channel);
            }
        }
        if (atomic_compare_exchange_strong(&async_channel->tail_state, &tail_state, new_tail_state)) {
            if (last) {
                /* the last slot is claimed, so nobody else changes the tail
                   until it is unlocked */
                atomic_store(&async_channel->tail_block, next_block);
                atomic_store(&async_channel->tail_state, new_tail_state & ~TAIL_LOCKED);
                /* is set before the last slot is written, so the consumer
                   always finds it when it leaves the block */
                atomic_store(&block->next, next_block);
            } else {
                free(next_block);
            }
            *position = tail_get_count(tail_state) + 1;
            struct slot_s * slot = get_slot(async_channel, block, index);
            return slot_get_data(async_channel, slot);
        }
        block = atomic_load(&async_channel->tail_block);
    }
}


/* Returns nonzero if the producers have to wait for the demand. */
int async_channel_send(async_channel_t * async_channel, void * ptr, uint64_t position) {
    struct slot_s * slot = get_slot_by_data(async_channel, ptr);
    if (atomic_exchange(&slot->state, SLOT_WRITTEN) == SLOT_CONSUMER_PARKED) {
        async_channel->resume_consumer(async_channel + 1);
    }
    if (async_channel->high_watermark == 0) {
        return 0;
    }
    uintptr_t waiters = atomic_load(&async_channel->waiters);
    if (waiters != WAITERS_NONE) {
        /* backpressured already, or closed and the next send fails anyway */
        return (waiters != WAITERS_CLOSED);
    }
    /* the count as it was when the slot was claimed */
    uint64_t consumed = atomic_load(&async_channel->consumed);
    if (position < (consumed + async_channel->high_watermark)) {
        return 0;
    }
    if (!atomic_compare_exchange_strong(&async_channel->waiters, &waiters, WAITERS_EMPTY)) {
        return (waiters != WAITERS_CLOSED);
    }
    /* the consumer could get to the low watermark before the switch and miss it,
       consumed is loaded first so the claimed count can not be below it */
    consumed = atomic_load(&async_channel->consumed);
    uint64_t count = tail_get_count(atomic_load(&async_channel->tail_state));
    if (count - consumed <= async_channel->low_watermark) {
        resume_waiters(async_channel, WAITERS_NONE);
        return 0;
    }
    return 1;
}


void async_channel_finish(async_channel_t * async_channel) {
    uint64_t tail_state = atomic_load(&async_channel->tail_state);
    struct block_s * block = atomic_load(&async_channel->tail_block);
    for (;;) {
        if (tail_state & TAIL_CLOSED) {
            return;
        }
        if (tail_state & TAIL_LOCKED) {
            /* the next block is being installed */
            spin_wait();
            tail_state = atomic_load(&async_channel->tail_state);
            block = atomic_load(&async_channel->tail_block);
            continue;
        }
        if (atomic_compare_exchange_strong(&async_channel->tail_state, &tail_state, tail_state | TAIL_CLOSED)) {
            /* the first unclaimed slot can not be claimed any more and marks the end */
            struct slot_s * slot = get_slot(async_channel, block, tail_get_index(tail_state));
            if (atomic_exchange(&slot->state, SLOT_CLOSED) == SLOT_CONSUMER_PARKED) {
                async_channel->resume_consumer(async_channel + 1);
            }
            resume_waiters(async_channel, WAITERS_CLOSED);
            return;
        }
        block = atomic_load(&async_channel->tail_block);
    }
}


void * async_channel_next(async_channel_t * async_channel) {
    struct block_s * block = async_channel->head_block;
    size_t index = async_channel->head_index;
    if (index == BLOCK_CAPACITY) {
        /* the block is released only now to keep the last returned element alive */
        struct block_s * next = atomic_load(&block->next);
        assert(next != NULL);
        free(block);
        block = next;
        index = 0;
        async_channel->head_block = block;
        async_channel->head_index = index;
    }
    struct slot_s * slot = get_slot(async_channel, block, index);
    unsigned char slot_state = atomic_load(&slot->state);
    if (slot_state == SLOT_WRITTEN) {
        async_channel->head_index = index + 1;
        uint64_t consumed = ++async_channel->head_count;
        if (async_channel->high_watermark != 0) {
            atomic_store(&async_channel->consumed, consumed);
            /* pairs with the switch to the backpressured mode in async_channel_send */
            uintptr_t waiters = atomic_load(&async_channel->waiters);
            if ((waiters != WAITERS_NONE) && (waiters != WAITERS_CLOSED)) {
                uint64_t count = tail_get_count(atomic_load(&async_channel->tail_state));
                if (count - consumed <= async_channel->low_watermark) {
                    resume_waiters(async_channel, WAITERS_NONE);
                }
            }
        }
        return slot_get_data(async_channel, slot);
    } else if (slot_state == SLOT_CLOSED) {
        return async_channel;
    } else {
        return NULL;
    }
}


/* Has to be called after async_channel_next returned NULL.
   Returns nonzero if the consumer is parked and resume_consumer will be called,
   0 if async_channel_next has something to return. */
int async_channel_consumer_try_park(async_channel_t * async_channel) {
    size_t index = async_channel->head_index;
    assert(index < BLOCK_CAPACITY);
    struct slot_s * slot = get_slot(async_channel, async_channel->head_block, index);
    unsigned char expected = SLOT_EMPTY;
    return atomic_compare_exchange_strong(&slot->state, &expected, SLOT_CONSUMER_PARKED);
}


/* Has to be called after async_channel_consumer_try_park() returned nonzero and before the consumer is resumed.
   Returns nonzero if the consumer is unparked and resume_consumer will not be called,
   0 if resume_consumer is called (or about to be called) instead. */
int async_channel_consumer_try_unpark(async_channel_t * async_channel) {
    size_t index = async_channel->head_index;
    struct slot_s * slot = get_slot(async_channel, async_channel->head_block, index);
    unsigned char expected = SLOT_CONSUMER_PARKED;
    return atomic_compare_exchange_strong(&slot->state, &expected, SLOT_EMPTY);
}


void async_channel_destroy(async_channel_t * async_channel) {
    /* a queued waiter keeps a producer, so the channel is closed or nobody waits */
    uintptr_t waiters = atomic_load(&async_channel->waiters);
    assert(waiters == WAITERS_NONE || waiters == WAITERS_EMPTY || waiters == WAITERS_CLOSED);
    (void) waiters;
    struct block_s * block = async_channel->head_block;
    while (block != NULL) {
        struct block_s * next = atomic_load(&block->next);
        free(block);
        block = next;
    }
    free(async_channel);
}


/* DEMAND, CLOSED or QUEUED (the channel is backpressured at the moment). */
int async_channel_demand(async_channel_t * async_channel) {
    uintptr_t waiters = atomic_load(&async_channel->waiters);
    if (waiters == WAITERS_NONE) {
        return ASYNC_CHANNEL_WAIT_DEMAND;
    } else if (waiters == WAITERS_CLOSED) {
        return ASYNC_CHANNEL_WAIT_CLOSED;
    } else {
        return ASYNC_CHANNEL_WAIT_QUEUED;
    }
}


void * async_channel_producer_waiter_create(size_t data_size) {
    struct producer_waiter_s * waiter = malloc(sizeof(struct producer_waiter_s) + data_size);
    if (waiter == NULL) {
        return NULL;
    }
    atomic_init(&waiter->ref_count, 1);
    waiter->next = NULL;
    atomic_init(&waiter->state, WAITER_INIT);
    return (waiter + 1);
}


void async_channel_producer_waiter_release(void * data) {
    struct producer_waiter_s * waiter = get_producer_waiter_by_data(data);
    size_t ref_count = atomic_fetch_sub(&waiter->ref_count, 1) - 1;
    if (ref_count == 0) {
        free(waiter);
    }
}

/* Queues the waiter until the low watermark is reached, its data is passed to resume_producer.
   The queue holds a reference to the waiter while it is queued.
   The waiter is resumed by whoever moves it out of WAITER_WAITING:
   async_channel_producer_try_park itself, resume_waiters or async_channel_producer_waiter_cancel. */
int async_channel_producer_try_park(async_channel_t * async_channel, void * data) {
    struct producer_waiter_s * waiter = get_producer_waiter_by_data(data);
    unsigned char expected = WAITER_INIT;
    if (!atomic_compare_exchange_strong(&waiter->state, &expected, WAITER_WAITING)) {
        return ASYNC_CHANNEL_WAIT_CANCELLED;
    }
    /* taken before the waiter is published, resume_waiters can drop it right away */
    atomic_fetch_add(&waiter->ref_count, 1);
    uintptr_t waiters = atomic_load(&async_channel->waiters);
    for (;;) {
        if ((waiters == WAITERS_NONE) || (waiters == WAITERS_CLOSED)) {
            /* not queued, drop the reference of the queue */
            size_t ref_count = (atomic_fetch_sub(&waiter->ref_count, 1) - 1);
            assert(ref_count == 1);
            (void) ref_count;
            expected = WAITER_WAITING;
            if (!atomic_compare_exchange_strong(&waiter->state, &expected, WAITER_RESUMED)) {
                assert(expected == WAITER_CANCELLED);
                return ASYNC_CHANNEL_WAIT_RACED;
            }
            return (waiters == WAITERS_NONE) ? ASYNC_CHANNEL_WAIT_DEMAND : ASYNC_CHANNEL_WAIT_CLOSED;
        } else {
            waiter->next = (waiters == WAITERS_EMPTY) ? NULL : (struct producer_waiter_s*) waiters;
            if (atomic_compare_exchange_weak(&async_channel->waiters, &waiters, (uintptr_t) waiter)) {
                return ASYNC_CHANNEL_WAIT_QUEUED;
            }
        }
    }
}


/* Returns nonzero if the waiter is queued and the caller has to resume it. */
int async_channel_producer_waiter_cancel(void * data) {
    struct producer_waiter_s * waiter = get_producer_waiter_by_data(data);
    unsigned char expected = WAITER_INIT;
    if (atomic_compare_exchange_strong(&waiter->state, &expected, WAITER_CANCELLED)) {
        return 0;
    }
    if (expected == WAITER_WAITING) {
        return atomic_compare_exchange_strong(&waiter->state, &expected, WAITER_CANCELLED);
    }
    return 0;
}
