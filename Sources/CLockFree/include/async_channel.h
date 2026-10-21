#if !defined(_ASYNC_CHANNEL_H)
#define _ASYNC_CHANNEL_H

#include <stddef.h>
#include <stdint.h>

typedef struct async_channel_s async_channel_t;

/* Outcomes passed to resume_producer. */
#define ASYNC_CHANNEL_WAITER_DEMAND 0   /* resume: the low watermark is reached */
#define ASYNC_CHANNEL_WAITER_CLOSED 1   /* resume: the channel is closed */

/* consumer_data receives the storage of consumer_data_size bytes allocated along
   with the channel (aligned as malloc does), it is passed to resume_consumer. */
async_channel_t * async_channel_create(
    size_t element_size,
    size_t element_alignment,
    size_t consumer_data_size,
    void ** consumer_data,
    void (*resume_consumer)(void * consumer_data),
    uint64_t low_watermark,
    uint64_t high_watermark,
    void (*resume_producer)(void * waiter_thunk, int outcome)
);

void * async_channel_reserve(async_channel_t * async_channel, uint64_t * position);
int async_channel_send(async_channel_t * async_channel, void * ptr, uint64_t position);
void * async_channel_next(async_channel_t * async_channel);
int async_channel_consumer_try_park(async_channel_t * async_channel);
int async_channel_consumer_try_unpark(async_channel_t * async_channel);
void async_channel_finish(async_channel_t * async_channel);
void async_channel_destroy(async_channel_t * async_channel);

/* Results of async_channel_demand and async_channel_producer_try_park. */
#define ASYNC_CHANNEL_WAIT_DEMAND 0     /* not waiting: the channel is not backpressured */
#define ASYNC_CHANNEL_WAIT_CLOSED 1     /* not waiting: the channel is closed */
#define ASYNC_CHANNEL_WAIT_QUEUED 2     /* waiting: resume_producer will be called */
#define ASYNC_CHANNEL_WAIT_CANCELLED 3  /* not waiting: cancelled before, the caller resumes */
#define ASYNC_CHANNEL_WAIT_RACED 4      /* not waiting: cancelled meanwhile, the canceller resumes */

int async_channel_demand(async_channel_t * async_channel);

void * async_channel_producer_waiter_create(size_t data_size);
void async_channel_producer_waiter_release(void * data);
int async_channel_producer_try_park(async_channel_t * async_channel, void * data);
int async_channel_producer_waiter_cancel(void * data);

/* Once the channel is closed and drained async_channel_next returns the channel itself. */
static inline int async_channel_is_closed(async_channel_t * async_channel, void * ptr) {
    return (ptr == (void*) async_channel);
}

#endif /* !defined(_ASYNC_CHANNEL_H) */
