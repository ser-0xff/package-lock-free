import LockFree

func asyncChannelEndToEnd(count: Int) async -> (dispatch: Duration, endToEnd: Duration) {
    let channel = AsyncChannel<Int>()
    let sink = channel.makeSink()
    let consumer = Task {
        var sum = 0
        for _ in 0..<count {
            sum &+= await channel.next()!
        }
        return (end: ContinuousClock.now, sum: sum)
    }
    let producer = Task {
        let start = ContinuousClock.now
        for i in 0..<count {
            sink.send(i)
        }
        return (start: start, end: ContinuousClock.now)
    }
    let produced = await producer.value
    let consumed = await consumer.value
    precondition(consumed.sum == expectedSum(count: count))
    return (dispatch: produced.end - produced.start, endToEnd: consumed.end - produced.start)
}
