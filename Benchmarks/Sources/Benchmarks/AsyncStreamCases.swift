func asyncStreamEndToEnd(count: Int) async -> (dispatch: Duration, endToEnd: Duration) {
    let (stream, continuation) = AsyncStream<Int>.makeStream(bufferingPolicy: .unbounded)
    let consumer = Task {
        var iterator = stream.makeAsyncIterator()
        var sum = 0
        for _ in 0..<count {
            sum &+= await iterator.next()!
        }
        return (end: ContinuousClock.now, sum: sum)
    }
    let producer = Task {
        let start = ContinuousClock.now
        for i in 0..<count {
            continuation.yield(i)
        }
        return (start: start, end: ContinuousClock.now)
    }
    let produced = await producer.value
    let consumed = await consumer.value
    precondition(consumed.sum == expectedSum(count: count))
    return (dispatch: produced.end - produced.start, endToEnd: consumed.end - produced.start)
}
