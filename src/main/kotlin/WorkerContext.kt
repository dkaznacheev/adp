class WorkerContext(
    val thisWorker: Int,
    val workers: List<Int>,
    val shuffleManager: ShuffleManager,
    val grpcShuffleManager: GrpcShuffleManager,
    val cache: CacheManager)