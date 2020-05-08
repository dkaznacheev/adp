class WorkerContext(
    val shuffleManager: ShuffleManager,
    val grpcShuffleManager: GrpcShuffleManager,
    val cache: CacheManager) {
    var workerId: String? = null
}