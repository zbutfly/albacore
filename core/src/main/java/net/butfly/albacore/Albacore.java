package net.butfly.albacore;

public interface Albacore {
	interface Props {
		final String PROP_CONFIG_EXTENSION = "albacore.config.ext";
		final String PROP_DEBUG_SUFFIX = "albacore.debug.suffix";
		final String PROP_GC_INTERVAL_MS = "albacore.gc.interval.ms";// 0
		final String PROP_APP_DAEMON = "albacore.app.daemon";// false
		final String PROP_APP_FORK_HARD = "albacore.app.fork.hard";// true
		final String PROP_APP_FORK_VM_ARGS = "albacore.app.vmconfig";
		final String PROP_CACHE_LOCAL_PATH = "albacore.cache.local.path";// "./cache/"
		final String PROP_STREAM_PARALLES = "albacore.parallel.stream.enable";// true
		final String PROP_CURRENCE_FORKJOIN = "albacore.concurrent.forkjoin";// true
		final String PROP_CURRENCE = "albacore.concurrence";// 0
		final String PROP_PARALLEL_FACTOR = "albacore.parallel.factor";// 1 //"albacore.io.stream.parallelism.factor"
		final String PROP_PARALLEL_POOL_SIZE_OBJECT = "albacore.parallel.object.cache.size";
		final String PROP_PARALLEL_POOL_SIZE_LOCKER = "albacore.parallel.pool.number.locker";
		final String PROP_PARALLEL_POOL_SIZE_LOCKER_FAIR = "albacore.parallel.pool.number.locker.fair";
		final String PROP_TEXT_DATE_FORMAT = "albacore.text.date.format";
	}
}
