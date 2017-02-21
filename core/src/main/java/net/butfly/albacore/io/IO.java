package net.butfly.albacore.io;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Logger;

public interface IO extends Sizable, Openable {
	class Context {
		private static final Logger logger = Logger.getLogger(Context.class);
		private static final int IO_PARALLELISM = calcIoParallelism();

		private static int calcIoParallelism() {
			if (Configs.MAIN_CONF.containsKey("albacore.io.parallelism.ratio")) {
				double r = Double.parseDouble(Configs.MAIN_CONF.get("albacore.io.parallelism.ratio"));
				int p = 16 + (int) Math.round((ForkJoinPool.getCommonPoolParallelism() - 16) * (r - 1));
				if (p < 2) p = 2;
				logger.info("AlbacoreIO parallelism calced as: " + p + " [from: (((-Dalbacore.io.parallelism.ratio(" + r
						+ ")) - 1) * (JVM_DEFAULT_PARALLELISM(" + ForkJoinPool.getCommonPoolParallelism()
						+ ") - IO_DEFAULT_PARALLELISM(16))) + IO_DEFAULT_PARALLELISM(16), Max=JVM_DEFAULT_PARALLELISM, Min=2 ]");
				return p;
			} else {
				logger.info("AlbacoreIO use JVM common ForkJoinPool.");
				return 0;
			}
		}
	}

	final static StreamExecutor io = new StreamExecutor(Context.IO_PARALLELISM, "AlbacoreIOExecutor", false);

	long size();

	static <V, A, R> R map(Iterable<V> col, Function<V, A> mapper, Collector<? super A, ?, R> collector) {
		return io.map(col, mapper, collector);
	}

	static <V, A, R> R collect(Iterable<V> col, Function<Stream<V>, Stream<A>> mapping, Collector<? super A, ?, R> collector) {
		return io.collect(col, mapping, collector);
	}

	static <V, R> R collect(Iterable<? extends V> col, Collector<V, ?, R> collector) {
		return io.collect(col, collector);
	}

	static <V, R> R collect(Stream<? extends V> stream, Collector<V, ?, R> collector) {
		return io.collect(stream, collector);
	}

	static <V> List<V> list(Stream<V> stream) {
		return io.list(stream);
	}

	static <V, R> List<R> list(Iterable<V> col, Function<V, R> mapper) {
		return io.list(col, mapper);
	}

	static <V> void each(Iterable<V> col, Consumer<? super V> consumer) {
		io.each(col, consumer);
	}
}
