package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.paral.Suppliterator;
import net.butfly.albacore.utils.collection.Streams;

public class StreamTest {
	static int max = 45;//Integer.MAX_VALUE;
	static int parallelism = 5;
	static ForkJoinPool ex = new ForkJoinPool(parallelism);
	static AtomicInteger seed = new AtomicInteger();
	static Function<Integer, Iterator<Integer>> iter = ii -> new Iterator<Integer>() {
		@Override
		public Integer next() {
			prefix(ii + "#source advanced...", ii);
//			waitSleep();
			return seed.getAndIncrement() >= max ? null : seed.get();
		}

		@Override
		public boolean hasNext() {
			return true;
		}
	};

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		AtomicInteger[] counts = new AtomicInteger[parallelism];
		for (int i = 0; i < parallelism; i++)
			counts[i] = new AtomicInteger();

		Stream<Integer>[] ss = new Stream[parallelism];
		for (int i = 0; i < parallelism; i++)
			ss[i] = Streams.of(new Suppliterator<Integer>(iter.apply(i), 100));
		Future<?>[] fs = new Future[parallelism];
		for (int i = 0; i < parallelism; i++) {
			int ii = i;
			fs[i] = ex.submit(() -> {
				ss[ii].forEach(v -> {
//					waitSleep();
					prefix(ii + "#split: " + v, ii);
					counts[ii].incrementAndGet();
				});
			});
		}
		for (int i = 0; i < parallelism; i++)
			fs[i].get();
		for (int i = 0; i < parallelism; i++)
			System.out.println("count#" + i + ":" + counts[i].get());
	}

	private static void prefix(String s, int tabs) {
		for (int j = 0; j < tabs; j++)
			s = "\t" + s;
		System.out.println(s);
	}
}
