package net.butfly.albacore.io;

import java.util.Spliterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albacore.utils.parallel.Suppliterator;

public class SplitTest {
	static int max = 350;
	static int parallelism = 4;
	static ForkJoinPool ex = new ForkJoinPool(parallelism);

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		AtomicInteger seed = new AtomicInteger();
		AtomicInteger[] counts = new AtomicInteger[parallelism];
		for (int i = 0; i < parallelism; i++)
			counts[i] = new AtomicInteger();

		Spliterator<Integer>[] ss = new Spliterator[parallelism];
		for (int i = 0; i < parallelism; i++) {
			int ii = i;
			ss[i] = new Suppliterator<Integer>(() -> {
				prefix(ii + "#source advanced...", ii);
				Concurrents.waitSleep();
				return seed.getAndIncrement() >= max ? null : seed.get();
			}, 100, () -> false);
		}
		Future<?>[] fs = new Future[parallelism];
		for (int i = 0; i < parallelism; i++) {
			int ii = i;
			fs[i] = ex.submit(() -> {
				ss[ii].forEachRemaining(v -> {
					Concurrents.waitSleep();
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
	};
}
