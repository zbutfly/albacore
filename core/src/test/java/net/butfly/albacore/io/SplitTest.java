package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;

public class SplitTest {

	public static void main(String[] args) {
		Spliterator<Integer> s = constr(10000);
		// IntStream.range(0, 100000).boxed().parallel().spliterator();
		// IntStream.iterate(0, i -> {
		// System.out.println("\t\t==>source advanced...");
		// return i + 1;
		// }).spliterator();
		AtomicInteger si = new AtomicInteger();
		Its.splitRun(s, 1024, sp -> System.out.println("split#" + si.getAndIncrement() + ": " + sp.estimateSize()));
		System.out.println("ready advancing..........................");
		System.out.println("s1: " + s.estimateSize());
	}

	public static Spliterator<Integer> constr(long max) {
		AtomicInteger seed = new AtomicInteger();
		Iterator<Integer> it = new Iterator<Integer>() {
			@Override
			public Integer next() {
				// System.out.println("\t\t==>source advanced...");
				return seed.getAndIncrement();
			}

			@Override
			public boolean hasNext() {
				return seed.get() < max;
			}
		};
		Spliterator<Integer> sp = Spliterators.spliterator(it, max, Spliterator.CONCURRENT | Spliterator.ORDERED | Spliterator.SIZED
				| Spliterator.SUBSIZED);
		// sp.hasCharacteristics(Spliterator.SUBSIZED);
		// sp.hasCharacteristics(Spliterator.SIZED);
		// sp.hasCharacteristics(Spliterator.CONCURRENT);
		// sp.hasCharacteristics(Spliterator.ORDERED);
		return sp;
	}
}
