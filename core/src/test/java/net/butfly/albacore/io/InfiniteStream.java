package net.butfly.albacore.io;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class InfiniteStream {

	public static void main(String[] args) {
		Random r = new Random();
		AtomicInteger seed = new AtomicInteger(1);
//		Spliterators.spliteratorUnknownSize(iterator, characteristics)
		IntStream s = IntStream.iterate(seed.get(), i -> {
			int d = seed.get();
			return seed.getAndSet(d + i);
		});
		s.forEach(i -> System.out.println(i));

		while (true) {
			Stream<Integer> s1 = StreamSupport.stream(s.spliterator().trySplit(), true);
			System.out.println(s1.count());
		}
	}

}
