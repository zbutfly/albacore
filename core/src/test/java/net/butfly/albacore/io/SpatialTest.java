package net.butfly.albacore.io;

import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import net.butfly.albacore.utils.collection.Its;
import net.butfly.albacore.utils.collection.Streams;
import net.butfly.albacore.utils.parallel.Parals;

public class SpatialTest {
	public static void main(String[] args) {
		Stream<Integer> s;
		int MAX = 10000;
		AtomicInteger seed = new AtomicInteger();
		s = Streams.of(() -> {
			int i = seed.getAndIncrement();
			System.out.println("\t\t==> advancing to " + i);
			return i;
		}, MAX, () -> seed.get() >= MAX).onClose(() -> {
			System.out.println("\t\t<== closed!");
		});
		Spliterator<Integer> it = s.spliterator();
		it.hasCharacteristics(Spliterator.SIZED);

		System.out.println("remain in parent: " + it.estimateSize());
		Map<Integer, Spliterator<Integer>> its = new ConcurrentHashMap<>();
		for (int i = 0; i < 5; i++)
			its.put(i, Its.wrap(it));
		for (int i : its.keySet())
			Parals.listen(() -> its.get(i).forEachRemaining(v -> System.out.println("#" + i + ": " + v + ", remain in parent: " + it
					.estimateSize())));
		System.out.println("remain in parent: " + it.estimateSize());

		// Stream<Entry<Integer, Spliterator<Integer>>> ss = Streams.spatial(s,
		// 10).entrySet().parallelStream();
		// ss.forEach(e -> Streams.of(e.getValue()).forEach(v ->
		// System.out.println("#" + e.getKey() + ":" + v)));

		// s = IntStream.iterate(0, i -> {
		// System.out.println("\t\t\t==> advancing...");
		// return i + 1;
		// }).boxed().limit(MAX);
	}
}
