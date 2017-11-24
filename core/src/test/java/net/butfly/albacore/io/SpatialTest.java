package net.butfly.albacore.io;

import static net.butfly.albacore.paral.Exeters.DEFEX;

import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import net.butfly.albacore.paral.steam.Steam;
import net.butfly.albacore.utils.collection.Its;

public class SpatialTest {
	public static void main(String[] args) {
		Steam<Integer> s;
		int MAX = 10000;
		AtomicInteger seed = new AtomicInteger();
		s = Steam.of(() -> {
			int i = seed.getAndIncrement();
			System.out.println("\t\t==> advancing to " + i);
			return i;
		}, MAX, () -> seed.get() >= MAX);
		Spliterator<Integer> it = s.spliterator();
		it.hasCharacteristics(Spliterator.SIZED);

		System.out.println("remain in parent: " + it.estimateSize());
		Map<Integer, Spliterator<Integer>> its = new ConcurrentHashMap<>();
		for (int i = 0; i < 5; i++)
			its.put(i, Its.wrap(it));
		for (int i : its.keySet())
			DEFEX.submit(() -> its.get(i).forEachRemaining(v -> System.out.println("#" + i + ": " + v + ", remain in parent: " + it
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
