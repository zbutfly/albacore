//package net.butfly.albacore.utils.collection;
//
//import static net.butfly.albacore.paral.steam.Sdream.of;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Random;
//import java.util.Set;
//import java.util.function.BinaryOperator;
//import java.util.function.Function;
//import java.util.function.Supplier;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//import net.butfly.albacore.utils.Pair;
//import net.butfly.albacore.utils.Utils;
//
//@Deprecated
//public final class Collections extends Utils {
//	@Deprecated
//	public static <T, R> List<R> transform(Iterable<T> original, Function<T, R> trans) {
//		if (original == null) return null;
//		return transform(original.iterator(), trans);
//	}
//
//	@Deprecated
//	public static <T, R> List<R> transform(Iterator<T> original, Function<T, R> trans) {
//		if (original == null) return null;
//		List<R> r = new ArrayList<>();
//		while (original.hasNext())
//			r.add(trans.apply(original.next()));
//		return r;
//	}
//
//	@Deprecated
//	public static <T, K, V> Map<K, List<V>> mapMap(Collection<T> list, Function<T, Pair<K, V>> mapping) {
//		return of(list).map(mapping).collect(Collectors.groupingByConcurrent(t -> t.v1(), Collectors.mapping(t -> t.v2(), Collectors
//				.toList())));
//	}
//
//	public static <T> List<T> unorderize(Collection<T> origin) {
//		if (null == origin) return null;
//		return of(origin).collect(Collectors.collectingAndThen(Collectors.partitioningBy(t -> r.nextBoolean()), m -> {
//			List<T> r1 = m.get(Boolean.TRUE);
//			r1.addAll(m.get(Boolean.FALSE));
//			return r1;
//		}));
//	}
//
//	public static <T> T random(Iterable<T> origin, int size) {
//		int i = (int) (Math.random() * size);
//		Iterator<T> it = origin.iterator();
//		T e = null;
//		while (it.hasNext() && i >= 0)
//			e = it.next();
//		return e;
//	}
//
//	public static <T> List<T> asList(Collection<T> col) {
//		if (null == col) return null;
//		return col instanceof List ? (List<T>) col : of(col).list();
//	}
//
//	public static <T> List<T> asList(Iterable<T> it) {
//		if (null == it) return null;
//		if (it instanceof List) return (List<T>) it;
//		return collect(it, Collectors.toList());
//	}
//
//	public static <T> Set<T> intersection(Collection<T> c1, Collection<T> c2) {
//		return of(c1).filter(c2::contains).collect(Collectors.toSet());
//	}
//
//	private static final Random r = new Random();
//
//	private static <T> Collection<List<T>> chop(Stream<T> origin, int parallelism) {
//		if (parallelism <= 1) return Arrays.asList(origin.list());
//		else return origin.collect(Collectors.groupingBy(x -> r.nextInt(parallelism))).values();
//	}
//
//	public static <T> Stream<List<T>> chopped(Stream<T> origin, int parallelism) {
//		return of(chop(origin, parallelism));
//	}
//
//	public static <T> Collection<List<T>> chopped(Collection<T> origin, int blockSize) {
//		return chop(of(origin), origin.size() / blockSize);
//	}
//
//	@Deprecated
//	public static <T> List<List<T>> chopStatic(List<T> origin, int blockSize) {
//		List<List<T>> parts = new ArrayList<>();
//		int originalSize = origin.size();
//		for (int i = 0; i < originalSize; i += blockSize)
//			parts.add(new ArrayList<>(origin.subList(i, Math.min(originalSize, i + blockSize))));
//		return parts;
//	}
//
//	public static <T> Iterator<T> iterator(Supplier<T> supplier) {
//		return new Iterator<T>() {
//			@Override
//			public boolean hasNext() {
//				return true;
//			}
//
//			@Override
//			public T next() {
//				return supplier.get();
//			}
//		};
//	}
//}
