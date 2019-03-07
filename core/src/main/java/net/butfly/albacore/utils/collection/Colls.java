package net.butfly.albacore.utils.collection;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

import net.butfly.albacore.io.lambda.BiFunction;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;

public interface Colls {
	static <E> List<E> list() {
		return new CopyOnWriteArrayList<>();
	}

	@SafeVarargs
	static <E> List<E> list(E... eles) {
		if (null == eles || eles.length == 0) return list();
		List<E> l = list();
		for (E e : eles)
			if (null != e) l.add(e);
		return l;
	}

	@SafeVarargs
	static <E, E1> List<E1> list(Function<E, E1> conv, E... eles) {
		if (null == eles) return list();
		List<Future<E1>> l = list();
		for (E e : eles)
			if (null != e) l.add(Exeter.of().submit(() -> conv.apply(e)));
		return Exeter.get(l);
	}

	static <E> List<E> list(Iterator<E> eles) {
		if (null == eles) return list();
		List<E> l = list();
		eles.forEachRemaining(e -> {
			if (null != e) l.add(e);
		});
		return l;
	}

	static <E, E1> List<E1> list(Sdream<E> eles, Function<E, E1> conv) {
		if (null == eles) return list();
		List<Future<E1>> l = list();
		eles.eachs(e -> {
			if (null != e) l.add(Exeter.of().submit(() -> conv.apply(e)));
		});
		return Exeter.get(l);
	}

	static <E, E1> List<E1> list(Iterator<E> eles, Function<E, E1> conv) {
		if (null == eles) return list();
		List<Future<E1>> l = list();
		eles.forEachRemaining(e -> {
			if (null != e) l.add(Exeter.of().submit(() -> conv.apply(e)));
		});
		return Exeter.get(l);
	}

	static <E> List<E> list(Iterable<E> eles) {
		if (eles instanceof List) return (List<E>) eles;
		return list(eles.iterator());
	}

	static <E, E1> List<E1> list(Iterable<E> eles, Function<E, E1> conv) {
		return list(eles.iterator(), conv);
	}

	static <K, E, E1> List<E1> list(Map<K, E> eles, BiFunction<K, E, E1> conv) {
		return list(eles.entrySet(), e -> conv.apply(e.getKey(), e.getValue()));
	}

	static int calcBatchParal(long total, long batchSize) {
		return total == 0 ? 0 : (int) (((total - 1) / batchSize) + 1);
	}

	static long calcBatchSize(long total, int parallelism) {
		return total == 0 ? 0 : (((total - 1) / parallelism) + 1);
	}

	public static <T> Set<T> intersection(Collection<T> c1, Collection<T> c2) {
		return Sdream.of(c1).filter(c2::contains).distinct();
	}

	static <T> Set<T> distinct() {
		return new ConcurrentSkipListSet<>();
	}

	static <E> List<E> flat(Iterable<List<E>> l) {
		List<E> ll = Colls.list();
		l.forEach(l0 -> ll.addAll(l0));
		return ll;
	}

	static boolean empty(Map<?, ?> m) {
		return null == m || m.isEmpty();
	}

	static boolean empty(List<?> l) {
		return null == l || l.isEmpty() || null == (l.get(0)); // TODO: Recursively check first?
	}

	static boolean empty(Collection<?> l) {
		return null == l || l.isEmpty();
	}

	static boolean empty(Iterable<?> l) {
		return null == l || !l.iterator().hasNext();
	}

	static boolean empty(CharSequence s) {
		return null == s || s.length() == 0;
	}

	static <T> boolean empty(T[] l) {
		return null == l || l.length == 0 || null == l[0];// TODO: Recursively check first?
	}
}
