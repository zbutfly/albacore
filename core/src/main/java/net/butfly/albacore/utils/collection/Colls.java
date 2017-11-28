package net.butfly.albacore.utils.collection;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;

import net.butfly.albacore.paral.steam.Sdream;

public interface Colls {
	static <E> List<E> list() {
		return new CopyOnWriteArrayList<>();
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
}
