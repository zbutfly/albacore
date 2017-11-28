package net.butfly.albacore.utils.collection;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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
}
