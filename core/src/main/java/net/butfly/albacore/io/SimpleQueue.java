package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.List;

import net.butfly.albacore.utils.async.Concurrents;

public final class SimpleQueue<V> extends HeapQueue<V, V> implements Queue<V, V> {
	private static final long serialVersionUID = -1;

	public SimpleQueue(String name, long capacity) {
		super(name, capacity, e -> e);
	}

	@Override
	public V dequeue() {
		while (true) {
			V e = dequeueRaw();
			if (null != e) return e;
			else if (!Concurrents.waitSleep(EMPTY_WAIT_MS)) return null;
		}
	}

	@Override
	public List<V> dequeue(long batchSize) {
		List<V> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			V e = dequeueRaw();
			if (null != e) {
				batch.add(e);
				if (empty()) gc();
			}
			if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		if (empty()) gc();
		return batch;
	}
}
