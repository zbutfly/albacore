package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.List;

import net.butfly.albacore.utils.async.Concurrents;

public class SimpleJavaQueue<E> extends JavaQueueImpl<E, E> implements SimpleQueue<E> {
	private static final long serialVersionUID = -1;

	public SimpleJavaQueue(String name, long capacity) {
		super(name, capacity);
	}

	@Override
	@Deprecated
	protected final E conv(E e) {
		return e;
	}

	@Override
	public E dequeue() {
		while (true) {
			E e = dequeueRaw();
			if (null != e) return e;
			else if (!Concurrents.waitSleep(EMPTY_WAIT_MS)) return null;
		}
	}

	@Override
	public List<E> dequeue(long batchSize) {
		List<E> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			E e = dequeueRaw();
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
