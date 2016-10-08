package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.List;

import net.butfly.albacore.utils.async.Concurrents;

public abstract class SimpleQueueImpl<D> extends QueueImpl<D, D, D> implements SimpleQueue<D> {
	private static final long serialVersionUID = -1;

	protected SimpleQueueImpl(String name, long capacity) {
		super(name, capacity);
	}

	@Override
	public boolean enqueue(D e) {
		while (full())
			if (!Concurrents.waitSleep(FULL_WAIT_MS)) logger.warn("Wait for full interrupted");
		return (enqueueRaw(e));
	}

	@Override
	public final D dequeue() {
		while (true) {
			D e = dequeueRaw();
			if (null != e) return e;
			else if (!Concurrents.waitSleep(EMPTY_WAIT_MS)) return null;
		}
	}

	@Override
	public List<D> dequeue(long batchSize) {
		List<D> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			D e = dequeueRaw();
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
