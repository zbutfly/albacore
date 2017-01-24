package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import net.butfly.albacore.utils.async.Concurrents;

public abstract class KeyInputImpl<K, V> extends InputImpl<V> {
	public KeyInputImpl(String name) {
		super(name);
	}

	public abstract Set<K> keys();

	public abstract V dequeue(K key);

	public List<V> dequeue(long batchSize, Iterable<K> keys) {
		List<V> batch = new ArrayList<>();
		long prev;
		try {
			do {
				prev = batch.size();
				for (K key : keys)
					this.readTo(key, batch);
				if (batch.size() >= batchSize) return batch;
				if (batch.size() == 0) Concurrents.waitSleep();
			} while (opened() && batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
			return batch;
		} finally {
			readCommit();
		}
	}

	protected abstract void readTo(K key, List<V> batch);

	protected abstract void readCommit();
}
