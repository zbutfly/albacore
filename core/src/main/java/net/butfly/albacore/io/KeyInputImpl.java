package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;

public abstract class KeyInputImpl<K, V> extends InputImpl<V> {
	public KeyInputImpl(String name) {
		super(name);
	}

	public abstract Set<K> keys();

	public abstract V dequeue(K key);

	@Override
	public final V dequeue() {
		while (true) {
			for (K k : Collections.disorderize(keys())) {
				V e = dequeue(k);
				if (null != e) return e;
			}
			if (!Concurrents.waitSleep()) return null;
		}
	}

	@Override
	public final Stream<V> dequeue(long batchSize) {
		return dequeue(batchSize, keys());
	}

	public Stream<V> dequeue(long batchSize, Iterable<K> keys) {
		List<V> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			for (K key : keys)
				read(key, batch);
			if (batch.size() >= batchSize) break;
			if (batch.size() == 0) Concurrents.waitSleep();
		} while (opened() && batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		return batch.parallelStream().filter(t -> null != t);
	}

	protected abstract void read(K key, List<V> batch);
}
