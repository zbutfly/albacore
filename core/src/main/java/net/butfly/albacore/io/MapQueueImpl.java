package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.async.Concurrents;

public class MapQueueImpl<K, E, Q extends AbstractQueue<E>> extends AbstractQueue<E> implements MapQueue<K, E> {
	private static final long serialVersionUID = 3551659378491886759L;
	private final Map<K, Q> queues;
	private final Q first;
	final Converter<E, K> keying;

	public MapQueueImpl(String name, long capacity, Converter<E, K> keying, Map<K, Q> queues) {
		super(name, capacity);
		queues = new ConcurrentHashMap<>();
		this.queues = queues;
		first = queues.values().iterator().next();
		this.keying = keying;
	}

	@SafeVarargs
	public MapQueueImpl(String name, long capacity, Converter<E, K> keying, Converter<K, Q> queuing, K... keys) {
		super(name, capacity);
		queues = new ConcurrentHashMap<>();
		for (K k : keys)
			queues.put(k, queuing.apply(k));
		first = queues.values().iterator().next();
		this.keying = keying;
	}

	@Override
	public final boolean empty(K key) {
		for (Q q : queues.values())
			if (!q.empty()) return false;
		return true;
	}

	@Override
	protected final E dequeueRaw() {
		List<E> l = dequeue(1, (K[]) null);
		return l.isEmpty() ? null : l.get(0);
	}

	@Override
	public final E dequeue(K key) {
		return queues.get(key).dequeue();
	}

	@Override
	public List<E> dequeue(long batchSize) {
		return dequeue(batchSize, (K[]) null);
	}

	@Override
	@SafeVarargs
	public final List<E> dequeue(long batchSize, K... key) {
		List<E> batch = new ArrayList<>();
		Iterable<K> ks = key == null || key.length == 0 ? queues.keySet() : Arrays.asList(key);
		int prev;
		do {
			prev = batch.size();
			for (K k : ks) {
				Q q = queues.get(k);
				E e = q.dequeueRaw();
				if (null != e) {
					batch.add(stats(Act.OUTPUT, e, () -> size()));
					if (q.empty()) q.gc();
				}
			}
			if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		return batch;
	}

	@Override
	protected boolean enqueueRaw(E e) {
		return enqueue(keying.apply(e), e);
	}

	@Override
	public final boolean enqueue(K key, E e) {
		return queues.get(key).enqueue(e);
	}

	@SafeVarargs
	@Override
	public final long enqueue(E... e) {
		return enqueue(keying, e);
	}

	@SafeVarargs
	@Override
	public final long enqueue(Converter<E, K> key, E... e) {
		while (full())
			Concurrents.waitSleep(FULL_WAIT_MS);
		List<E> remain = new ArrayList<>(Arrays.asList(e));
		long c = 0;
		while (!remain.isEmpty())
			remain = enqueue(key, remain, c);
		return c;
	}

	private final List<E> enqueue(Converter<E, K> key, List<E> l, long... c) {
		List<E> remain = new ArrayList<>();
		for (E ee : l)
			if (ee != null) {
				Q q = queues.get(key.apply(ee));
				if (!q.full() && q.enqueueRaw(ee) && null != stats(Act.INPUT, ee, () -> size())) c[0]++;
				else remain.add(ee);
			}
		return remain;
	}

	@Override
	public final long enqueue(Converter<E, K> key, Iterable<E> it) {
		return enqueue(key, it.iterator());
	}

	@Override
	public final long enqueue(Converter<E, K> key, Iterator<E> iter) {
		long c = 0;
		while (iter.hasNext()) {
			E e = iter.next();
			if (null != e && enqueue(key.apply(e), e)) c++;
		}
		return c;
	}

	@Override
	public final long size() {
		long s = 0;
		for (Q q : queues.values())
			s += q.size();
		return s;
	}

	@Override
	public final long size(K key) {
		return queues.get(key).size();
	}

	@Override
	public Converter<E, Long> statsing() {
		return first.statsing();
	}
}
