package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.butfly.albacore.io.queue.MapQ;
import net.butfly.albacore.lambda.Converter;

public abstract class MapOutputImpl<K, I> extends OutputImpl<I> implements MapQ<K, I, Void> {
	private static final long serialVersionUID = 5132827356023019769L;
	private final Converter<I, K> keying;

	protected MapOutputImpl(String name, Converter<I, K> keying) {
		super(name);
		this.keying = keying;
	}

	@Override
	public final long size() {
		return super.size();
	}

	@Override
	public final long enqueue(Converter<I, K> keying, Iterator<I> iter) {
		Map<K, List<I>> m = new HashMap<>();
		long c = 0;
		while (iter.hasNext()) {
			I e = iter.next();
			if (null != e) {
				m.computeIfAbsent(keying.apply(e), kk -> new ArrayList<>()).add(e);
				c++;
			}
		}
		for (Entry<K, List<I>> e : m.entrySet())
			enqueue(e.getKey(), e.getValue().iterator());
		return c;
	}

	@Override
	public final long enqueue(Converter<I, K> keying, Iterable<I> it) {
		return enqueue(keying, it.iterator());
	}

	@Override
	public final long enqueue(Converter<I, K> key, @SuppressWarnings("unchecked") I... e) {
		return enqueue(Arrays.asList(e));
	}

	@Override
	public final boolean enqueue(I e) {
		return enqueue(keying.apply(e), Arrays.asList(e).iterator()) == 1;
	}

	@Override
	public final long enqueue(Iterator<I> iter) {
		return enqueue(keying, iter);
	}

	@Override
	public final long enqueue(Iterable<I> it) {
		return enqueue(keying, it);
	}

	/* disable dequeue on output */

	@Override
	public final List<Void> dequeue(long batchSize, K key) {
		return null;
	}

}
