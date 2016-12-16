package net.butfly.albacore.io;

import java.util.List;

import net.butfly.albacore.io.queue.MapQ;
import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;

public abstract class MapInput<K, O> extends Input<O> implements MapQ<K, Void, O> {
	private static final long serialVersionUID = 5132827356023019769L;

	protected MapInput(String name) {
		super(name);
	}

	@Override
	public long size() {
		return super.size();
	}

	@Override
	public void closing() {
		for (K k : keys()) {
			Q<Void, O> q = q(k);
			if (null != q) q.close();
		}
	}

	@Override
	public O dequeue0() {
		for (K k : Collections.disorderize(keys())) {
			Q<Void, O> q = q(k);
			if (null == q) continue;
			O o = q.dequeue0();
			if (null != o) return o;
		}
		return null;
	}
	/* disable enqueue on input */

	@Override
	@Deprecated
	public final long enqueue(Converter<Void, K> keying, List<Void> it) {
		return 0;
	}

	@Override
	@Deprecated
	public final long enqueue(Converter<Void, K> keying, Void... e) {
		return 0;
	}

	@Deprecated
	@Override
	public boolean enqueue0(K key, Void e) {
		return false;
	}

}
