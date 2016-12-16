package net.butfly.albacore.io;

import java.util.List;
import net.butfly.albacore.io.queue.MapQ;
import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.lambda.Converter;

public abstract class MapOutput<K, I> extends Output<I> implements MapQ<K, I, Void> {
	private static final long serialVersionUID = 5132827356023019769L;
	private final Converter<I, K> keying;

	protected MapOutput(String name, Converter<I, K> keying) {
		super(name);
		this.keying = keying;
	}

	@Override
	public long size() {
		return MapQ.super.size();
	}

	@Override
	public void closing() {
		for (K k : keys()) {
			Q<I, Void> q = q(k);
			if (null != q) q.close();
		}
	}

	/* from Q */
	@Override
	public final boolean enqueue0(I e) {
		return enqueue0(keying.apply(e), e);
	}

	@Override
	public final long enqueue(List<I> it) {
		return enqueue(keying, it);
	}

	/* disable dequeue on output */
	@Override
	@Deprecated
	public final Void dequeue0(K key) {
		return null;
	}

	@SafeVarargs
	@Override
	public final List<Void> dequeue(long batchSize, K... key) {
		return null;
	}
}
