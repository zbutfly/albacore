package net.butfly.albacore.io;

import java.util.Iterator;

import net.butfly.albacore.io.queue.MapQ;
import net.butfly.albacore.lambda.Converter;

public abstract class MapInputImpl<K, O> extends InputImpl<O> implements MapQ<K, Void, O> {
	private static final long serialVersionUID = 5132827356023019769L;

	protected MapInputImpl(String name) {
		super(name);
	}

	@Override
	public long size() {
		return super.size();
	}

	/* disable enqueue on input */

	@Override
	@Deprecated
	public final long enqueue(Converter<Void, K> keying, Iterable<Void> it) {
		return MapQ.super.enqueue(keying, it);
	}

	@Override
	@Deprecated
	public final long enqueue(Converter<Void, K> keying, Void... e) {
		return MapQ.super.enqueue(keying, e);
	}

	@Override
	@Deprecated
	public final long enqueue(Converter<Void, K> keying, Iterator<Void> iter) {
		return 0;
	}

	@Deprecated
	@Override
	public long enqueue(K key, Iterator<Void> e) {
		return 0;
	}

}
