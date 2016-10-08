package net.butfly.albacore.io;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Reflections;

public class SimpleMapQueue<K, V> extends MapQueueImpl<K, V, V, V> implements SimpleQueue<V> {
	private static final long serialVersionUID = 7806980932643866182L;
	private final Converter<V, K> keying;

	public SimpleMapQueue(String name, long capacity, Converter<V, K> keying) {
		super(name, capacity);
		Reflections.noneNull("", keying);
		this.keying = keying;
	}

	@Override
	protected K keying(V v) {
		return keying.apply(v);
	}
}
