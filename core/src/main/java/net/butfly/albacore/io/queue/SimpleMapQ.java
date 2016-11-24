package net.butfly.albacore.io.queue;

import net.butfly.albacore.lambda.Converter;

public abstract class SimpleMapQ<K, V> extends MapQImpl<K, V, V> implements Q<V, V> {
	private static final long serialVersionUID = 7806980932643866182L;

	public SimpleMapQ(String name, long capacity, Converter<V, K> keying) {
		super(name, capacity, keying);
	}
}
