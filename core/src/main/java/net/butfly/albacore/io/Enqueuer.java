package net.butfly.albacore.io;

import java.io.Serializable;

import net.butfly.albacore.paral.Sdream;

@FunctionalInterface
public interface Enqueuer<V> extends Serializable {
	void enqueue(Sdream<V> items);

	default void failed(Sdream<V> fails) {}

	default void succeeded(long success) {}
}
