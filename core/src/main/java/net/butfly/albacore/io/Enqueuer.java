package net.butfly.albacore.io;

import net.butfly.albacore.paral.steam.Sdream;

@FunctionalInterface
public interface Enqueuer<V> {
	void enqueue(Sdream<V> items);

	default void failed(Sdream<V> fails) {}

	default void succeeded(long success) {}
}
