package net.butfly.albacore.io;

import net.butfly.albacore.paral.steam.Steam;

@FunctionalInterface
public interface Enqueuer<V> {
	void enqueue(Steam<V> items);

	default void failed(Steam<V> fails) {}

	default void succeeded(long success) {}
}
