package net.butfly.albacore.io;

import java.util.function.Consumer;

import net.butfly.albacore.paral.steam.Steam;

@FunctionalInterface
public interface Dequeuer<V> {
	void dequeue(Consumer<Steam<V>> using, int batchSize);
}
