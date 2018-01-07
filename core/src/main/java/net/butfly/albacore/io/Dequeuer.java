package net.butfly.albacore.io;

import java.util.function.Consumer;

import net.butfly.albacore.paral.Sdream;

@FunctionalInterface
public interface Dequeuer<V> {
	void dequeue(Consumer<Sdream<V>> using);
}
