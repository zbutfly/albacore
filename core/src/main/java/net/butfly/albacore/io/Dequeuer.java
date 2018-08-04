package net.butfly.albacore.io;

import java.io.Serializable;

import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.paral.Sdream;

@FunctionalInterface
public interface Dequeuer<V> extends Serializable {
	void dequeue(Consumer<Sdream<V>> using);
}
