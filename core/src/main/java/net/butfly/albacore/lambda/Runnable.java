package net.butfly.albacore.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface Runnable extends java.lang.Runnable, Serializable {
	void run();
}
