package net.butfly.albacore.io.lambda;

import java.io.Serializable;

public interface Function<T, R> extends java.util.function.Function<T, R>, Serializable {}
