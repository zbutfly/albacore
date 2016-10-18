package net.butfly.albacore.io;

import java.io.Closeable;
import java.io.Serializable;

interface AbstractQueue<I, O> extends Closeable, Serializable {
	String name();
}