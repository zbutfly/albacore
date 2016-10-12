package net.butfly.albacore.io;

public interface InputQueue<O> extends Queue<Void, O> {
	@Override
	default long size() {
		return Long.MAX_VALUE;
	}
}
