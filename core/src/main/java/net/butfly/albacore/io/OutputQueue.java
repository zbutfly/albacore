package net.butfly.albacore.io;

public interface OutputQueue<I> extends Queue<I, Void> {
	@Override
	default long size() {
		return 0;
	}
}
