package net.butfly.albacore.io;

import java.util.Arrays;

import net.butfly.albacore.io.queue.Q;

public interface Output<I> extends Q<I, Void> {
	@Override
	default long size() {
		return 0;
	}

	@Override
	default long enqueue(@SuppressWarnings("unchecked") I... e) {
		return enqueue(Arrays.asList(e));
	}
}
