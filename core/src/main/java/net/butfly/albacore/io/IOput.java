package net.butfly.albacore.io;

public interface IOput {
	long size();

	default long defaultBatch() {
		return 5000;
	}
}
