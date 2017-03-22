package net.butfly.albacore.utils;

import java.util.Arrays;
import java.util.stream.Collectors;

public class IPv4 extends IPAddr {
	private final int[] segs;

	public IPv4(String spec) {
		this(Arrays.stream(spec.split(".", 4)).mapToInt(i -> Integer.parseInt(i)).toArray());
	}

	public IPv4(int... segs) {
		super(4);
		this.segs = new int[4];
		for (int i = 0; i < segs.length && i < this.segs.length; i++)
			this.segs[i] = segs[i];
	}

	@Override
	public String toString() {
		return Arrays.stream(segs).boxed().map(i -> Integer.toHexString(i)).collect(Collectors.joining("."));
	}
}
