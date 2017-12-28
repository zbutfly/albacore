package net.butfly.albacore.utils;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IPv4 extends IPAddr {
	private final int[] segs;

	public IPv4(String spec) {
		this(Arrays.stream(spec.split(".", 4)).mapToInt(new ToIntFunction<String>() {
			@Override
			public int applyAsInt(String i) {
				return Integer.parseInt(i);
			}
		}).toArray());
	}

	public IPv4(int... segs) {
		super(4);
		this.segs = new int[4];
		for (int i = 0; i < segs.length && i < this.segs.length; i++)
			this.segs[i] = segs[i];
	}

	@Override
	public String toString() {
		Stream<String> s = Arrays.stream(segs).boxed().map(new Function<Integer, String>() {
			@Override
			public String apply(Integer i) {
				return Integer.toHexString(i);
			}
		});
		return s.collect(Collectors.joining("."));
	}
}
