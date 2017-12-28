package net.butfly.albacore.utils.key;

import java.util.Arrays;

import net.butfly.albacore.utils.logger.Loggable;

public abstract class IdGenerator<K> extends Loggable {
	public abstract byte[] bytes();

	public abstract K generate();

	public abstract long machine();

	public abstract String gen0();

	public char[] encode(byte[] from, int len) {
		char[] n = new char[len];
		from = Arrays.copyOf(from, len);
		byte v1, v2;
		for (int i = 2, j = 3; j < len; i += 3, j += 4) {
			n[j - 3] = _ctx.CHARS_64[from[i - 2] & 63];
			v1 = from[i - 2];
			v1 >>= 6;
			v1 &= 3;
			v2 = from[i - 1];
			v2 <<= 2;
			v2 &= 60;
			n[j - 2] = _ctx.CHARS_64[v1 | v2];
			v1 = from[i - 1];
			v1 >>= 4;
			v1 &= 15;
			v2 = from[i];
			v2 <<= 4;
			v2 &= 48;
			n[j - 1] = _ctx.CHARS_64[v1 | v2];
			v2 = from[i];
			v2 >>= 2;
			v2 &= 63;
			n[j] = _ctx.CHARS_64[v2];
		}
		return n;
	}

	static class _ctx {
		private static char[] CHARS_64 = new char[64];
		static {
			for (int i = 0; i < 26; i++)
				CHARS_64[i] = (char) ('a' + i);
			for (int i = 0; i < 26; i++)
				CHARS_64[i + 26] = (char) ('A' + i);
			for (int i = 0; i < 10; i++)
				CHARS_64[i + 52] = (char) ('0' + i);
			CHARS_64[62] = '_';
			CHARS_64[63] = '-';
		}
	}
}