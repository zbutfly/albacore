package net.butfly.albacore.utils.key;

import java.util.UUID;

import net.butfly.albacore.utils.Keys;
import net.butfly.albacore.utils.Texts;

public class UUIDGenerator extends IdGenerator<UUID> {
	public static final UUIDGenerator GEN = new UUIDGenerator();

	@Override
	public UUID generate() {
		return UUID.randomUUID();
	}

	@Override
	public long machine() {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] bytes() {
		UUID v = generate();
		return Keys.bytes(v.getMostSignificantBits(), v.getLeastSignificantBits());
	}

	public byte[] bytes0() {
		UUID v = generate();
		return Keys.bytes(v.getMostSignificantBits());
	}

	@Override
	public String gen0() {
		return gen(24);
	}

	public String gen00() {
		return gen(12);
	}

	public String gen(int len) {
		return new String(encode(len > 12 ? bytes() : bytes0(), len));
	}

	public static void main(String... args) {
		UUID v = UUID.randomUUID();
		System.out.println(v);
		System.out.println(Texts.byte2hex(Keys.bytes(v.getMostSignificantBits(), v.getLeastSignificantBits())));
		System.out.println(Texts.byte2hex(Keys.bytes(v.getMostSignificantBits())));
		System.out.println(new String(GEN.encode(Keys.bytes(v.getMostSignificantBits()), 16)));
		System.out.println(v);
	}
}