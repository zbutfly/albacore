package net.butfly.albacore.io.std;

import java.util.UUID;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.InputImpl;

public final class RandomStringInput extends InputImpl<String> {
	public RandomStringInput() {
		super();
		open();
	}

	public static final Input<String> INSTANCE = new RandomStringInput();

	@Override
	protected String dequeue() {
		return UUID.randomUUID().toString();
	}
}
