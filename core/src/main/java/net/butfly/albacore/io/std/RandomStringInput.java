package net.butfly.albacore.io.std;

import java.util.UUID;

import net.butfly.albacore.io.Input;

public class RandomStringInput extends Input<String> {
	private static final long serialVersionUID = 7782039002400807964L;
	public static final Input<String> INSTANCE = new RandomStringInput();

	private RandomStringInput() {
		super("RANDOM-STRING-INPUT-QUEUE");
	}

	@Override
	public String dequeue0() {
		return UUID.randomUUID().toString();
	}
}
