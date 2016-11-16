package net.butfly.albacore.io.impl;

import java.util.UUID;

import net.butfly.albacore.io.InputQueue;
import net.butfly.albacore.io.InputQueueImpl;

public class RandomStringInput extends InputQueueImpl<String> implements InputQueue<String> {
	private static final long serialVersionUID = 7782039002400807964L;
	public static final InputQueue<String> INSTANCE = new RandomStringInput();

	private RandomStringInput() {
		super("RANDOM-STRING-INPUT-QUEUE");
	}

	@Override
	protected String dequeueRaw() {
		return UUID.randomUUID().toString();
	}
}
