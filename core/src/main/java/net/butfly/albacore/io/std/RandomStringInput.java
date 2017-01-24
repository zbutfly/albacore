package net.butfly.albacore.io.std;

import java.util.UUID;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.InputImpl;

public class RandomStringInput extends InputImpl<String> {
	public static final Input<String> INSTANCE = new RandomStringInput();

	@Override
	public String dequeue(boolean block) {
		return UUID.randomUUID().toString();
	}
}
