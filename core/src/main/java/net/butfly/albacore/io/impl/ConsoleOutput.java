package net.butfly.albacore.io.impl;

import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;

public class ConsoleOutput extends OutputQueueImpl<String> implements OutputQueue<String> {
	private static final long serialVersionUID = 7782039002400807964L;

	public ConsoleOutput() {
		super("CONSOLE-OUTPUT-QUEUE");
	}

	@Override
	protected boolean enqueueRaw(String s) {
		stats(Act.INPUT, s);
		System.out.println(s);
		return true;
	}
}
