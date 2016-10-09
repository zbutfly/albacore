package net.butfly.albacore.io.impl;

import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;

public class ConsoleOutput extends OutputQueueImpl<String, String> implements OutputQueue<String> {
	private static final long serialVersionUID = 7782039002400807964L;
	public static final OutputQueue<String> INSTANCE = new ConsoleOutput("CONSOLE-OUTPUT-QUEUE");

	private ConsoleOutput(String name) {
		super(name);
	}

	@Override
	protected boolean enqueueRaw(String d) {
		stats(Act.INPUT, d);
		System.out.println(d);
		return true;
	}
}
