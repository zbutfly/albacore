package net.butfly.albacore.io.std;

import net.butfly.albacore.io.OutputImpl;

public class ConsoleOutput extends OutputImpl<String> {
	private static final long serialVersionUID = 7782039002400807964L;

	public ConsoleOutput() {
		super("CONSOLE-OUTPUT-QUEUE");
	}

	@Override
	public boolean enqueue(String s) {
		System.out.println(s);
		return true;
	}
}
