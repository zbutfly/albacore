package net.butfly.albacore.io.std;

import net.butfly.albacore.io.OutputImpl;

public class ConsoleOutput extends OutputImpl<String> {
	@Override
	protected boolean enqueue(String item) {
		System.out.println(item);
		return true;
	}
}
