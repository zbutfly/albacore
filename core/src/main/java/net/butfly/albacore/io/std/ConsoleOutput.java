package net.butfly.albacore.io.std;

import net.butfly.albacore.io.OutputImpl;

public class ConsoleOutput extends OutputImpl<String> {
	@Override
	public boolean enqueue(String s) {
		System.out.println(s);
		return true;
	}
}
