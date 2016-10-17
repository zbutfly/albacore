package net.butfly.albacore.io.impl;

import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;
import net.butfly.albacore.lambda.Converter;

public class ConsoleOutput<E> extends OutputQueueImpl<E, String> implements OutputQueue<E> {
	private static final long serialVersionUID = 7782039002400807964L;

	public ConsoleOutput(Converter<E, String> stringify) {
		super("CONSOLE-OUTPUT-QUEUE", stringify);
	}

	@Override
	protected boolean enqueueRaw(E e) {
		String s = conv.apply(e);
		stats(Act.INPUT, s);
		System.out.println(s);
		return true;
	}
}
