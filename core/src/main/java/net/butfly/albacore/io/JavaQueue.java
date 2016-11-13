package net.butfly.albacore.io;

import net.butfly.albacore.lambda.Converter;

public final class JavaQueue<I, O> extends JavaQueueImpl<I, O> {
	private static final long serialVersionUID = -4817498673299277451L;
	private Converter<I, O> conv;

	public JavaQueue(String name, long capacity, Converter<I, O> conv) {
		super(name, capacity);
		this.conv = conv;
	}

	@Override
	protected O conv(I e) {
		return conv.apply(e);
	}
}
