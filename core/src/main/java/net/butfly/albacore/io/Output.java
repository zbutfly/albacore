package net.butfly.albacore.io;

import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.io.queue.QImpl;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;

public abstract class Output<I> extends QImpl<I, Void> {
	private static final long serialVersionUID = -1;

	protected Output(String name) {
		super(name, Long.MAX_VALUE);
	}

	@Override
	public long size() {
		return 0;
	}

	@Override
	public <I0> Output<I0> prior0(Converter<I0, I> conv) {
		return priors(Collections.convAs(conv));
	}

	@Override
	public <I0> Output<I0> priors(Converter<List<I0>, List<I>> conv) {
		return new Output<I0>("Conv" + Output.this.name()) {
			private static final long serialVersionUID = -3972222736579861184L;

			@Override
			public boolean enqueue0(I0 item) {
				List<I> dd = conv.apply(Arrays.asList(item));
				if (null == dd || dd.isEmpty()) return false;
				return Output.this.enqueue0(dd.get(0));
			}

			@Override
			public long enqueue(List<I0> items) {
				return Output.this.enqueue(conv.apply(items));
			}

			@Override
			public long size() {
				return Output.this.size();
			}

			@Override
			public void closing() {}

			@Override
			public String toString() {
				return Output.this.getClass().getName() + "Prior:" + name();
			}
		};
	}

	/* disable dequeue on output */
	@Override
	@Deprecated
	public final Void dequeue0() {
		return null;
	}

	@Override
	@Deprecated
	public final List<Void> dequeue(long batchSize) {
		return null;
	}

}
