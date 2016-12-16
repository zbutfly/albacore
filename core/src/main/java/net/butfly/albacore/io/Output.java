package net.butfly.albacore.io;

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

	public <I0> Output<I0> prior(Converter<I0, I> conv) {
		return new Output<I0>(Output.this.name() + "-prior-" + conv.toString()) {
			private static final long serialVersionUID = -3972222736579861184L;

			@Override
			public boolean enqueue0(I0 item) {
				return Output.this.enqueue0(conv.apply(item));
			}

			@Override
			public long enqueue(List<I0> items) {
				return Output.this.enqueue(Collections.transform(items, conv));
			}

			@Override
			public long size() {
				return Output.this.size();
			}

			@Override
			protected void closing() {
				Output.this.close();
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
