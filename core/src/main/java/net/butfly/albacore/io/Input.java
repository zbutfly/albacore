package net.butfly.albacore.io;

import java.util.List;
import net.butfly.albacore.io.queue.QImpl;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;

public abstract class Input<O> extends QImpl<Void, O> {
	private static final long serialVersionUID = -1;

	protected Input(String name) {
		super(name, -1);
	}

	@Override
	public long size() {
		return Long.MAX_VALUE;
	}

	@Override
	public <O2> Input<O2> then(Converter<O, O2> conv) {
		return new Input<O2>(Input.this.name() + "-Then") {
			private static final long serialVersionUID = -8694670290655232938L;

			@Override
			public O2 dequeue0() {
				return conv.apply(((Input<O>) Input.this).dequeue0());
			}

			@Override
			public List<O2> dequeue(long batchSize) {
				return Collections.transform(Input.this.dequeue(batchSize), conv);
			}

			@Override
			public long size() {
				return Input.this.size();
			}

			@Override
			public void closing() {
				Input.this.close();
			}
		};
	}

	/* disable enqueue on input */
	@Override
	@Deprecated
	public final boolean enqueue0(Void d) {
		return false;
	}

	@Override
	@Deprecated
	public final long enqueue(List<Void> iter) {
		return 0;
	}

	@Override
	@Deprecated
	public final long enqueue(Void... e) {
		return 0;
	}
}
