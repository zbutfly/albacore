package net.butfly.albacore.io;

import java.util.Arrays;
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
		return thens(Collections.convAs(conv));
	}

	@Override
	public <O2> Input<O2> thens(Converter<List<O>, List<O2>> conv) {
		return new Input<O2>(Input.this.name() + "Conv") {
			private static final long serialVersionUID = -8694670290655232938L;

			@Override
			public O2 dequeue0() {
				O v = ((Input<O>) Input.this).dequeue0();
				if (null == v) return null;
				List<O2> l = conv.apply(Arrays.asList(v));
				if (null == l || l.isEmpty()) return null;
				return l.get(0);
			}

			@Override
			public List<O2> dequeue(long batchSize) {
				List<O> l = Input.this.dequeue(batchSize);
				return conv.apply(l);
			}

			@Override
			public long size() {
				return Input.this.size();
			}

			@Override
			public boolean empty() {
				return Input.this.empty();
			}

			@Override
			public String toString() {
				return Input.this.getClass().getName() + "Then:" + name();
			}

			@Override
			public void close() {
				Input.super.close();
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
