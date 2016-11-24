package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.List;

import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;

public interface Input<O> extends Q<Void, O> {
	@Override
	default long size() {
		return Long.MAX_VALUE;
	}

	default <O2> Input<O2> then(Converter<O, O2> conv) {
		return new InputImpl<O2>(Input.this.name()) {
			private static final long serialVersionUID = -8694670290655232938L;

			@Override
			public boolean empty() {
				return Input.this.empty();
			}

			@Override
			public O2 dequeue() {
				return conv.apply(Input.this.dequeue());
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
			public void close() {
				Input.this.close();
			}
		};
	}

	@Override
	@Deprecated
	default boolean enqueue(Void d) {
		return false;
	}

	@Override
	@Deprecated
	default long enqueue(Iterator<Void> iter) {
		return 0;
	}

	@Override
	@Deprecated
	default long enqueue(Iterable<Void> it) {
		return 0;
	}

	@Override
	@Deprecated
	default long enqueue(Void... e) {
		return 0;
	}
}
