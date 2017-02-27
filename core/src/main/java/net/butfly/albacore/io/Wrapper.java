package net.butfly.albacore.io;

import java.util.function.Consumer;
import java.util.stream.Stream;

import net.butfly.albacore.utils.logger.Logger;

interface Wrapper {
	static <T> WrapInput<T> wrap(Input<?> base, Dequeue<T> d) {
		WrapInput<T> i = new WrapInput<T>(base) {
			@Override
			public void dequeue(Consumer<Stream<T>> using, long batchSize) {
				d.dequeue(using, batchSize);
			}
		};
		i.open();
		return i;
	}

	static <T> WrapOutput<T> wrap(Output<?> base, Enqueue<T> d) {
		WrapOutput<T> i = new WrapOutput<T>(base) {
			@Override
			public long enqueue(Stream<T> items) {
				return d.enqueue(items);
			}
		};
		i.open();
		return i;
	}

	abstract class WrapInput<V> implements Input<V> {
		private final Input<?> origin;

		private WrapInput(Input<?> origin) {
			this.origin = origin;
		}

		@Override
		public long capacity() {
			return origin.capacity();
		}

		@Override
		public boolean empty() {
			return origin.empty();
		}

		@Override
		public boolean full() {
			return origin.full();
		}

		@Override
		public Logger logger() {
			return origin.logger();
		}

		@Override
		public String name() {
			return origin.name() + "Wrapper";
		}

		@Override
		public long size() {
			return origin.size();
		}
	}

	abstract class WrapOutput<V> implements Output<V> {
		private final Output<?> origin;

		private WrapOutput(Output<?> origin) {
			this.origin = origin;
		}

		@Override
		public long capacity() {
			return origin.capacity();
		}

		@Override
		public boolean empty() {
			return origin.empty();
		}

		@Override
		public boolean full() {
			return origin.full();
		}

		@Override
		public Logger logger() {
			return origin.logger();
		}

		@Override
		public String name() {
			return origin.name() + "Wrapper";
		}

		@Override
		public long size() {
			return origin.size();
		}
	}
}