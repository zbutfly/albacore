package net.butfly.albacore.io;

import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.lambda.Runnable;
import net.butfly.albacore.utils.logger.Logger;

public interface Wrapper {
	static <T> WrapInput<T> wrap(Input<?> base, Dequeue<T> d) {
		WrapInput<T> i = new WrapInput<T>(base) {
			@Override
			public long dequeue(Function<Stream<T>, Long> using, long batchSize) {
				return d.dequeue(using, batchSize);
			}
		};
		return i;
	}

	static <T> WrapOutput<T> wrap(Output<?> base, Enqueue<T> d) {
		WrapOutput<T> i = new WrapOutput<T>(base) {
			@Override
			public long enqueue(Stream<T> items) {
				return d.enqueue(items);
			}
		};
		return i;
	}

	abstract class WrapInput<V> implements Input<V> {
		@Override
		public abstract long dequeue(Function<Stream<V>, Long> using, long batchSize);

		protected final Input<?> base;

		protected WrapInput(Input<?> origin) {
			this.base = origin;
		}

		@Override
		public long capacity() {
			return base.capacity();
		}

		@Override
		public boolean empty() {
			return base.empty();
		}

		@Override
		public boolean full() {
			return base.full();
		}

		@Override
		public Logger logger() {
			return base.logger();
		}

		@Override
		public String name() {
			return base.name();
		}

		@Override
		public long size() {
			return base.size();
		}

		@Override
		public String toString() {
			return base.toString() + "WrapIn";
		}

		@Override
		public boolean opened() {
			return base.opened();
		}

		@Override
		public boolean closed() {
			return base.closed();
		}

		@Override
		public void opening(Runnable handler) {
			base.opening(handler);
		}

		@Override
		public void closing(Runnable handler) {
			base.closing(handler);
		}

		@Override
		public void open() {
			base.open();
		}

		@Override
		public void close() {
			base.close();
		}
	}

	abstract class WrapOutput<V> implements Output<V> {
		@Override
		public abstract long enqueue(Stream<V> items);

		private final Output<?> base;

		private WrapOutput(Output<?> origin) {
			this.base = origin;
		}

		@Override
		public long capacity() {
			return base.capacity();
		}

		@Override
		public boolean empty() {
			return base.empty();
		}

		@Override
		public boolean full() {
			return base.full();
		}

		@Override
		public Logger logger() {
			return base.logger();
		}

		@Override
		public String name() {
			return base.name();
		}

		@Override
		public long size() {
			return base.size();
		}

		@Override
		public String toString() {
			return base.toString() + "WrapOut";
		}

		@Override
		public boolean opened() {
			return base.opened();
		}

		@Override
		public boolean closed() {
			return base.closed();
		}

		@Override
		public void opening(Runnable handler) {
			base.opening(handler);
		}

		@Override
		public void closing(Runnable handler) {
			base.closing(handler);
		}

		@Override
		public void open() {
			base.open();
		}

		@Override
		public void close() {
			base.close();
		}
	}
}