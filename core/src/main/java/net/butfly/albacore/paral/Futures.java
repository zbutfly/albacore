package net.butfly.albacore.paral;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface Futures {
	static <T> Future<T> local(Callable<T> c) {
		try {
			T t = c.call();
			return done(t);
		} catch (Exception e) {
			return fail(e);
		}
	}

	static <T> Future<T> done(T t) {
		return new $Priv$.DoneFuture<T>(t);
	}

	static <T> Future<T> fail(Throwable c) {
		return new $Priv$.FailFuture<>(c);
	}

	class $Priv$ {
		static final class DoneFuture<T> implements Future<T> {
			public final T x;

			private DoneFuture(T x) {
				this.x = x;
			}

			@Override
			public T get() throws InterruptedException, ExecutionException {
				return x;
			}

			@Override
			public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
				return x;
			}

			@Override
			public boolean isDone() {
				return true;
			}

			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				return false;
			}

			@Override
			public boolean isCancelled() {
				return false;
			}

			@Override
			public String toString() {
				return "DoneFuture->" + x;
			}
		}

		static final class FailFuture<T> implements Future<T> {

			public final Throwable cause;

			private FailFuture(Throwable cause) {
				Objects.requireNonNull(cause);
				this.cause = cause instanceof ExecutionException ? cause.getCause() : cause;
			}

			@Override
			public T get() throws InterruptedException, ExecutionException {
				if (cause instanceof InterruptedException) throw (InterruptedException) cause;
				else throw new ExecutionException(unwrap(cause));
			}

			@Override
			public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
				return get();
			}

			@Override
			public boolean isDone() {
				return true;
			}

			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				return false;
			}

			@Override
			public boolean isCancelled() {
				return false;
			}

			@Override
			public String toString() {
				return "FailFuture->" + cause.toString();
			}
		}
	}
}
