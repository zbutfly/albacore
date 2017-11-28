package net.butfly.albacore.paral.split;

import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

abstract class LockSpliterator<E> implements Spliterator<E> {
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	protected final <V> V read(Callable<V> doing) {
		lock.readLock().lock();
		try {
			return doing.call();
		} catch (Exception e) {
			throw e instanceof RuntimeException ? (RuntimeException) e : new IllegalStateException(e);
		} finally {
			lock.readLock().unlock();
		}
	}

	protected final <V> V write(Callable<V> doing) {
		lock.writeLock().lock();
		try {
			return doing.call();
		} catch (Exception e) {
			throw e instanceof RuntimeException ? (RuntimeException) e : new IllegalStateException(e);
		} finally {
			lock.writeLock().unlock();
		}
	}
}
