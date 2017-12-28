package net.butfly.albacore.io;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Joiner;

@Deprecated
public class EnqueueException extends RuntimeException {
	private static final long serialVersionUID = 7327387986796280231L;
	private final ConcurrentMap<Object, Throwable> fails;
	private AtomicLong success = new AtomicLong();

	public EnqueueException() {
		super();
		fails = new ConcurrentHashMap<Object, Throwable>();
	}

	@Override
	public String getMessage() {
		return Joiner.on("\n").join(fails.values().stream().map(new Function<Throwable, Object>() {
			@Override
			public Object apply(Throwable e) {
				return e.getMessage();
			}
		}).collect(Collectors.toList()));
	}

	@SuppressWarnings("unchecked")
	public <V> List<V> fails() {
		Stream<V> s = fails.keySet().stream().map(new Function<Object, V>() {
			@Override
			public V apply(Object v) {
				return (V) v;
			}
		});
		return s.collect(Collectors.<V> toList());
	}

	public <V> EnqueueException fail(V value, Throwable cause) {
		fails.putIfAbsent(value, cause);
		return this;
	}

	public <V> Exception fails(Collection<V> values) {
		values.forEach(new Consumer<V>() {
			@Override
			public void accept(V v) {
				fails.putIfAbsent(v, new RuntimeException());
			}
		});
		return this;
	}

	@Override
	public synchronized Throwable getCause() {
		Iterator<Throwable> it = fails.values().iterator();
		Throwable c;
		while (it.hasNext())
			if ((c = it.next()) != null) return c;
		return null;
	}

	public boolean empty() {
		return fails.isEmpty();
	}

	public long success() {
		return success.get();
	}

	public EnqueueException success(long successCount) {
		success.addAndGet(successCount);
		return this;
	}
}
