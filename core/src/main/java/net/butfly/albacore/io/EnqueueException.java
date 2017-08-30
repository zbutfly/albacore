package net.butfly.albacore.io;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;

public class EnqueueException extends RuntimeException {
	private static final long serialVersionUID = 7327387986796280231L;
	private final ConcurrentMap<Object, Throwable> fails;
	private AtomicLong success = new AtomicLong();

	public EnqueueException() {
		super();
		fails = new ConcurrentHashMap<>();
	}

	@Override
	public String getMessage() {
		return Joiner.on("\n").join(fails.values().stream().map(e -> e.getMessage()).collect(Collectors.toList()));
	}

	@SuppressWarnings("unchecked")
	public <V> List<V> fails() {
		return fails.keySet().stream().map(v -> (V) v).collect(Collectors.toList());
	}

	public <V> EnqueueException fail(V value, Throwable cause) {
		fails.putIfAbsent(value, cause);
		return this;
	}

	public <V> Exception fails(Collection<V> values) {
		values.forEach(v -> fails.putIfAbsent(v, new RuntimeException()));
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
