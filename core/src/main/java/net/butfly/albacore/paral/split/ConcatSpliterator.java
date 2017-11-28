package net.butfly.albacore.paral.split;

import static net.butfly.albacore.paral.split.SplitChars.merge;

import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

public class ConcatSpliterator<E> extends WrapperSpliterator<E> {
	private final Spliterator<E> another;

	public ConcatSpliterator(Spliterator<E> first, Spliterator<E> second) {
		super(first);
		this.another = Objects.requireNonNull(second);
	}

	@Override
	public int characteristics() {
		return merge(impl.characteristics(), another.characteristics());
	}

	@Override
	public long estimateSize() {
		long sz1 = impl.estimateSize();
		if (sz1 == Long.MAX_VALUE) return Long.MAX_VALUE;
		long sz2 = another.estimateSize();
		if (sz1 == Long.MAX_VALUE || sz2 >= Long.MAX_VALUE - sz1) return Long.MAX_VALUE;
		return sz1 + sz2;
	}

	@Override
	public boolean tryAdvance(Consumer<? super E> using) {
		if (impl.tryAdvance(using)) return true;
		return another.tryAdvance(using);
	}

	@Override
	public Spliterator<E> trySplit() {
		Spliterator<E> s = impl.trySplit();
		return null != s ? s : another.trySplit();
	}
}
