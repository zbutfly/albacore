package net.butfly.albacore.paral.split;

import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

public class ConvedSpliterator<E, E0> extends ConvedSpliteratorBase<E, E0> {
	private final Function<E0, E> conv;

	public ConvedSpliterator(Spliterator<E0> impl, Function<E0, E> conv) {
		super(impl, impl.characteristics());
		this.conv = conv;
	}

	@Override
	public boolean tryAdvance(Consumer<? super E> using) {
		AtomicBoolean notUsed = new AtomicBoolean(true);
		boolean advanced = false;
		while (notUsed.get() && (advanced = impl.tryAdvance(e0 -> {
			if (null != e0) {
				E e = conv.apply(e0);
				if (null != e) {
					try {
						using.accept(e);
					} finally {
						notUsed.lazySet(false);
					}
				}
			}
		})));
		return advanced;
	}

	@Override
	public Spliterator<E> trySplit() {
		Spliterator<E0> s = impl.trySplit();
		return null == s ? null : new ConvedSpliterator<>(s, conv);
	}
}
