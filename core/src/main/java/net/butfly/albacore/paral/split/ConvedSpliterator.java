package net.butfly.albacore.paral.split;

import java.util.Spliterator;
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
		return impl.tryAdvance(e0 -> {
			if (null == e0) return;
			E e = conv.apply(e0);
			if (null == e) return;
			using.accept(conv.apply(e0));
		});
	}

	@Override
	public Spliterator<E> trySplit() {
		Spliterator<E0> s = impl.trySplit();
		return null == s ? null : new ConvedSpliterator<>(s, conv);
	}
}
