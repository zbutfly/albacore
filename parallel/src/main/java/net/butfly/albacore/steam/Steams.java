package net.butfly.albacore.steam;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

public interface Steams {
	static <E, E1> Spliterator<E1> map(Spliterator<E> s, Function<E, E1> conv) {
		return new Spliterator<E1>() {
			@Override
			public int characteristics() {
				return s.characteristics();
			}

			@Override
			public long estimateSize() {
				return s.estimateSize();
			}

			@Override
			public boolean tryAdvance(Consumer<? super E1> using) {
				return s.tryAdvance(e -> using.accept(conv.apply(e)));
			}

			@Override
			public Spliterator<E1> trySplit() {
				Spliterator<E> ss = s.trySplit();
				return null == ss ? null : map(ss, conv);
			}
		};
	}
}
