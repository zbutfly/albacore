package net.butfly.albacore.paral.split;

import static net.butfly.albacore.paral.Sdream.of;

import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class FilteredSpliterator<E> extends WrapperSpliterator<E> {
	private final Predicate<E> filter;

	public FilteredSpliterator(Spliterator<E> impl, Predicate<E> filter) {
		super(impl);
		this.filter = filter;
	}

	@Override
	public boolean tryAdvance(Consumer<? super E> using) {
		Optional<E> op;
		while (null != (op = next())) {
			E e = op.orElse(null);
			if (filter.test(e)) {
				using.accept(e);
				return true;
			}
		}
		return false;
	}

	@Override
	public Spliterator<E> trySplit() {
		Spliterator<E> ss = impl.trySplit();
		return null == ss ? null : of(ss).filter(filter).spliterator();
	}
}
