package net.butfly.albacore.paral.split;

import static net.butfly.albacore.paral.steam.Sdream.of;

import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicReference;
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
		AtomicReference<E> ref = new AtomicReference<>();
		do {
			if (!impl.tryAdvance(ref::lazySet)) return false;
			E e = ref.get();
			if (filter.test(e)) {
				using.accept(e);
				return true;
			}
		} while (true);
	}

	@Override
	public Spliterator<E> trySplit() {
		Spliterator<E> ss = impl.trySplit();
		return null == ss ? null : of(ss).filter(filter).spliterator();
	}
}
