package net.butfly.albacore.paral.split;

import static net.butfly.albacore.paral.split.SplitChars.NON_DISTINCT;
import static net.butfly.albacore.paral.split.SplitChars.NON_ORDERED;
import static net.butfly.albacore.paral.split.SplitChars.NON_SIZED;
import static net.butfly.albacore.paral.split.SplitChars.NON_SORTED;
import static net.butfly.albacore.paral.split.SplitChars.NON_SUBSIZED;

import java.util.Spliterator;
import java.util.function.Consumer;

import net.butfly.albacore.paral.steam.Sdream;

public class FlatedSpliterator<E> extends PooledSpliteratorBase<E, Sdream<E>> {

	public FlatedSpliterator(Spliterator<Sdream<E>> impl) {
		super(impl, impl.characteristics() //
				& NON_SIZED & NON_SUBSIZED & NON_DISTINCT & NON_ORDERED & NON_SORTED);
	}

	@Override
	public long estimateSize() {
		return Long.MAX_VALUE;
	}

	@Override
	public boolean tryAdvance(Consumer<? super E> using) {
		boolean hasNext = true;
		E e;
		while (null == (e = pool.poll()))
			if (!(hasNext = impl.tryAdvance(s -> s.each(pool::offer)))) break;
		if (null == e && null == (e = pool.poll()) && !hasNext) return false;
		using.accept(e);
		return true;
	}

	@Override
	public Spliterator<E> trySplit() {
		Spliterator<Sdream<E>> ss = impl.trySplit();
		return null == ss ? null : new FlatedSpliterator<>(ss);
	}
}
