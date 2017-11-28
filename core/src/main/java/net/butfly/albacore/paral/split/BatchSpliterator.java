package net.butfly.albacore.paral.split;

import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

import net.butfly.albacore.paral.steam.Sdream;
import net.butfly.albacore.utils.collection.Colls;

public class BatchSpliterator<E> extends ConvedSpliteratorBase<Sdream<E>, E> {
	private int batchSize;

	public BatchSpliterator(Spliterator<E> impl, int batchSize) {
		super(impl, impl.characteristics());
		this.batchSize = batchSize;
	}

	@Override
	public long estimateSize() {
		return super.estimateSize() == Long.MAX_VALUE ? Long.MAX_VALUE : super.estimateSize() / batchSize;
	}

	@Override
	public boolean tryAdvance(Consumer<? super Sdream<E>> using) {
		List<E> batch = Colls.list();
		while (batch.size() < batchSize && impl.tryAdvance(batch::add)) {}
		if (batch.isEmpty()) return false;
		using.accept(Sdream.of(batch));
		return true;
	}

	@Override
	public Spliterator<Sdream<E>> trySplit() {
		Spliterator<E> ss = impl.trySplit();
		return null == ss ? null : new BatchSpliterator<>(ss, batchSize);
	}
}
