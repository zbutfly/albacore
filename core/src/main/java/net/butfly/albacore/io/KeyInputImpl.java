//package net.butfly.albacore.io;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Set;
//import java.util.function.Consumer;
//import java.util.stream.Stream;
//
//import net.butfly.albacore.utils.async.Concurrents;
//
//@Deprecated
//public abstract class KeyInputImpl<K, V> extends InputImpl<V> {
//	private List<K> keyAccess = null;
//
//	public KeyInputImpl(String name) {
//		super(name);
//	}
//
//	public abstract Set<K> keys();
//
//	public abstract V dequeue(K key);
//
//	@Override
//	protected final V dequeue() {
//		List<K> ks = keyAccess();
//		while (true) {
//			for (K k : ks) {
//				V e = dequeue(k);
//				if (null != e) return e;
//			}
//			if (!Concurrents.waitSleep()) return null;
//		}
//	}
//
//	protected List<K> keyAccess() {
//		if (keyAccess == null) keyAccess = new ArrayList<>(keys());
//		if (keyAccess.size() != keys().size()) {
//			keyAccess.clear();
//			keyAccess.addAll(keys());
//		}
//		java.util.Collections.shuffle(keyAccess);
//		return keyAccess;
//	}
//
//	@Override
//	public final void dequeue(Consumer<Stream<V>> using, long batchSize) {
//		using.accept(dequeue(batchSize, keys()));
//	}
//
//	public Stream<V> dequeue(long batchSize, Iterable<K> keys) {
//		List<V> batch = new ArrayList<>();
//		long prev;
//		do {
//			prev = batch.size();
//			for (K key : keys)
//				read(key, batch);
//			if (batch.size() >= batchSize) break;
//			if (batch.size() == 0) Concurrents.waitSleep();
//		} while (opened() && batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
//		return Streams.of(batch);
//	}
//
//	protected abstract void read(K key, List<V> batch);
//}
