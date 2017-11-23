//package net.butfly.albacore.steam;
//
//import static net.butfly.albacore.steam.IterableSteam.SafeIterator.of;
//
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.NoSuchElementException;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.function.BinaryOperator;
//import java.util.function.Consumer;
//import java.util.function.Function;
//
//import net.butfly.albacore.utils.Pair;
//
//public class IterableSteam<E> extends SteamBase<E, Iterable<E>, IterableSteam<E>> implements Steam<E> {
//	public IterableSteam(Iterable<E> impl) {
//		super(of(impl));
//	}
//
//	@Override
//	public boolean next(Consumer<E> using) {
//		E e = impl.next();
//		if (null == e) return false;
//		using.accept(e);
//		return true;
//
//	}
//
//	@Override
//	public <R> Steam<R> map(Function<E, R> conv) {
//		return Steam.wrap(new Iterable<R>() {
//			@Override
//			public boolean hasNext() {
//				return impl.hasNext();
//			}
//
//			@Override
//			public R next() {
//				E n = impl.next();
//				return null == n ? null : conv.apply(n);
//			}
//		});
//	}
//
//	@Override
//	public <R> Steam<R> mapFlat(Function<E, List<R>> flat) {
//		Iterable<R> r = new Iterable<R>() {
//			private final BlockingQueue<R> nexts = new LinkedBlockingQueue<>();
//
//			@Override
//			public boolean hasNext() {
//				return !nexts.isEmpty() || impl.hasNext();
//			}
//
//			@Override
//			public R next() {
//				R n;
//				do {
//					while (!nexts.isEmpty())
//						if (null != (n = nexts.poll())) return n;
//					E t = impl.next();
//					if (null == t) return null;
//					List<R> l = flat.apply(t);
//					if (null == l || l.isEmpty()) continue;
//					for (R rr : l)
//						if (null != rr) nexts.offer(rr);
//				} while (hasNext());
//
//				return null;
//			}
//		};
//		return Steam.wrap(r);
//
//	}
//
//	@Override
//	public E reduce(BinaryOperator<E> accumulator) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <E1> Steam<Pair<E, E1>> join(Function<E, E1> func) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public <K> Map<K, Steam<E>> groupBy(Function<E, K> keying) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public List<Steam<E>> partition(int parts) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@Override
//	public List<Steam<E>> batch(long maxBatch) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	public static final class SafeIterator<E> implements Iterator<E> {
//		private final Iterator<E> origin;
//
//		public static <E> SafeIterator<E> of(Iterator<E> origin) {
//			if (origin instanceof SafeVarargs) return (SafeIterator<E>) origin;
//			return new SafeIterator<>(origin);
//		}
//
//		private SafeIterator(Iterator<E> origin) {
//			super();
//			this.origin = origin;
//		}
//
//		@Override
//		public boolean hasNext() {
//			synchronized (origin) {
//				return origin.hasNext();
//			}
//		}
//
//		@Override
//		public E next() {
//			E e = null;
//			while (null == e)
//				synchronized (origin) {
//					try {
//						e = origin.next();
//					} catch (NoSuchElementException ex) {
//						return null;
//					}
//				}
//			return e;
//		}
//	}
//
//	public static final class SafeIterable<E> implements Iterable<E> {
//		private final Iterator<E> safe;
//
//		private SafeIterable(Iterable<E> origin) {
//			super();
//			this.safe = of(origin.iterator());
//		}
//
//		@Override
//		public Iterator<E> iterator() {
//			return safe;
//		}
//
//	}
//}
