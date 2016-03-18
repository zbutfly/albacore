package net.butfly.albacore.calculus.functor;

import java.io.Serializable;

import scala.Tuple2;

public interface Functor extends Serializable {
	interface Pair extends Functor {
		@FunctionalInterface
		interface PairMap<K, V, T> extends Pair {
			public Tuple2<K, V> call(T t) throws Exception;
		}

		@FunctionalInterface
		interface Join extends Pair {
			public void call() throws Exception;
		}

		@FunctionalInterface
		interface KeyReduce<T1, T2, R> extends Pair {
			public R call(T1 v1, T2 v2) throws Exception;
		}

		@FunctionalInterface
		interface Finalize<D> extends Pair {
			public void call(D d) throws Exception;
		}
	}
}
