package net.butfly.albacore.calculus.factor.filter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import net.butfly.albacore.calculus.utils.Reflections;

public abstract class Filter implements Serializable {
	private static final long serialVersionUID = -8552695266561976312L;

	public static abstract class FieldFilter<V> extends Filter {
		private static final long serialVersionUID = 2471530170788545882L;
		public String field;

		public FieldFilter(String field) {
			super();
			Reflections.noneNull("Need field define", field);
			this.field = field;
		}
	}

	public static final class Equal<V> extends FieldFilter<V> {
		private static final long serialVersionUID = -3327669047546685341L;
		public V value;

		public Equal(String field, V value) {
			super(field);
			this.value = value;
		}
	}

	public static final class In<V> extends FieldFilter<V> {
		private static final long serialVersionUID = -3327669047546685341L;
		public Collection<V> values;

		public In(String field, Collection<V> values) {
			super(field);
			this.values = values;
		}

		@SafeVarargs
		public In(String field, V... value) {
			super(field);
			this.values = new HashSet<>(Arrays.asList(value));
		}
	}

	public static final class Between<V> extends FieldFilter<V> {
		private static final long serialVersionUID = -2093747604546307799L;
		public V min, max;

		public Between(String field, V min, V max) {
			super(field);
			this.min = min;
			this.max = max;
		}
	}

	public static class And extends Filter {
		private static final long serialVersionUID = -644453919882630263L;
		public List<Filter> filters;

		public And(Filter... filters) {
			super();
			this.filters = new ArrayList<>(Arrays.asList(filters));
		}
	}

	public static final class Or extends Filter {
		private static final long serialVersionUID = -6023895386145578847L;
		public List<Filter> filters;

		public Or(Filter... filters) {
			super();
			this.filters = new ArrayList<>(Arrays.asList(filters));
		}
	}

	public static final class Limit extends Filter {
		private static final long serialVersionUID = -2980235677478896288L;
		public long limit;

		public Limit(long limit) {
			super();
			this.limit = limit;
		}
	}

	public static final class Skip extends Filter {
		private static final long serialVersionUID = 4735511859113429102L;
		public long skip;

		public Skip(long skip) {
			super();
			this.skip = skip;
		}
	}

	public static final And SkipPage(long skip, long limit) {
		return new And(new Limit(limit + skip), new Skip(skip));
	};

	public static final class Page<K> extends Filter {
		private static final long serialVersionUID = 2847379064068669852L;
		public K offset;
		public long limit;

		public Page(K offset, long limit) {
			this.offset = offset;
			this.limit = limit;
		}
	}
}
