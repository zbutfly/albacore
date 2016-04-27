package net.butfly.albacore.calculus.factor.filter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import net.butfly.albacore.calculus.utils.Reflections;

public interface FactorFilter extends Serializable {
	public static abstract class ByField<V> implements FactorFilter {
		private static final long serialVersionUID = -1L;
		public String field;

		public ByField(String field) {
			super();
			Reflections.noneNull("Need field define", field);
			this.field = field;
		}
	}

	public static abstract class ByFieldValue<V> extends ByField<V> {
		private static final long serialVersionUID = -1L;
		public V value;

		public ByFieldValue(String field, V value) {
			super(field);
			this.value = value;
		}
	}

	public static final class Equal<V> extends ByFieldValue<V> {
		private static final long serialVersionUID = -6072059360387978184L;
		public V value;

		public Equal(String field, V value) {
			super(field, value);
		}
	}

	public static final class NotEqual<V> extends ByFieldValue<V> {
		private static final long serialVersionUID = -5946751264164934310L;
		public V value;

		public NotEqual(String field, V value) {
			super(field, value);
		}
	}

	public class Less<V> extends ByFieldValue<V> {
		private static final long serialVersionUID = 4593053001556231013L;
		public V value;

		public Less(String field, V value) {
			super(field, value);
		}

	}

	public class Greater<V> extends ByFieldValue<V> {
		private static final long serialVersionUID = 6841459886500231382L;
		public V value;

		public Greater(String field, V value) {
			super(field, value);
		}

	}

	public class LessOrEqual<V> extends ByFieldValue<V> {
		private static final long serialVersionUID = -4045493244001924022L;
		public V value;

		public LessOrEqual(String field, V value) {
			super(field, value);
		}
	}

	public class GreaterOrEqual<V> extends ByFieldValue<V> {
		private static final long serialVersionUID = 8679129438234499986L;
		public V value;

		public GreaterOrEqual(String field, V value) {
			super(field, value);
		}
	}

	public static final class In<V> extends ByField<V> {
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

	public static class And implements FactorFilter {
		private static final long serialVersionUID = -644453919882630263L;
		public List<FactorFilter> filters;

		public And(FactorFilter... filters) {
			super();
			this.filters = new ArrayList<>(Arrays.asList(filters));
		}

		public static final And SkipPage(long skip, long limit) {
			return new And(new Limit(limit + skip), new Skip(skip));
		}

		public static final <V> And Between(String field, V min, V max) {
			return new And(new GreaterOrEqual<>(field, min), new LessOrEqual<>(field, max));
		}
	}

	public static class Not implements FactorFilter {
		private static final long serialVersionUID = 6621724392062910751L;
		public FactorFilter filter;

		public Not(FactorFilter filter) {
			super();
			this.filter = filter;
		}
	}

	public static final class Or implements FactorFilter {
		private static final long serialVersionUID = -6023895386145578847L;
		public List<FactorFilter> filters;

		public Or(FactorFilter... filters) {
			super();
			this.filters = new ArrayList<>(Arrays.asList(filters));
		}
	}

	public static final class Limit implements FactorFilter {
		private static final long serialVersionUID = 6389298147357096849L;
		public long limit;

		public Limit(long limit) {
			super();
			this.limit = limit;
		}
	}

	public static final class Random implements FactorFilter {
		private static final long serialVersionUID = -2980235677478896288L;
		public float chance;

		public Random(float chance) {
			super();
			this.chance = chance;
		}
	}

	public static final class Sort extends ByField<Boolean> {
		private static final long serialVersionUID = 2917438870291349552L;
		public boolean asc;

		public Sort(String field, boolean asc) {
			super(field);
			this.asc = asc;
		}
	}

	public static final class Skip implements FactorFilter {
		private static final long serialVersionUID = 4735511859113429102L;
		public long skip;

		public Skip(long skip) {
			super();
			this.skip = skip;
		}
	}

	public static final class Page<K> implements FactorFilter {
		private static final long serialVersionUID = 2847379064068669852L;
		public K offset;
		public long limit;

		public Page(K offset, long limit) {
			this.offset = offset;
			this.limit = limit;
		}
	}

	public static final class Regex extends ByField<String> implements FactorFilter {
		private static final long serialVersionUID = 4509935644112856045L;
		public Pattern regex;

		public Regex(String field, String regex) {
			super(field);
			this.regex = Pattern.compile(regex);
		}
	}

	/**
	 * For mongodb, $where: {javascript}
	 * 
	 * @author zx
	 */
	public static final class Where extends ByFieldValue<String> implements FactorFilter {
		private static final long serialVersionUID = 44828739379557295L;

		public Where(String field, String where) {
			super(field, where);
		}

	}

	/**
	 * For mongodb, $type: int
	 * 
	 * @author zx
	 */
	public static final class Type extends ByFieldValue<Integer> implements FactorFilter {
		private static final long serialVersionUID = 8237205268336851434L;

		public Type(String field, int type) {
			super(field, type);
		}
	}
}
