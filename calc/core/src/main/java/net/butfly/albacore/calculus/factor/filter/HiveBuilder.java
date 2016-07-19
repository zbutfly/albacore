package net.butfly.albacore.calculus.factor.filter;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.lambda.Func;
import net.butfly.albacore.calculus.marshall.HiveMarshaller;
import net.butfly.albacore.calculus.utils.Reflections;

public class HiveBuilder<F extends Factor<F>> extends Builder<String, String, F> {
	private static final long serialVersionUID = -5901254740507977704L;
	long limit = -1;
	long skip = -1;
	double random = -1;

	public HiveBuilder(Class<F> factor, HiveMarshaller marshaller) {
		super(factor, marshaller);
	}

	@Override
	public String filter(FactorFilter... filters) {
		if (filters.length == 0) return null;
		StringBuilder hql = new StringBuilder(" where ");
		if (filters.length == 1) hql.append("(").append(filterOne(filters[0])).append(")");
		else hql.append("(").append(filterOne(new FactorFilter.And(filters))).append(")");
		if (limit >= 0) hql.append(" limit ").append(limit);
		return hql.toString();
	}

	@Override
	protected String expression(Class<?> type, Object value) {
		return CONVERTERS.get(type).call(value);
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected String filterOne(FactorFilter filter) {
		if (filter.getClass().equals(FactorFilter.Limit.class)) {
			this.limit = ((FactorFilter.Limit) filter).limit;
			return null;
		}
		if (filter.getClass().equals(FactorFilter.Random.class)) {
			this.random = ((FactorFilter.Random) filter).chance;
			return null;
		}
		StringBuilder q = new StringBuilder();
		if (filter instanceof FactorFilter.ByField) {
			Field field = Reflections.getDeclaredField(factor, ((FactorFilter.ByField<?>) filter).field);
			String qulifier = marshaller.parseQualifier(field);
			if (filter instanceof FactorFilter.ByFieldValue) {
				if (!ops.containsKey(filter.getClass()))
					throw new UnsupportedOperationException("Unsupportted filter: " + filter.getClass());
				return q.append(qulifier).append(ops.get(filter.getClass()))
						.append(expression(field.getType(), ((FactorFilter.ByFieldValue<?>) filter).value)).toString();
			}
			if (filter.getClass().equals(FactorFilter.In.class)) {
				q.append(" in (");
				boolean first = true;
				for (Object v : ((FactorFilter.In) filter).values) {
					if (!first) q.append(", ");
					else first = false;
					q.append(expression(field.getType(), v));
				}
				return q.append(")").toString();
			}
		}
		if (filter.getClass().equals(FactorFilter.And.class)) {
			boolean first = true;
			for (FactorFilter f : ((FactorFilter.And) filter).filters) {
				if (null == f) continue;
				if (!first) q.append(" and ");
				else first = false;
				q.append(filterOne(f));
			}
			return q.toString();
		}
		if (filter.getClass().equals(FactorFilter.Or.class)) {
			boolean first = true;
			for (FactorFilter f : ((FactorFilter.And) filter).filters) {
				if (null == f) continue;
				if (!first) q.append(" or ");
				else first = false;
				q.append(filterOne(f));
			}
			return q.toString();
		}

		throw new UnsupportedOperationException("Unsupportted filter: " + filter.getClass());
	}

	private static final Map<Class<? extends FactorFilter>, String> ops = new HashMap<>();
	static {
		ops.put(FactorFilter.Equal.class, " = ");
		ops.put(FactorFilter.NotEqual.class, " <> ");
		ops.put(FactorFilter.Less.class, " < ");
		ops.put(FactorFilter.Greater.class, " > ");
		ops.put(FactorFilter.LessOrEqual.class, " <= ");
		ops.put(FactorFilter.GreaterOrEqual.class, " >= ");
	}
	private static Func<Object, String> DEFAULT_CONV = o -> null == o ? null : o.toString();
	@SuppressWarnings("rawtypes")
	private static final Map<Class, Func<Object, String>> CONVERTERS = new HashMap<>();
	static {
		CONVERTERS.put(String.class, val -> null == val ? null : "\"" + val + "\"");
		CONVERTERS.put(Integer.class, DEFAULT_CONV);
		CONVERTERS.put(Boolean.class, DEFAULT_CONV);
		CONVERTERS.put(Long.class, DEFAULT_CONV);
		CONVERTERS.put(Double.class, DEFAULT_CONV);
		CONVERTERS.put(Float.class, DEFAULT_CONV);
		CONVERTERS.put(Short.class, DEFAULT_CONV);
		CONVERTERS.put(Byte.class, DEFAULT_CONV);
		CONVERTERS.put(BigDecimal.class, DEFAULT_CONV);

		CONVERTERS.put(int.class, DEFAULT_CONV);
		CONVERTERS.put(boolean.class, DEFAULT_CONV);
		CONVERTERS.put(long.class, DEFAULT_CONV);
		CONVERTERS.put(double.class, DEFAULT_CONV);
		CONVERTERS.put(float.class, DEFAULT_CONV);
		CONVERTERS.put(short.class, DEFAULT_CONV);
		CONVERTERS.put(byte.class, DEFAULT_CONV);
	}

}
