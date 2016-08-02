package net.butfly.albacore.calculus.factor.filter;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.lambda.Func;
import net.butfly.albacore.calculus.marshall.RowMarshaller;
import net.butfly.albacore.calculus.utils.Reflections;

public class HiveBuilder<F extends Factor<F>> extends Builder<String, String, F> {
	private static final long serialVersionUID = -5901254740507977704L;
	long limit = -1;
	long offset = -1;
	double random = -1;

	public HiveBuilder(Class<F> factor, RowMarshaller marshaller) {
		super(factor, marshaller);
	}

	public StringBuilder finalize(StringBuilder hql) {
		if (random >= 0) hql.insert(0, "select * from (").append(") where rand() <= + ").append(random).append(
				" distribute by rand() sort by rand");
		if (offset >= 0) hql.insert(0, "select *, row_number() over () as hivern from (").append(") where hivern >= ").append(offset);
		if (limit >= 0) hql.append(" limit ").append(limit);
		return hql;
	}

	@Override
	public String filter(FactorFilter... filters) {
		if (filters.length == 0) return null;
		StringBuilder hql = new StringBuilder(" where ");
		if (filters.length == 1) hql.append("(").append(filterOne(filters[0])).append(")");
		else hql.append("(").append(filterOne(new FactorFilter.And(filters))).append(")");
		return hql.toString();
	}

	@Override
	protected String expression(Class<?> type, Object value) {
		return CONVERTERS.get(type).call(value);
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected String filterOne(FactorFilter filter) {
		if (null == filter) return null;
		if (filter.getClass().equals(FactorFilter.Limit.class)) {
			this.limit = ((FactorFilter.Limit) filter).limit;
			return null;
		}
		if (filter.getClass().equals(FactorFilter.Skip.class)) {
			this.offset = ((FactorFilter.Skip) filter).skip + 1;
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
				if (!ops.containsKey(filter.getClass())) throw new UnsupportedOperationException("Unsupportted filter: " + filter
						.getClass());
				q.append(qulifier).append(ops.get(filter.getClass())).append(expression(field.getType(),
						((FactorFilter.ByFieldValue<?>) filter).value));
			} else if (filter.getClass().equals(FactorFilter.In.class) && ((FactorFilter.In) filter).values.size() > 0) {
				q.append(" in (");
				boolean first = true;
				for (Object v : ((FactorFilter.In) filter).values) {
					if (!first) q.append(", ");
					else first = false;
					q.append(expression(field.getType(), v));
				}
				q.append(")");
			} else if (filter.getClass().equals(FactorFilter.Regex.class)) q.append(qulifier).append(" rlike \"").append(
					((FactorFilter.Regex) filter).regex.toString().replaceAll("\\\\", "\\\\\\\\")).append("\"");
		} else if (filter.getClass().equals(FactorFilter.And.class) && ((FactorFilter.And) filter).filters.size() > 0) {
			boolean first = true;
			for (FactorFilter f : ((FactorFilter.And) filter).filters) {
				String qq = filterOne(f);
				if (null == f || null == qq) continue;
				if (!first) q.append(" and ");
				else first = false;
				q.append(qq);
			}
		} else if (filter.getClass().equals(FactorFilter.Or.class) && ((FactorFilter.Or) filter).filters.size() > 0) {
			boolean first = true;
			for (FactorFilter f : ((FactorFilter.And) filter).filters) {
				String qq = filterOne(f);
				if (null == f || null == qq) continue;
				if (!first) q.append(" or ");
				else first = false;
			}
		} else logger.warn("Unsupportted filter: " + filter.getClass() + ", ignored in hive.");
		return q.length() > 0 ? q.toString() : null;
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
