package net.butfly.albacore.expr.fel;

import java.util.Map;
import java.util.Map.Entry;

import com.greenpineyu.fel.Expression;
import com.greenpineyu.fel.context.ArrayCtxImpl;
import com.greenpineyu.fel.context.FelContext;
import com.greenpineyu.fel.exception.ParseException;

import net.butfly.albacore.expr.Engine;
import net.butfly.albacore.utils.collection.Maps;

public class FelEngine implements Engine {
	private final static com.greenpineyu.fel.FelEngine engine = Fels.scan();

	private final static Map<String, Expression> exprs = Maps.of();

	@SuppressWarnings("unchecked")
	@Override
	public <T> T exec(String felExpr, Map<String, Object> context) {
		FelContext ctx = new ArrayCtxImpl();
		if (null != context && !context.isEmpty()) for (Entry<String, Object> e : context.entrySet())
			ctx.set(e.getKey(), e.getValue());
		Expression ex = exprs.computeIfAbsent(felExpr, expr -> {
			try {
				return engine.compile(felExpr, ctx);
			} catch (ParseException e) {
				logger.error("Expression parsing fail", e);
				return null;
			}
		});
		if (null == ex) return null;
		Object r = ex.eval(ctx);
		return Fels.isNull(r) ? null : (T) r;
	}
}
