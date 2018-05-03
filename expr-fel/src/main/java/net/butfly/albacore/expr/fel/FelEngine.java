package net.butfly.albacore.expr.fel;

import java.util.Map;
import java.util.Map.Entry;

import com.greenpineyu.fel.Expression;
import com.greenpineyu.fel.context.ArrayCtxImpl;
import com.greenpineyu.fel.context.FelContext;

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
		Expression ex = exprs.computeIfAbsent(felExpr, expr -> engine.compile(expr, ctx));
		if (null == ex) throw new IllegalArgumentException("Expression [" + felExpr + "] " + "from context [" + context + "] compile fail");
		Object r;
		try {
			r = ex.eval(ctx);
		} catch (Exception e) {
			throw new IllegalArgumentException("Expression [" + felExpr + "] " + "from context [" + context + "] compile fail");
		}
		return Fels.isNull(r) ? null : (T) r;
	}

	public static final void main(String... args) {
		FelEngine e = new FelEngine();
		Object v1 = e.exec("", Maps.of("B040001", "1030001", "B040002", "VALUE"));
		System.out.println(v1);
	}
}
