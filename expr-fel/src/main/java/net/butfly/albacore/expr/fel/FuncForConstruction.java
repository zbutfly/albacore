package net.butfly.albacore.expr.fel;

import static net.butfly.albacore.expr.fel.Fels.isNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.expr.Engine;
import net.butfly.albacore.expr.fel.FelFunc.Func;

public interface FuncForConstruction {

	@Func
	static class BuildMapFunc extends FelFunc<Map<String, Object>> {
		@Override
		protected boolean valid(int argl) {
			return argl >= 2;
		}

		@Override
		public Map<String, Object> invoke(Object... args) {
			if (isNull(args[0]) || isNull(args[1]) || args.length % 2 != 0)
				return null;
			Map<String, Object> map = new HashMap<>();
			for (int i = 0; i < args.length; i++) {
				String k = args[i].toString();
				Object v = args[++i];
				if (!isNull(k) && !isNull(v))
				map.put(k, v);
			}
			return map;
		}
	}

	@Func
	static class BuildListFunc extends FelFunc<List<Object>> {
		@Override
		protected boolean valid(int argl) {
			return argl >= 1;
		}

		@Override
		public List<Object> invoke(Object... args) {
			if (isNull(args[0]))
				return null;
			return Arrays.asList(args);
		}
	}

	public static void main(String[] args) {
		Map<String, Object> map = new HashMap<>();
		map.put("x", "a");
		map.put("y", "b");
		System.out.println(Engine.eval("buildMap(\"x\", x, \"y\", y)", map).toString());
		System.out.println(Engine.eval("buildList(x, y)", map).toString());
	}
}
