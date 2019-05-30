package net.butfly.albacore.expr.fel;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.butfly.albacore.expr.Engine;
import net.butfly.albacore.expr.fel.FelFunc.Func;

public interface FuncForRelation {

	@Func
	class CompareFunc extends FelFunc<Integer> {
		@Override
		protected boolean valid(int argl) {
			return argl == 2;
		}

		@Override
		public Integer invoke(Object... args) {
			if (Number.class.isAssignableFrom(args[0].getClass())
					&& Number.class.isAssignableFrom(args[1].getClass())) {
				double d = ((Number) args[0]).doubleValue() - ((Number) args[1]).doubleValue();
				return d == 0 ? 0 : (d > 0 ? 1 : -1);
			}
//			if (!((String.class.isAssignableFrom(args[0].getClass())
//					&& String.class.isAssignableFrom(args[1].getClass()))
//					|| (Date.class.isAssignableFrom(args[0].getClass())
//							&& Date.class.isAssignableFrom(args[1].getClass())))) {
			if (!(args[0].getClass().equals(args[1].getClass()) && args[0] instanceof Comparable && args[1] instanceof Comparable)) {
				args[0] = args[0].toString();
				args[1] = args[1].toString();
			}
			int c = ((Comparable) args[0]).compareTo(args[1]);
			return c == 0 ? 0 : (c > 0 ? 1 : -1);
		}
	}

	public static void main(String[] args) {
		Map<String, Object> map = new HashMap<>();
		map.put("a", new Date());
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		map.put("b", new Date());
		System.out.println(Engine.eval("compare(a, b)", map).toString());
//		System.out.println(((Comparable) map.get("a")).compareTo(map.get("b")));
	}
}
