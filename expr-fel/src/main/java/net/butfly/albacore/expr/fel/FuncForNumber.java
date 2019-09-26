package net.butfly.albacore.expr.fel;

import static net.butfly.albacore.expr.fel.Fels.isNull;

import net.butfly.albacore.expr.fel.FelFunc.Func;

public interface FuncForNumber {

	@Func
	class AbsFunc extends FelFunc<Number> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		public Number invoke(Object... args) {
			if (!isNull(args[0])) {
				if (Integer.class.isAssignableFrom(args[0].getClass()))
					return Math.abs((Integer) args[0]);
				if (Double.class.isAssignableFrom(args[0].getClass()))
					return Math.abs((Double) args[0]);
				if (Float.class.isAssignableFrom(args[0].getClass()))
					return Math.abs((Float) args[0]);
				if (Long.class.isAssignableFrom(args[0].getClass()))
					return Math.abs((Long) args[0]);
			}
			logger.error(args[0] + " type:" + args.getClass() + " is not a number type!");
			return null;
		}
	}
}
