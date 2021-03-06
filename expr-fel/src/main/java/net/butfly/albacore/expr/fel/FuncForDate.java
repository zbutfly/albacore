package net.butfly.albacore.expr.fel;

import java.text.ParseException;
import java.util.Date;

import net.butfly.albacore.expr.fel.FelFunc.Func;
import net.butfly.albacore.utils.Texts;

public interface FuncForDate {
	@Func
	class DateToStrFunc extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl == 2;
		}

		@Override
		public String invoke(Object... args) {
			return Texts.formatDate((String) args[1], (Date) args[0]);
		}
	}

	@Func
	class StrToDateFunc extends FelFunc<Date> {
		@Override
		protected boolean valid(int argl) {
			return argl == 2;
		}

		@Override
		public Date invoke(Object... args) {
			try {
				return Texts.parseDate((String) args[1], (String) args[0]);
			} catch (ParseException e) {
				throw new RuntimeException("Expression eval for date parsing fail", e);
			}
		}
	}

	@Func
	class MillsToDateFunc extends FelFunc<Date> {
		@Override
		protected boolean valid(int argl) {
			return argl == 2 || argl == 1;
		}

		@Override
		public Date invoke(Object... args) {
			long ms;
			if (Fels.isNull(args[0])) throw new RuntimeException("Mills long should not be null");
			if (args[0] instanceof CharSequence) ms = Long.parseLong(args[0].toString());
			else if (args[0] instanceof Number) ms = ((Number) args[0]).longValue();
			else throw new RuntimeException("Mills long should be string or number, but [" + args[0].getClass() + "] found.");
			if (args.length > 1) ms += ((Number) args[1]).intValue() * 3600000;// 60 * 60 * 1000
			return new Date(ms);
		}
	}
}
