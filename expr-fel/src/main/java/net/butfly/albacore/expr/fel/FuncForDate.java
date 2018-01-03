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
				return Texts.parseDate(String.valueOf(args[1]), String.valueOf(args[0]));
			} catch (ParseException e) {
				logger.debug("Expression eval for date parsing fail", e);
				return null;
			}
		}
	}
}
