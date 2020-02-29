package net.butfly.albacore.expr.fel;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

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
	class StrToDateByLocaleFunc extends FelFunc<Date> {
		@Override
		protected boolean valid(int argl) {
			return argl == 3;
		}

		@Override
		public Date invoke(Object... args) {
			if (null == args[2] || !(args[2] instanceof CharSequence)) throw new IllegalArgumentException("please support legal locale type like:[US], [EN], ...");
			Locale locale;
			switch ((String) args[2]) {
			case "US": locale = Locale.US;
			break;
			default:locale = Locale.getDefault(Locale.Category.FORMAT);
			}
			try {
				return Texts.parseDate((String) args[1], (String) args[0], locale);
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

	@Func
	class DateToMillsFunc extends FelFunc<Long> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		public Long invoke(Object... args) {
			if (args[0] instanceof Date) {
				return ((Date) args[0]).getTime();
			} else throw new RuntimeException("This is not a Date type!");
		}
	}

	@Func
	class DateValidFunc extends FelFunc<Boolean> {

		@Override
		protected boolean valid(int argl) {
			return argl == 4;
		}

		@Override
		protected Boolean invoke(Object... args) {
			long timestamp = 0l;
			if (args[0] instanceof Number) timestamp = ((Number) args[0]).longValue();
			else if (args[0] instanceof Date) timestamp = ((Date) args[0]).getTime();
			else throw new UnsupportedOperationException("1st parameter must be Date or Long");
			if (!(args[1] instanceof Date)) 
				throw new UnsupportedOperationException("2nd parameter must be Date");
			long compareTimestamp = ((Date) args[1]).getTime();
			if (!(args[2] instanceof Number) || !(args[3] instanceof Number)) 
				throw new UnsupportedOperationException("3rd and 4th parameter must be Integer");
			int lower = ((Number)args[2]).intValue();
			int upper = ((Number)args[3]).intValue();
			return timestamp >= (compareTimestamp - (lower * 24 * 3600000)) 
					&& timestamp <= (compareTimestamp + (upper * 24 * 3600000));
		}
	}

	@Func
	class NowFunc extends FelFunc<Date> {
		@Override
		protected boolean valid(int argl) {
			return argl == 0;
		}

		@Override
		public Date invoke(Object... args) {
				return new Date();
		}
	}

	@Func
	class DateTimeFormatterToLongFunc extends FelFunc<Long> {
		@Override
		protected boolean valid(int argl) {
			return argl == 3;
		}

		@Override
		public Long invoke(Object... args) {
			if (Fels.isNull(args[1])) throw new RuntimeException("Time Formatter should not be null");
			if (args[0] instanceof CharSequence && args[1] instanceof CharSequence && args[2] instanceof CharSequence) {
				DateTimeFormatter dtf = DateTimeFormatter.ofPattern(args[1].toString());
				LocalDateTime ldt = LocalDateTime.parse(args[0].toString(), dtf);
				return ldt.atOffset(ZoneOffset.of(args[2].toString())).toInstant().toEpochMilli();
			}
			throw new IllegalArgumentException("params are illegal");
		}
	}
	
}
