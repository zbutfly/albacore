package net.butfly.albacore.expr.fel;

import static net.butfly.albacore.expr.fel.Fels.isNull;

import java.util.Calendar;
import java.util.Date;

import net.butfly.albacore.expr.fel.FelFunc.Func;
import net.butfly.albacore.utils.logger.Logger;

public interface FuncTimeCompare {
	static final Logger logger = Logger.getLogger(FuncTimeCompare.class);

	@Func
	public class AddDayFunc extends FelFunc<Date> {
		@Override
		protected boolean valid(int argl) {
			return argl == 2;
		}

		@Override
		protected Date invoke(Object... args) {
			if ((isNull(args[0]) || isNull(args[1])) || 
					!(args[0] instanceof Date) || 
					!Number.class.isAssignableFrom(args[1].getClass()))
				return null;
			return compare((Date)args[0], Integer.parseInt(args[1].toString()));
		}

		public static Date compare(Date time, int num) {
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(time);
			calendar.add(Calendar.DAY_OF_YEAR, num);
			return calendar.getTime();
		}
	}
	
	@Func
	public class IsgtFunc extends FelFunc<Boolean> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		protected Boolean invoke(Object... args) {
			if (args[0] instanceof Date) {
				Date d1 = (Date)args[0];
				Date d2 = new Date();
				return d1.compareTo(d2) >= 0;
			}
			return false;
		}
	}
}
