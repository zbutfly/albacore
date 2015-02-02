package net.butfly.albacore.utils;

import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import net.butfly.albacore.exception.SystemException;

public final class DateUtils extends Utils {
	private DateUtils() {}

	// prefix convenient for calculating
	private static final String PREFIX = "1";

	protected static final long DIFF_OF_CST_GMT = 8 * 60 * 60 * 1000;
	protected static final long MILLISECONDS_OF_DAY = 24 * 60 * 60 * 1000;

	public static DateFormat httpFormat = Texts.dateFormat("EEE, dd MMM yyyy HH:mm:ss z");

	public static DateFormat defaultDateFromat = Texts.dateFormat("yyyy-MM-dd");
	public static DateFormat detaultTimeFromat = Texts.dateFormat("HH:mm:ss");
	public static DateFormat shortTimeFromat = Texts.dateFormat("HH:mm");
	public static DateFormat databaseTimeFromat = Texts.dateFormat("HHmm");
	public static DateFormat db2TimeFromat = Texts.dateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");

	public static String formatForDB2(Date date) {
		return db2TimeFromat.format(null == date ? new Date() : date);
	}

	// format hhmm (0000 ~ 2359) to a int value 10000 ~ 12359 for comparing
	public static int format(String time) {
		if (time.length() != 4) { throw new SystemException("TimeFormater : Format of time String is not right! actual(" + time
				+ "), expected format(hhmm)"); }
		return Integer.valueOf(PREFIX + time).intValue();
	}

	// format the time field of Date to format hhmm
	public static String extractTime(Date date) {
		return databaseTimeFromat.format(null == date ? new Date() : date);
	}

	// format the date field of Date to format YYYY-MM-DD
	public static String extractDate(Date date) {
		return defaultDateFromat.format(null == date ? new Date() : date);
	}

	// compose a date(YYYY-MM-DD) and a String represented time(HHMM)
	public static Date composeDate(Date date, String time) {
		Calendar cal = new GregorianCalendar();
		cal.setTime(null == date ? new Date() : date);
		// it is OK to set hour like 02/03...
		cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(time.substring(0, 2)));
		cal.set(Calendar.MINUTE, Integer.parseInt(time.substring(2)));
		return cal.getTime();
	}

	// format Date, set fields to zero except year, month, date
	// get the internal days between two dates

	public static int daysBetween(Date early, Date late) {
		return (int) ((beginMSOfDay(late) - beginMSOfDay(early)) / (MILLISECONDS_OF_DAY));
	}

	public static Date beginOfDay(Date date) {
		long ms = date.getTime() + DIFF_OF_CST_GMT;
		return new Date((ms - ms % MILLISECONDS_OF_DAY) - DIFF_OF_CST_GMT);
	}

	public static long getTimeWithDayBegin(Date date) {
		long ms = date.getTime() + DIFF_OF_CST_GMT;
		return (ms - ms % MILLISECONDS_OF_DAY) - DIFF_OF_CST_GMT;
	}

	public static Date endOfDay(Date date) {
		long ms = date.getTime() + DIFF_OF_CST_GMT;
		return new Date((ms - ms % MILLISECONDS_OF_DAY + MILLISECONDS_OF_DAY - 1) - DIFF_OF_CST_GMT);
	}

	@SuppressWarnings("deprecation")
	public static Date create(int year, int month, int day) {
		return new Date(year, month, day);
	}

	@SuppressWarnings("deprecation")
	public static Date create(int year, int month, int day, int hour, int minute, int second) {
		return new Date(year, month, day, hour, minute, second);
	}

	public static boolean isSameDay(Date a, Date b) {
		return beginMSOfDay(a) == beginMSOfDay(b);
	}

	public static boolean after(Date late, Date early) {
		return beginMSOfDay(late) > beginMSOfDay(early);
	}

	private static long beginMSOfDay(Date date) {
		return beginOfDay(null == date ? new Date() : date).getTime();
	}

	public static boolean isEndOfMonth(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DAY_OF_MONTH, 1);
		return 1 == cal.get(Calendar.DAY_OF_MONTH);
	}

	public static boolean isEndOFYear(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.DAY_OF_MONTH, 1);
		return 1 == cal.get(Calendar.DAY_OF_YEAR);
	}

}
