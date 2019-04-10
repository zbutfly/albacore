package net.butfly.albacore.utils.logger.log4j;

import static net.butfly.albacore.utils.Texts.AnsiColor.END_COLOUR;
import static net.butfly.albacore.utils.Texts.AnsiColor.FG_BLUE;
import static net.butfly.albacore.utils.Texts.AnsiColor.FG_CYAN;
import static net.butfly.albacore.utils.Texts.AnsiColor.FG_GREEN;
import static net.butfly.albacore.utils.Texts.AnsiColor.FG_RED;
import static net.butfly.albacore.utils.Texts.AnsiColor.FG_YELLOW;
import static net.butfly.albacore.utils.Texts.AnsiColor.colorStart;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Priority;
import org.apache.log4j.spi.LoggingEvent;

public class ANSIConsoleAppender extends ConsoleAppender {
	private static final int[] DEFAULT_AS_TRACE = new int[] { 0, FG_BLUE };
	private static Map<Integer, int[]> LEVEL_CODES = new ConcurrentHashMap<>();
	static {
		LEVEL_CODES.put(Priority.FATAL_INT, new int[] { 1, FG_RED });
		LEVEL_CODES.put(Priority.ERROR_INT, new int[] { 0, FG_RED });
		LEVEL_CODES.put(Priority.WARN_INT, new int[] { 0, FG_YELLOW });
		LEVEL_CODES.put(Priority.INFO_INT, new int[] { 0, FG_GREEN });
		LEVEL_CODES.put(Priority.DEBUG_INT, new int[] { 0, FG_CYAN });
	}
	private static String REPLACE_END_COLOR = END_COLOUR.replace("[", "\\[");

	@Override
	protected void subAppend(LoggingEvent event) {
		String c = start(event.getLevel());
		qw.write(c);
		// super.subAppend() from org.apache.log4j.WriterAppender
		qw.write(layout.format(event).replaceAll(REPLACE_END_COLOR, c)); // revert to origin color

		if (layout.ignoresThrowable()) {
			String[] s = event.getThrowableStrRep();
			if (s != null) {
				int len = s.length;
				for (int i = 0; i < len; i++) {
					qw.write(s[i]);
					qw.write(Layout.LINE_SEP);
				}
			}
		}

		if (shouldFlush(event)) qw.flush();
		// super.subAppend() from org.apache.log4j.WriterAppender
		qw.write(END_COLOUR);
		if (immediateFlush) qw.flush();
	}

	/**
	 * default as trace
	 */
	private static String start(Level level, int... otherCode) {
		int[] codes = LEVEL_CODES.getOrDefault(level.toInt(), DEFAULT_AS_TRACE);
		if (otherCode.length > 0) {
			int ol = codes.length;
			codes = Arrays.copyOf(codes, ol + otherCode.length);
			System.arraycopy(otherCode, 0, codes, ol, otherCode.length);
		}
		return colorStart(codes);
	}

	public static String colorize(String origin, Level level, int... otherCode) {
		return start(level, otherCode) + origin + END_COLOUR;
	}
}