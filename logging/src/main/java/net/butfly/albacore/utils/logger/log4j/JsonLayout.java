package net.butfly.albacore.utils.logger.log4j;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.slf4j.MDC;

import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.json.Jsonx;

public class JsonLayout extends Layout {
	private boolean debug = false;// stacktrack outputting
	private String timeFormat = "yyyy-MM-dd hh:mm:ss,SSS";

	public boolean isDebug() { return debug; }

	public void setDebug(boolean debug) { this.debug = debug; }

	public String getTimeFormat() { return timeFormat; }

	public void setTimeFormat(String timeFormat) { this.timeFormat = timeFormat; }

	@Override
	public String getContentType() { return "application/json"; }

	@Override
	public void activateOptions() {}

	@Override
	public String format(LoggingEvent event) {
		Map<String, Object> v = new ConcurrentHashMap<>();
		v.putAll(MDC.getCopyOfContextMap());
		// v.put("DPC_NAME", Texts.formatDate(timeFormat, new Date(event.timeStamp)));
		// v.put("DPC_STATS_IN_TOTAL", Texts.formatDate(timeFormat, new Date(event.timeStamp)));
		// v.put("DPC_STATS_OUT_TOTAL", Texts.formatDate(timeFormat, new Date(event.timeStamp)));
		v.put("OP_TIME", Texts.formatDate(timeFormat, new Date(event.timeStamp)));
		v.put("LOG_NAME", event.getLoggerName());
		ThrowableInformation thi = event.getThrowableInformation();
		if (null != thi) {
			v.put("ERR_MESSAGE", thi.getThrowable().getMessage());
			if (debug) v.put("ERR_STACKTRACE", thi.getThrowableStrRep());
		}
		return Jsonx.json(v);
	}

	@Override
	public boolean ignoresThrowable() {
		return false;
	}
}
