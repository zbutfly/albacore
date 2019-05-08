package net.butfly.albacore.utils.logger.log4j;

import java.util.Date;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Layout;
import org.apache.log4j.MDC;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public String format(LoggingEvent event) {
		Map<String, Object> v = new ConcurrentHashMap<>();
		Hashtable mdc = MDC.getContext();
		if (null != mdc) mdc.forEach((k, vv) -> {
			if (null != vv) v.put(k.toString(), vv);
		});
		v.put("OP_TIME", Texts.formatDate(timeFormat, new Date(event.timeStamp)));
		ThrowableInformation thi = event.getThrowableInformation();
		if (null != thi) {
			v.put("ERR_MESSAGE", thi.getThrowable().getMessage());
			if (debug) v.put("ERR_STACKTRACE", thi.getThrowableStrRep());
		}
		v.put("LOG_MESSAGE", event.getMessage());
		return Jsonx.json(v) + "\n";
	}

	@Override
	public boolean ignoresThrowable() {
		return false;
	}
}
