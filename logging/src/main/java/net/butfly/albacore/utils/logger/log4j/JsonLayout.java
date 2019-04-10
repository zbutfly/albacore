package net.butfly.albacore.utils.logger.log4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.MDC;

import net.butfly.albacore.utils.json.Jsonx;

public class JsonLayout extends Layout {
	@Override
	public String getContentType() {
		return "application/json";
	}

	@Override
	public void activateOptions() {
	}

	@Override
	public String format(LoggingEvent event) {
		Map<String, Object> v = event(event);
		v.putAll(MDC.getCopyOfContextMap());
		return Jsonx.json(v);
	}

	private Map<String, Object> event(LoggingEvent event) {
		return new ConcurrentHashMap<>();
	}

	@Override
	public boolean ignoresThrowable() {
		return false;
	}
}
