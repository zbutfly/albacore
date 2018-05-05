package net.butfly.albacore.utils;

import java.lang.instrument.Instrumentation;

/**
 * Add the following to your MANIFEST.MF: <br>
 * <blockquote> Premain-Class: {@code net.butfly.albacore.utils.InstrumentalSizeOf.Instrument} </blockquote>
 */
@Deprecated
public interface InstrumentalSizeOf extends SizeOfSupport {
	public static long sizeOf(Object obj) {
		try {
			return Instrument.instrumentation.getObjectSize(obj);
		} catch (Exception ex) {
			return 0;
		}
	}

	@Override
	default long _sizeOf() {
		return sizeOf(this);
	}

	static class Instrument {
		private static Instrumentation instrumentation;

		private Instrument() {}

		public static void premain(String args, Instrumentation inst) {
			instrumentation = inst;
		}
	}
}
