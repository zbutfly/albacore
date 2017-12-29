package net.butfly.albacore.utils;

public class Syss {
	public static long sizeOf(Object obj) {
		try {
			return jdk.nashorn.internal.ir.debug.ObjectSizeCalculator.getObjectSize(obj);
		} catch (Exception ex) {
			return 0;
		}
	}
}
