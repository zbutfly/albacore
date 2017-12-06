package net.butfly.albacore.utils;

import java.io.Serializable;

public class Triple<T1, T2, T3> implements Serializable {
	private static final long serialVersionUID = 1277520665103226906L;
	public final T1 v1;
	public final T2 v2;
	public final T3 v3;

	public Triple(T1 v1, T2 v2, T3 v3) {
		super();
		this.v1 = v1;
		this.v2 = v2;
		this.v3 = v3;
	}

	@Override
	public String toString() {
		return "<" + v1 + "," + v2 + "," + v3 + ">";
	}

	public static <T1, T2, T3> Triple<T1, T2, T3> of(T1 v1, T2 v2, T3 v3) {
		return new Triple<>(v1, v2, v3);
	}

	@Override
	public boolean equals(Object obj) {
		if (null == obj || !Triple.class.isAssignableFrom(obj.getClass())) return false;
		@SuppressWarnings("rawtypes")
		Triple p = (Triple) obj;
		if (null == v1 && p.v1 != v1) return false;
		if (null == v2 && p.v2 != v2) return false;
		if (null == v3 && p.v3 != v3) return false;
		return v1.equals(p.v1) && v2.equals(p.v2) && v3.equals(p.v3);
	}

	@Override
	public int hashCode() {
		return Pair.hashCode(v1, v2, v3);
	}
}
