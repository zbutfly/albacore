package net.butfly.bus.test;

import java.io.Serializable;
import java.util.Random;

public class Bean implements Serializable {
	private static final long serialVersionUID = -2963162163893587423L;

	public enum Enums {
		V1, V2, V3
	}

	public int number;
	public long size;
	public Enums type;
	public String title;
	public Bean bean;

	public Bean() {
		this(true);
	}

	static Random r = new Random();

	private Bean(boolean embed) {
		super();
		this.number = r.nextInt(10);
		this.size = r.nextLong();
		this.title = Double.toHexString(r.nextGaussian());
		this.type = Enums.values()[r.nextInt(3)];
		bean = embed ? new Bean(false) : null;
	}

	public String titles() {
		return title + (null == bean ? "" : " / " + bean.title);
	}
}
