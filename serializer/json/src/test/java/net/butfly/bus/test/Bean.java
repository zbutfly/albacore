package net.butfly.bus.test;

import java.io.Serializable;

import org.apache.commons.lang3.RandomStringUtils;

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

	private Bean(boolean embed) {
		super();
		this.number = (int) (Math.random() * 10);
		this.size = (long) (Math.random() * 1000);
		this.title = RandomStringUtils.randomAlphanumeric(16);
		this.type = Enums.values()[(int) (Math.random() * 3)];
		bean = embed ? new Bean(false) : null;
	}

	public String titles() {
		return title + (null == bean ? "" : " / " + bean.title);
	}
}
