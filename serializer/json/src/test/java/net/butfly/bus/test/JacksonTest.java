package net.butfly.bus.test;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.lang3.RandomStringUtils;

import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.serder.JsonSerder;

public class JacksonTest {
	public enum Enums {
		V1, V2, V3
	}

	public static class Bean implements Serializable {
		private static final long serialVersionUID = -2963162163893587423L;
		public int number;
		public long size;
		public Enums type;
		public String title;
		public Bean bean;

		private Bean() {
			this(true);
		}

		private Bean(boolean embed) {
			super();
			this.number = (int) (Math.random() * 10);
			this.size = (long) (Math.random() * 10);
			this.title = RandomStringUtils.randomAlphanumeric(16);
			this.type = Enums.values()[(int) (Math.random() * 3)];
			bean = embed ? new Bean(false) : null;
		}

		public String titles() {
			return title + (null == bean ? "" : " / " + bean.title);
		}
	}

	public static void main(String... arg) throws IOException {
		final JsonSerder jsd = new JsonSerder();
		final BsonSerder bsd = new BsonSerder();

		Bean o = new Bean();
		String s = jsd.ser(o);
		System.out.println("Origin: " + s);
		System.out.println("JSON: " + o.titles() + " => " + jsd.der(s, Bean.class).titles());
		System.out.println("BSON: " + o.titles() + " => " + bsd.der(bsd.ser(o), Bean.class).titles());

		System.out.println("JSON args: " + o.titles() + " => " + ((Bean) jsd.der(jsd.ser(new Object[] { o, 1, true }), Bean.class,
				int.class, boolean.class)[0]).titles());
		System.out.println("BSON args: " + o.titles() + " => " + ((Bean) bsd.der(bsd.ser(new Object[] { o, 1, true }), Bean.class,
				int.class, boolean.class)[0]).titles());
	}
}
