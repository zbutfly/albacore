package net.butfly.albacore.test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang3.RandomStringUtils;
import org.bson.BSONObject;

import net.butfly.albacore.serder.BsonObjectSerder.MongoSerder;

public class BsonTest {
	public static void main(String... arg) throws IOException {

		Bean o = new Bean();
		BSONObject s = MongoSerder.DEFAULT.ser(o);
		s.put("now", new Date());
		System.out.println("Origin: " + s.get("title") + s.get("now"));
		System.out.println("BSON: " + o.titles() + " => " + MongoSerder.DEFAULT.der(s, Bean.class).titles());

		// System.out.println("BSON args: " + o.titles() + " => " + ((Bean)
		// ser.der(ser.ser(new Object[] { o, 1, true }), Bean.class,
		// int.class, boolean.class)[0]).titles());
	}

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
		public Date now;

		public Bean() {
			this(true);
		}

		private Bean(boolean embed) {
			super();
			this.number = (int) (Math.random() * 10);
			this.size = (long) (Math.random() * 10);
			this.title = RandomStringUtils.randomAlphanumeric(16);
			this.type = Enums.values()[(int) (Math.random() * 3)];
			bean = embed ? new Bean(false) : null;
			now = new Date();
		}

		public String titles() {
			return title + (null == bean ? "" : " / " + bean.title);
		}
	}
}
