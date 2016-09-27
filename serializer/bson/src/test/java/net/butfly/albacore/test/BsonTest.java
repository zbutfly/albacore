//package net.butfly.albacore.test;
//
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.Serializable;
//
//import org.apache.commons.lang3.RandomStringUtils;
//import org.bson.BSONObject;
//
//import com.google.common.reflect.TypeToken;
//import com.mongodb.BasicDBObject;
//
//import net.butfly.albacore.serder.BsonObjectSerder;
//import net.butfly.albacore.serder.BsonSerder;
//import net.butfly.albacore.serder.Serder;
//
//public class BsonTest {
//	public static void main(String... arg) throws IOException {
//		Serder<Object, BSONObject> ser = new BsonSerder().then(new BsonObjectSerder(), TypeToken.of(ByteArrayOutputStream.class));
//
//		Bean o = new Bean();
//		BSONObject s = ser.ser(o);
//		System.out.println("Origin: " + ((BasicDBObject) s).toJson());
//		System.out.println("BSON: " + o.titles() + " => " + ser.der(s, TypeToken.of(Bean.class)).titles());
//
//		// System.out.println("BSON args: " + o.titles() + " => " + ((Bean)
//		// ser.der(ser.ser(new Object[] { o, 1, true }), Bean.class,
//		// int.class, boolean.class)[0]).titles());
//	}
//
//	public enum Enums {
//		V1, V2, V3
//	}
//
//	public static class Bean implements Serializable {
//		private static final long serialVersionUID = -2963162163893587423L;
//		public int number;
//		public long size;
//		public Enums type;
//		public String title;
//		public Bean bean;
//
//		public Bean() {
//			this(true);
//		}
//
//		private Bean(boolean embed) {
//			super();
//			this.number = (int) (Math.random() * 10);
//			this.size = (long) (Math.random() * 10);
//			this.title = RandomStringUtils.randomAlphanumeric(16);
//			this.type = Enums.values()[(int) (Math.random() * 3)];
//			bean = embed ? new Bean(false) : null;
//		}
//
//		public String titles() {
//			return title + (null == bean ? "" : " / " + bean.title);
//		}
//	}
//}
