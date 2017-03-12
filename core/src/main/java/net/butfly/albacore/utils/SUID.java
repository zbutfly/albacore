package net.butfly.albacore.utils;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.utils.key.ObjectIdGenerator;
import net.butfly.albacore.utils.key.SnowflakeIdGenerator;
import net.butfly.albacore.utils.key.UUIDGenerator;

/**
 * Short unique id, 12 byte (ObjectId) and 16 char (a-zA-Z0-9_-)
 * 
 * @author butfly
 */
public final class SUID extends Utils {
	private SUID() {
		super();
	}

	public static String snowflake() {
		return SnowflakeIdGenerator.GEN.gen0();
	}

	public static String objid() {
		return ObjectIdGenerator.GEN.gen0();
	}

	/**
	 * @param len
	 *            suggestion: 12 (most half of uuid) or 24 (whole of uuid), but
	 *            should be divided into 4
	 * @return
	 */
	public static String uuid(int len) {
		return UUIDGenerator.GEN.gen(len);
	}

	public static void main(String... args) {
		ConcurrentSkipListSet<String> ids = new ConcurrentSkipListSet<>();
		int n = 2;
		Thread[] t = new Thread[n];
		long now = System.currentTimeMillis();
		AtomicLong c = new AtomicLong();
		for (int i = 0; i < n; i++)
			t[i] = new Thread(() -> {
				while (true) {
					String s = snowflake();// uuid(12);
					if (ids.contains(s)) {
						System.out.println(Thread.currentThread().getName() + ": " + s + "[" + s.length() + "], duplicated #" + ids.size());
						Thread.currentThread().interrupt();
					} else {
						double sp = ((double) (System.currentTimeMillis() - now)) / c.incrementAndGet();
						System.out.println(Thread.currentThread().getName() + ": " + s + "[" + s.length() + "] #" + ids.size() + ", avg: "
								+ sp + " ms/id");
						ids.add(s);
					}
				}
			});
		for (Thread tt : t)
			tt.start();
		for (Thread tt : t)
			try {
				tt.join();
			} catch (InterruptedException e) {}
	}
}
