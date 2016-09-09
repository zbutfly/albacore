package net.butfly.albacore.utils.key;

import static java.lang.System.exit;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

/**
 * SnowflakeIdGenerator
 *
 * @author Maxim Khodanovich
 * @version 21.01.13 17:16
 *
 *          id is composed of: <br />
 *          time - 41 bits (millisecond precision w/ a custom epoch gives us 69
 *          years) <br />
 *          configured machine id - 10 bits - gives us up to 1024 machines
 *          <br />
 *          sequence number - 12 bits - rolls over every 4096 per machine (with
 *          protection to avoid rollover in the same ms)
 *
 */
public class SnowflakeIdGenerator extends IdGenerator<Long> {
	private final long datacenterIdBits = 10L;
	private final long maxDatacenterId = -1L ^ (-1L << datacenterIdBits);
	private final long timestampBits = 41L;
	private final long datacenterIdShift = 64L - datacenterIdBits;
	private final long timestampLeftShift = 64L - datacenterIdBits - timestampBits;
	private final long sequenceMax = 4096;
	private final long twepoch = 1288834974657L;
	private final long datacenterId;
	private volatile long lastTimestamp = -1L;
	private volatile long sequence = 0L;

	public SnowflakeIdGenerator() throws GetHardwareIdFailed {
		datacenterId = machine();
		if (datacenterId > maxDatacenterId || datacenterId < 0) throw new GetHardwareIdFailed("datacenterId > maxDatacenterId");
	}

	@Override
	public synchronized Long generate() {
		long timestamp = System.currentTimeMillis();
		if (timestamp < lastTimestamp) throw new InvalidSystemClock("Clock moved backwards. Refusing to generate id for " + (lastTimestamp
				- timestamp) + " milliseconds.");
		if (lastTimestamp == timestamp) {
			sequence = (sequence + 1) % sequenceMax;
			if (sequence == 0) {
				timestamp = tilNextMillis(lastTimestamp);
			}
		} else {
			sequence = 0;
		}
		lastTimestamp = timestamp;
		return ((timestamp - twepoch) << timestampLeftShift) | (datacenterId << datacenterIdShift) | sequence;
	}

	protected long tilNextMillis(long lastTimestamp) {
		long timestamp = System.currentTimeMillis();
		while (timestamp <= lastTimestamp) {
			timestamp = System.currentTimeMillis();
		}
		return timestamp;
	}

	@Override
	protected long machine() throws GetHardwareIdFailed {
		try {
			InetAddress ip = InetAddress.getLocalHost();
			NetworkInterface network = NetworkInterface.getByInetAddress(ip);
			byte[] mac = network.getHardwareAddress();
			// System.out.println(DatatypeConverter.printHexBinary(mac));
			long id = ((0x000000FF & (long) mac[mac.length - 1]) | (0x0000FF00 & (((long) mac[mac.length - 2]) << 8))) >> 6;
			// System.out.println(id);
			return id;
		} catch (SocketException e) {
			throw new GetHardwareIdFailed(e);
		} catch (UnknownHostException e) {
			throw new GetHardwareIdFailed(e);
		}
	}

	public static void main(String[] args) throws GetHardwareIdFailed, InvalidSystemClock {
		SnowflakeIdGenerator generator = new SnowflakeIdGenerator();
		int n = Integer.parseInt(args[0]);
		Set<Long> ids = new HashSet<Long>();
		for (int i = 0; i < n; i++) {
			long id = generator.generate();
			if (ids.contains(id)) {
				System.out.println("Duplicate id:" + id);
				exit(1);
			}
			ids.add(id);
			System.out.println(id);
		}
	}

}