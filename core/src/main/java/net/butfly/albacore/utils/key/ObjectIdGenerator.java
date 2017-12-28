package net.butfly.albacore.utils.key;

import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import net.butfly.albacore.utils.Texts;

/**
 * Copy ObjectId implementation from MongoDB.
 * 
 * @author butfly
 */
public class ObjectIdGenerator extends IdGenerator<String> {
	public static final ObjectIdGenerator GEN = new ObjectIdGenerator();
	private static final AtomicInteger _nextInc = new AtomicInteger((new java.util.Random()).nextInt());
	private static int _genmachine;

	public ObjectIdGenerator() {
		_genmachine = (int) machine();
	}

	@Override
	public byte[] bytes() {
		final int _time;
		final int _inc;
		_time = (int) (System.currentTimeMillis() / 1000);
		_inc = _nextInc.getAndIncrement();

		byte bytes[] = new byte[12];
		ByteBuffer buf = ByteBuffer.wrap(bytes);
		// by default BB is big endian like we need
		buf.putInt(_time);
		buf.putInt(_genmachine);
		buf.putInt(_inc);
		return bytes;
	}

	@Override
	public String generate() {
		return Texts.byte2hex(bytes());
	}

	@Override
	public String gen0() {
		return new String(encode(ObjectIdGenerator.GEN.bytes(), 16));
	}

	@Override
	public final long machine() {
		try {
			// build a 2-byte machine piece based on NICs info
			int machinePiece;
			{
				try {
					StringBuilder sb = new StringBuilder();
					Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
					while (e.hasMoreElements()) {
						NetworkInterface ni = e.nextElement();
						sb.append(ni.toString());
					}
					machinePiece = sb.toString().hashCode() << 16;
				} catch (Throwable e) {
					// exception sometimes happens with IBM JVM, use random
					logger().warn(e.getMessage(), e);
					machinePiece = (new Random().nextInt()) << 16;
				}
				logger().trace("machine piece post: " + Integer.toHexString(machinePiece));
			}

			// add a 2 byte process piece. It must represent not only the JVM
			// but the class loader.
			// Since static var belong to class loader there could be collisions
			// otherwise
			final int processPiece;
			{
				int processId = new java.util.Random().nextInt();
				try {
					processId = java.lang.management.ManagementFactory.getRuntimeMXBean().getName().hashCode();
				} catch (Throwable t) {}

				ClassLoader loader = Thread.currentThread().getContextClassLoader();
				int loaderId = loader != null ? System.identityHashCode(loader) : 0;

				StringBuilder sb = new StringBuilder();
				sb.append(Integer.toHexString(processId));
				sb.append(Integer.toHexString(loaderId));
				processPiece = sb.toString().hashCode() & 0xFFFF;
				logger().trace("process piece: " + Integer.toHexString(processPiece));
			}

			int result = machinePiece | processPiece;
			logger().trace("machine : " + Integer.toHexString(_genmachine));
			return result;
		} catch (Exception e) {
			throw new GetHardwareIdFailed(e);
		}
	}
}