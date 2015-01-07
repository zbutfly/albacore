package net.butfly.albacore.utils;

import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyUtils extends UtilsBase {
	public static String uuid() {
		return UUID.randomUUID().toString();
	}

	public static String objectId() {
		return ObjectIdGenerator.generateKey();
	}

	public static String join(String[] list) {
		return join(list, ",");
	}

	public static String join(String[] list, String split) {
		StringBuilder sb = new StringBuilder();
		for (String tt : list)
			sb.append(tt).append(split);
		return sb.substring(0, sb.length() - split.length());
	}

	/**
	 * Copy ObjectId implementation from MongoDB.
	 * 
	 * @author butfly
	 */
	private static class ObjectIdGenerator {
		private static final Logger LOGGER = LoggerFactory.getLogger("org.bson.ObjectID");
		private static AtomicInteger _nextInc = new AtomicInteger((new java.util.Random()).nextInt());
		private static final int _genmachine = generateMachine();

		private static String generateKey() {
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

			return ByteUtils.byte2hex(bytes);
		}

		private static int generateMachine() {
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
						LOGGER.warn(e.getMessage(), e);
						machinePiece = (new Random().nextInt()) << 16;
					}
					LOGGER.trace("machine piece post: " + Integer.toHexString(machinePiece));
				}

				// add a 2 byte process piece. It must represent not only the JVM but the class loader.
				// Since static var belong to class loader there could be collisions otherwise
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
					LOGGER.trace("process piece: " + Integer.toHexString(processPiece));
				}

				int result = machinePiece | processPiece;
				LOGGER.trace("machine : " + Integer.toHexString(_genmachine));
				return result;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
