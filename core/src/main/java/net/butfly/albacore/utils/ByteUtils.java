package net.butfly.albacore.utils;


public final class ByteUtils extends UtilsBase {
	public static String byte2hex(byte[] data) {
		if (null == data) return null;
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < data.length; i++) {
			String temp = Integer.toHexString(((int) data[i]) & 0xFF);
			for (int t = temp.length(); t < 2; t++)
				sb.append("0");
			sb.append(temp);
		}
		return sb.toString();
	}

	public static byte[] hex2byte(String hexStr) {
		if (null == hexStr) { return null; }
		byte[] bts = new byte[hexStr.length() / 2];
		int i = 0;
		int j = 0;
		for (; j < bts.length; j++) {
			bts[j] = (byte) Integer.parseInt(hexStr.substring(i, i + 2), 16);
			i += 2;
		}
		return bts;
	}
}
