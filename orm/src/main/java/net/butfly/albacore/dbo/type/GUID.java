package net.butfly.albacore.dbo.type;

import java.io.Serializable;
import java.security.SecureRandom;

import net.butfly.albacore.exception.SystemException;

public class GUID implements Serializable {
	private static final long serialVersionUID = 3588060775997379504L;
	private static final int GUID_BITS_SIZE = 128;
	private static final int GUID_BUFF_SIZE = GUID_BITS_SIZE / 8;
	private static volatile SecureRandom numberGenerator = null;
	private byte[] data;


	public GUID(byte[] data) {
		if (null == data || GUID_BUFF_SIZE != data.length)
			throw new SystemException("SYS_100", "GUID should be initialized by a " + GUID_BUFF_SIZE + " byte buffer.");
		this.data = data;
	}

	public GUID(String hex) {
		this.data = new byte[GUID_BUFF_SIZE];
		int i = 0;
		for (int j = 0; j < GUID_BUFF_SIZE; j++) {
			this.data[j] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
			i += 2;
		}
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < data.length; i++) {
			String temp = Integer.toHexString(((int) data[i]) & 0xFF);
			for (int t = temp.length(); t < 2; t++) {
				sb.append("0");
			}
			sb.append(temp);
		}
		return sb.toString();
	}

	public byte[] data() {
		return this.data;
	}

	public static GUID randomGUID() {
		SecureRandom ng = numberGenerator;
		if (null == ng) numberGenerator = ng = new SecureRandom();

		byte[] randomBytes = new byte[GUID_BUFF_SIZE];
		ng.nextBytes(randomBytes);
		// randomBytes[6] &= 0x0f; /* clear version */
		// randomBytes[6] |= 0x40; /* set to version 4 */
		// randomBytes[8] &= 0x3f; /* clear variant */
		// randomBytes[8] |= 0x80; /* set to IETF variant */
		return new GUID(randomBytes);
	}
}
