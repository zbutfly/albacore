package net.butfly.albacore.io;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class UriSocketAddress extends InetSocketAddress {
	private static final long serialVersionUID = 7907073418302217762L;
	public static final int UNDEFINED_PORT = -1;
	private final int[] secondaryPorts;

	public UriSocketAddress(InetAddress addr, int mainPort, int... secondaryPorts) {
		super(addr, mainPort);
		this.secondaryPorts = secondaryPorts;
	}

	public UriSocketAddress(int mainPort, int... secondaryPorts) {
		super(mainPort);
		this.secondaryPorts = secondaryPorts;
	}

	public UriSocketAddress(String hostname, int mainPort, int... secondaryPorts) {
		super(hostname, mainPort);
		this.secondaryPorts = secondaryPorts;
	}

	public int getPort(int i) {
		if (i <= 0) return this.getPort();
		if (i > secondaryPorts.length) i = secondaryPorts.length;
		return secondaryPorts[i - 1];
	}
}
