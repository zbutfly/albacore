package net.butfly.albacore.utils.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Proxy.Type;
import java.nio.channels.SocketChannel;

import sun.nio.ch.WindowsSelectorProvider;

@SuppressWarnings("restriction")
public class ProxableSelectorProvider extends WindowsSelectorProvider {
	@Override
	public SocketChannel openSocketChannel() throws IOException {
		boolean socks = Boolean.parseBoolean(System.getProperty("socksProxySet"));
		if (socks) return new ProxiedSocketChannelImpl(new Proxy(Type.SOCKS, new InetSocketAddress(
				System.getProperty("socksProxyHost", "localhost"), Integer.parseInt(System.getProperty("socksProxyPort", "7070")))));
		boolean http = Boolean.parseBoolean(System.getProperty("httpProxySet"));
		if (http)
			return new ProxiedSocketChannelImpl(new Proxy(Type.HTTP, new InetSocketAddress(System.getProperty("httpProxyHost", "localhost"),
					Integer.parseInt(System.getProperty("httpProxyPort", "8080")))));
		return super.openSocketChannel();
	}
}
