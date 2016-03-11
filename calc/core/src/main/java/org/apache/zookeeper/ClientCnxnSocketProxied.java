package org.apache.zookeeper;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public class ClientCnxnSocketProxied extends ClientCnxnSocketNIO {
	ClientCnxnSocketProxied() throws IOException {
		super();
	}

	/**
	 * create a socket channel.
	 * 
	 * @return the created socket channel
	 * @throws IOException
	 */
	@Override
	SocketChannel createSock() throws IOException {
		SocketChannel sock;
		sock = SocketChannel.open();
		sock.configureBlocking(false);
		sock.socket().setSoLinger(false, -1);
		sock.socket().setTcpNoDelay(true);
		return sock;
	}
}
