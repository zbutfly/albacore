package net.butfly.albacore.utils.http;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.utils.UtilsBase;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.reactor.IOReactorException;

public class HTTPAsyncUtils extends UtilsBase {
	private static NHttpClientConnectionManager asyncman;
	static {
		try {
			asyncman = new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor());
		} catch (IOReactorException e) {
			throw new SystemException("", e);
		}
	}

	public static CloseableHttpAsyncClient create() {
		return HttpAsyncClients.createMinimal(asyncman);
	}
}
