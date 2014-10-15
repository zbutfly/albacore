package net.butfly.albacore.utils.http;

import java.util.concurrent.TimeUnit;

import net.butfly.albacore.exception.SystemException;

import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.reactor.IOReactorException;
import org.reflections.util.Utils;

public class HttpClientUtils extends Utils {
	private static HttpClientConnectionManager connman;
	private static NHttpClientConnectionManager asyncman;
	static {
		long time = Long.parseLong(System.getProperty("albacore.http.pool.live", "60"));
		connman = new PoolingHttpClientConnectionManager(time, TimeUnit.SECONDS);
		try {
			asyncman = new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor());
		} catch (IOReactorException e) {
			throw new SystemException("", e);
		}
	}

	public static CloseableHttpClient create() {
		return HttpClients.createMinimal(connman);
	}

	public static CloseableHttpAsyncClient createAsync() {
		return HttpAsyncClients.createMinimal(asyncman);
	}
}
