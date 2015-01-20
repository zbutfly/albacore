package net.butfly.albacore.utils.http;

import java.util.concurrent.TimeUnit;

import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;

@Deprecated
public final class HttpClientFactory {
	private static final ClientConnectionManager sharedConnectionManager = createSharedConnectionManager();
	private static final HttpClient sharedClient = new DefaultHttpClient(sharedConnectionManager, createDefaultHttpParameters());
	private static final int DEFAULT_MAX_CONNECTIONS_LIMIT = 100;

	private HttpClientFactory() {}

	public static void cleanup() {
		sharedConnectionManager.closeExpiredConnections();
		sharedConnectionManager.closeIdleConnections(0, TimeUnit.SECONDS);
	}

	public static HttpClient getSharedClient() {
		return sharedClient;
	}

	public static HttpClient createSafeHttpClient() {
		return new DefaultHttpClient(sharedConnectionManager, createDefaultHttpParameters());
	}

	public static HttpClient createSingleHttpClient() {
		return new DefaultHttpClient(createDefaultHttpParameters());
	}

	private static ClientConnectionManager createSharedConnectionManager() {
		SchemeRegistry reg = new SchemeRegistry();
		reg.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
		reg.register(new Scheme("https", 443, SSLSocketFactory.getSocketFactory()));
		ThreadSafeClientConnManager r = new ThreadSafeClientConnManager(reg);
		r.setMaxTotal(DEFAULT_MAX_CONNECTIONS_LIMIT);
		return r;
	}

	private static HttpParams createDefaultHttpParameters() {
		HttpParams params = new BasicHttpParams();
		HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
		HttpProtocolParams.setContentCharset(params, HTTP.DEFAULT_CONTENT_CHARSET);
		HttpProtocolParams.setUseExpectContinue(params, true);
		return params;
	}
}
