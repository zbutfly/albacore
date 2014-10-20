package net.butfly.albacore.utils.http;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import net.butfly.albacore.utils.UtilsBase;

import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.HttpConnectionFactory;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.BrowserCompatHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultHttpResponseParser;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.http.impl.io.DefaultHttpRequestWriterFactory;
import org.apache.http.impl.io.DefaultHttpResponseParserFactory;
import org.apache.http.io.HttpMessageParser;
import org.apache.http.io.HttpMessageParserFactory;
import org.apache.http.io.HttpMessageWriterFactory;
import org.apache.http.io.SessionInputBuffer;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicLineParser;
import org.apache.http.message.LineParser;
import org.apache.http.util.CharArrayBuffer;

public class HTTPUtils extends UtilsBase {
	private static final int DEFAULT_TIMEOUT = 5000;
	private static HttpClientConnectionManager connman;
	static {
		long time = Long.parseLong(System.getProperty("albacore.http.pool.live", "60"));
		connman = new PoolingHttpClientConnectionManager(time, TimeUnit.SECONDS);
	}

	public static CloseableHttpClient create() {
		return HttpClients.createMinimal(connman);
	}

	public static CloseableHttpClient createFull(int timeout, int maxConnections) {
		return createFull(timeout, maxConnections, -1, null);
	}

	public static CloseableHttpClient createFull(int timeout, int maxConnections, int maxPerRoute,
			Map<String, InetAddress[]> dnsMap) {
		// Use custom message parser / writer to customize the way HTTP messages are parsed from and written
		// out to the data stream.
		HttpMessageParserFactory<HttpResponse> responseParserFactory = new DefaultHttpResponseParserFactory() {
			@Override
			public HttpMessageParser<HttpResponse> create(SessionInputBuffer buffer, MessageConstraints constraints) {
				LineParser lineParser = new BasicLineParser() {
					@Override
					public Header parseHeader(final CharArrayBuffer buffer) {
						try {
							return super.parseHeader(buffer);
						} catch (ParseException ex) {
							return new BasicHeader(buffer.toString(), null);
						}
					}
				};
				return new DefaultHttpResponseParser(buffer, lineParser, DefaultHttpResponseFactory.INSTANCE, constraints) {
					@Override
					protected boolean reject(final CharArrayBuffer line, int count) {
						// try to ignore all garbage preceding a status line infinitely
						return false;
					}
				};
			}
		};
		HttpMessageWriterFactory<HttpRequest> requestWriterFactory = new DefaultHttpRequestWriterFactory();

		// Use a custom connection factory to customize the process of initialization of outgoing HTTP
		// connections. Beside standard connection configuration parameters HTTP connection factory can
		// define message parser / writer routines to be employed by individual connections.
		HttpConnectionFactory<HttpRoute, ManagedHttpClientConnection> connFactory = new ManagedHttpClientConnectionFactory(
				requestWriterFactory, responseParserFactory);

		// Client HTTP connection objects when fully initialized can be bound to an arbitrary network
		// socket. The process of network socket initialization, its connection to a remote address and
		// binding to a local one is controlled by a connection socket factory.

		// SSL context for secure connections can be created either based on system or application specific
		// properties.
		SSLContext sslcontext = SSLContexts.createSystemDefault();
		// Use custom hostname verifier to customize SSL hostname verification.
		X509HostnameVerifier hostnameVerifier = new BrowserCompatHostnameVerifier();

		// Create a registry of custom connection socket factories for supported protocol schemes.
		Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory> create()
				.register("http", PlainConnectionSocketFactory.INSTANCE)
				.register("https", new SSLConnectionSocketFactory(sslcontext, hostnameVerifier)).build();

		// Use custom DNS resolver to override the system DNS resolution.
		DnsResolver dnsResolver = new SystemDefaultDnsResolver() {
			@Override
			public InetAddress[] resolve(final String host) throws UnknownHostException {
				if (dnsMap != null && dnsMap.containsKey(host)) return dnsMap.get(host);
				return super.resolve(host);
			}
		};

		// Create a connection manager with custom configuration.
		PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry,
				connFactory, dnsResolver);

		// Create socket configuration
		SocketConfig socketConfig = SocketConfig.custom().setTcpNoDelay(true).build();
		// Configure the connection manager to use socket configuration either by default or for a specific
		// host.
		connManager.setDefaultSocketConfig(socketConfig);
		// XXX: connManager.setSocketConfig(new HttpHost("somehost", 80), socketConfig);

		// Create message constraints
		MessageConstraints messageConstraints = MessageConstraints.custom().setMaxHeaderCount(200).setMaxLineLength(2000)
				.build();
		// Create connection configuration
		ConnectionConfig connectionConfig = ConnectionConfig.custom().setMalformedInputAction(CodingErrorAction.IGNORE)
				.setUnmappableInputAction(CodingErrorAction.IGNORE).setCharset(Consts.UTF_8)
				.setMessageConstraints(messageConstraints).build();
		// Configure the connection manager to use connection configuration either by default or for a
		// specific host.
		connManager.setDefaultConnectionConfig(connectionConfig);
		// XXX: connManager.setConnectionConfig(new HttpHost("somehost", 80), ConnectionConfig.DEFAULT);

		// Configure total max or per route limits for persistent connections that can be kept in the pool
		// or leased by the connection manager.
		if (maxConnections > 0) connManager.setMaxTotal(maxConnections);
		if (maxPerRoute > 0) connManager.setDefaultMaxPerRoute(maxPerRoute);
		// XXX: connManager.setMaxPerRoute(new HttpRoute(new HttpHost("somehost", 80)), 20);
		connManager.setValidateAfterInactivity(50);

		// Use custom cookie store if necessary.
		CookieStore cookieStore = new BasicCookieStore();
		// Use custom credentials provider if necessary.
		CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		// Create global request configuration
		if (timeout <= 0) timeout = DEFAULT_TIMEOUT;
		RequestConfig defaultRequestConfig = RequestConfig.custom().setCookieSpec(CookieSpecs.BEST_MATCH)
				.setExpectContinueEnabled(true)
				.setTargetPreferredAuthSchemes(Arrays.asList(AuthSchemes.NTLM, AuthSchemes.DIGEST))
				.setProxyPreferredAuthSchemes(Arrays.asList(AuthSchemes.BASIC)).setSocketTimeout(timeout)
				.setConnectTimeout(timeout).setConnectionRequestTimeout(timeout).build();
		CloseableHttpClient c = HttpClients.custom().setConnectionManager(connManager).setDefaultCookieStore(cookieStore)
				.setDefaultCredentialsProvider(credentialsProvider).setDefaultRequestConfig(defaultRequestConfig).build();
		// XXX: .setProxy(new HttpHost("myproxy", 8080))
		return c;
	}
}
