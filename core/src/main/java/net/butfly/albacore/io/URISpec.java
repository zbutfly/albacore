package net.butfly.albacore.io;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Joiner;

import net.butfly.albacore.utils.Pair;

public final class URISpec {
	private final List<String> schemas;
	private final String username;
	private final String password;
	private final List<Pair<String, Integer>> hosts;
	private final List<String> paths;
	private final Properties query;
	private final String fragment;

	public URISpec(String str) {
		this(str, -1);
	}

	public URISpec(String str, int defaultPort) {
		super();
		schemas = new ArrayList<>();
		URI uri = parseURI(str, schemas);
		hosts = new ArrayList<>();
		if (uri.getHost() != null) {
			String[] u = uri.getUserInfo().split(":", 2);
			username = u[0];
			password = u.length == 1 ? null : u[1];
			hosts.add(new Pair<>(uri.getHost(), uri.getPort()));
		} else {
			String[] a = uri.getAuthority().split("@", 2);
			if (a.length == 1) {
				username = null;
				password = null;
				fillHostPort(a[0], defaultPort);
			} else {
				String[] up = a[0].split(":", 2);
				username = up[0];
				password = up.length == 1 ? null : up[1];
				fillHostPort(a[1], defaultPort);
			}
		}
		String p = uri.getPath();
		paths = null != p ? Arrays.asList(uri.getPath().replaceAll("^/", "").split("/")) : new ArrayList<>();
		query = new Properties();
		if (null != uri.getQuery()) for (String q : uri.getQuery().split("&")) {
			String[] kv = q.split("=", 2);
			query.setProperty(kv[0], kv.length == 1 ? "" : kv[1]);
		}
		fragment = uri.getFragment();
	}

	private void fillHostPort(String hostPorts, int defaultPort) {
		for (String hostPort : hostPorts.split(",")) {
			String[] hp = hostPort.split(":", 2);
			hosts.add(new Pair<>(hp[0], hp.length == 1 ? defaultPort : Integer.parseInt(hp[1])));
		}
	}

	private static URI parseURI(String spec, List<String> schemas) {
		URI uri;
		try {
			uri = new URI(spec);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
		schemas.add(uri.getScheme());
		if (uri.getAuthority() != null) return uri;
		return parseURI(uri.getSchemeSpecificPart(), schemas);
	}

	public String getSchema() {
		return schemas.isEmpty() ? null : new StringBuilder().append(Joiner.on(':').join(schemas)).toString();
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public Iterator<Pair<String, Integer>> getHosts() {
		return hosts.iterator();
	}

	public String getPath() {
		return paths.isEmpty() ? null : new StringBuilder().append('/').append(Joiner.on('/').join(paths)).toString();
	}

	public String getQuery() {
		return query.isEmpty() ? null
				: new StringBuilder().append('?').append(Joiner.on('&').join(query.entrySet().stream().map(e -> e.getKey().toString() + "="
						+ e.getValue().toString()).iterator())).toString();
	}

	public String getParameter(String name) {
		return query.getProperty(name);
	}

	public String getParameter(String name, String defaultValue) {
		return query.getProperty(name, defaultValue);
	}

	public String getFragment() {
		return fragment;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (!schemas.isEmpty()) sb.append(getSchema()).append(':');
		sb.append("//");
		if (null != username) {
			sb.append(username);
			if (null != password) sb.append(':').append(password);
			sb.append('@');
		}
		sb.append(Joiner.on(',').join(hosts.stream().map(p -> p.value1() + (p.value2() <= 0 ? "" : (":" + p.value2()))).iterator()));
		if (!paths.isEmpty()) sb.append(getPath());
		if (!query.isEmpty()) sb.append(getQuery());
		if (fragment != null) sb.append('#').append(fragment);
		return sb.toString();
	}

	public URI toURI() throws URISyntaxException {
		return new URI(toString());
	}
}