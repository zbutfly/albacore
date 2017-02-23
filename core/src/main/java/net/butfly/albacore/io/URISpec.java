package net.butfly.albacore.io;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import net.butfly.albacore.utils.Pair;

public class URISpec {
	protected final List<String> schemes;
	protected final String username;
	protected final String password;
	protected final List<Pair<String, Integer>> hosts;
	protected final List<String> paths;
	protected final Map<String, String> query;
	protected final String fragment;

	public URISpec(String str) {
		this(str, -1);
	}

	public URISpec(String str, int defaultPort) {
		super();
		schemes = new ArrayList<>();
		URI uri = parseURI(str, schemes);
		hosts = new ArrayList<>();
		if (uri.getHost() != null) {
			if (null != uri.getUserInfo()) {
				String[] u = uri.getUserInfo().split(":", 2);
				username = u[0];
				password = u.length == 1 ? null : u[1];
			} else {
				username = null;
				password = null;
			}
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
		paths = null == p ? new ArrayList<>()
				: Stream.of(uri.getPath().split("/")).filter(seg -> seg.trim().length() > 0).collect(Collectors.toList());
		query = new ConcurrentHashMap<>();
		if (null != uri.getQuery()) for (String q : uri.getQuery().split("&")) {
			String[] kv = q.split("=", 2);
			query.put(kv[0], kv.length == 1 ? "" : kv[1]);
		}
		fragment = uri.getFragment();
	}

	private void fillHostPort(String hostPorts, int defaultPort) {
		for (String hostPort : hostPorts.split(",")) {
			String[] hp = hostPort.split(":", 2);
			hosts.add(new Pair<>(hp[0], hp.length == 1 ? defaultPort : Integer.parseInt(hp[1])));
		}
	}

	private static URI parseURI(String spec, List<String> schemes) {
		URI uri;
		try {
			uri = new URI(spec);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
		schemes.add(uri.getScheme());
		if (uri.getAuthority() != null) return uri;
		return parseURI(uri.getSchemeSpecificPart(), schemes);
	}

	public String getScheme() {
		return schemes.isEmpty() ? null : new StringBuilder().append(Joiner.on(':').join(schemes)).toString();
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public List<Pair<String, Integer>> getHosts() {
		return ImmutableList.copyOf(hosts);
	}

	public String getHost() {
		return Joiner.on(',').join(Streams.of(hosts).map(p -> p.v1() + (p.v2() <= 0 ? "" : (":" + p.v2()))).iterator());
	}

	public String[] getPaths() {
		return paths.toArray(new String[paths.size()]);
	}

	private String join(String[] segs) {
		return segs == null || segs.length == 0 ? null : Joiner.on('/').join(segs);
	}

	public String getPathAt(int at, String... defaults) {
		return at >= paths.size() || at < 0 ? join(defaults) : paths.get(at);
	}

	public String getPathAtLast(int at, String... defaults) {
		return getPathAt(paths.size() - at - 1, defaults);
	}

	public String getPath(int segs) {
		return join((segs > paths.size() ? paths : paths.subList(0, segs)).toArray(new String[paths.size()]));
	}

	public String getPathSkip(int segs) {
		return join((segs >= paths.size() ? null : paths.subList(segs, paths.size())).toArray(new String[paths.size()]));
	}

	public String getPathLast(int segs) {
		return join((segs > paths.size() ? paths : paths.subList(paths.size() - segs, paths.size())).toArray(new String[paths.size()]));
	}

	public String getPath() {
		return paths.isEmpty() ? null : "/" + join(paths.toArray(new String[paths.size()]));
	}

	public String getQuery() {
		return query.isEmpty() ? null
				: new StringBuilder().append(Joiner.on('&').join(Streams.of(query.entrySet()).map(e -> e.getKey().toString() + "=" + e
						.getValue().toString()).iterator())).toString();
	}

	public Map<String, String> getParameters() {
		return query;
	}

	public String getParameter(String name) {
		return query.get(name);
	}

	public String getParameter(String name, String defaultValue) {
		return query.getOrDefault(name, defaultValue);
	}

	public String getFragment() {
		return fragment;
	}

	public String getAuthority() {
		StringBuilder sb = new StringBuilder();
		if (null != username) {
			sb.append(username);
			if (null != password) sb.append(':').append(password);
			sb.append('@');
		}
		sb.append(getHost());
		return sb.toString();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (!schemes.isEmpty()) sb.append(getScheme()).append(':');
		sb.append("//");
		sb.append(getAuthority());
		if (!paths.isEmpty()) sb.append(getPath());
		if (!query.isEmpty()) sb.append('?').append(getQuery());
		if (fragment != null) sb.append('#').append(fragment);
		return sb.toString();
	}

	public URI toURI() {
		try {
			return new URI(toString());
		} catch (URISyntaxException e) {
			return null;
		}
	}
}