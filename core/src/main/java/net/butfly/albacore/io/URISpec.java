package net.butfly.albacore.io;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import net.butfly.albacore.utils.Pair;

/**
 * Parse URI string like:
 * <ul>
 * <li>schema1:schema2:schema3://hello:world@host1:80,host2:81,host3:82/p1/p2/p3/file.ext?q1=v1&q2=v2#ref</li>
 * <li>schema1:schema2:schema3:host1,host2,host3/p1/p2/p3/file.ext?q1=v1&q2=v2#ref</li>
 * </ul>
 * 
 * @author zx
 */
public final class URISpec implements Serializable {
	private static final long serialVersionUID = -2912181622902556535L;
	private final List<String> schemes;
	private final boolean opaque;
	private final String username;
	private final String password;
	private final List<Pair<String, Integer>> hosts;
	private final List<String> paths;
	private final String file;

	private final Map<String, String> query;
	private final String frag;
	private final int defPort;

	public URISpec(String str) {
		this(str, -1);
	}

	private URISpec(String schemes, boolean opaque, String username, String password, String host, int defPort, String pathfile,
			String frag, String query) {
		super();
		this.opaque = opaque;
		this.schemes = Arrays.stream(schemes.split(":")).collect(Collectors.toList());
		this.username = username;
		this.password = password;
		this.hosts = parseHostPort(host, defPort);
		Pair<String, String> divs = split2last(pathfile, '/');
		this.paths = Arrays.asList(divs.v1().split("/"));
		this.file = divs.v2();
		this.query = parseQueryMap(query);
		this.frag = frag;
		this.defPort = defPort;
	}

	public URISpec(String spec, int defaultPort) {
		super();

		String remain = spec;
		String[] segs;
		Pair<String, String> divs;

		frag = (divs = split2last(remain, '#')).v2();
		remain = divs.v1();

		query = parseQueryMap((divs = split2last(remain, '?')).v2());
		remain = divs.v1();

		if ((segs = remain.split("://", 2)).length == 2) {
			opaque = false;
			schemes = Arrays.asList(segs[0].split(":"));
			remain = segs[1];
		} else {
			opaque = true;
			remain = (divs = split2last(remain, ':')).v1();
			if (divs.v2() != null) {
				schemes = Arrays.asList(remain.split(":"));
				remain = divs.v2();
			} else schemes = new ArrayList<>();
		}

		if ((segs = remain.split("/", 2)).length == 2) {
			file = ornull((divs = split2last(segs[1], '/')).v2());
			paths = Arrays.asList(divs.v1().split("/"));
		} else {
			file = null;
			paths = new ArrayList<>();
		}

		if ((segs = segs[0].split("@", 2)).length == 2) {
			remain = segs[1];
			password = (segs = segs[0].split(":", 2)).length == 2 ? segs[1] : null;
			username = ornull(segs[0]);
		} else {
			remain = segs[0];
			password = null;
			username = null;
		}
		hosts = parseHostPort(remain, defaultPort);
		defPort = defaultPort;
	}

	private Map<String, String> parseQueryMap(String query) {
		if (query == null) return new ConcurrentHashMap<>();
		return Arrays.stream(query.split("&")).parallel().map(q -> q.split("=", 2)).collect(Collectors.toConcurrentMap(kv -> kv[0],
				kv -> kv[1]));
	}

	private List<Pair<String, Integer>> parseHostPort(String remain, int defaultPort) {
		return Arrays.stream(remain.split(",")).map(s -> {
			String[] hp = s.split(":", 2);
			String h;
			int p;
			try {
				p = hp.length == 2 ? Integer.parseInt(hp[1]) : defaultPort;
				h = hp[0];
			} catch (NumberFormatException e) {
				p = defaultPort;
				h = s;
			}
			return new Pair<>(h, p < 0 ? null : p);
		}).collect(Collectors.toList());
	}

	public String getScheme() {
		return schemes.isEmpty() ? null : schemes.stream().collect(Collectors.joining(":"));
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
		return hosts.stream().map(p -> p.v2() == null || p.v2() < 0 ? p.v1()
				: new StringBuilder(p.v1()).append(":").append(p.v2()).toString()).collect(Collectors.joining(","));
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

	public String getPathFile() {
		String p = getPath();
		if (p == null) return null;
		return null == file ? p : p + "/" + file;
	}

	public String getQuery() {
		return query.isEmpty() ? null
				: query.entrySet().parallelStream().map(e -> e.getKey().toString() + "=" + e.getValue().toString()).collect(Collectors
						.joining("&"));
	}

	public Map<String, String> getParameters() {
		return ImmutableMap.copyOf(query);
	}

	public String getParameter(String name) {
		return query.get(name);
	}

	public String getParameter(String name, String defaultValue) {
		return query.getOrDefault(name, defaultValue);
	}

	public String getFragment() {
		return frag;
	}

	public boolean isOpaque() {
		return opaque;
	}

	public String getFile() {
		return file;
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

	public int getDefaultPort() {
		return defPort;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (!schemes.isEmpty()) sb.append(getScheme()).append(':');
		sb.append("//");
		sb.append(getAuthority());
		if (!paths.isEmpty()) sb.append(getPath());
		if (!query.isEmpty()) sb.append('?').append(getQuery());
		if (frag != null) sb.append('#').append(frag);
		return sb.toString();
	}

	public URI toURI() {
		try {
			return new URI(toString());
		} catch (URISyntaxException e) {
			return null;
		}
	}

	@Override
	public URISpec clone() {
		return new URISpec(getScheme(), opaque, username, password, getHost(), defPort, getPathFile(), frag, getQuery());
	}

	public URISpec redirect(String host, int port) {
		String h = host;
		if (port >= 0) h += ":" + port;
		return new URISpec(getScheme(), opaque, username, password, h, defPort, getPathFile(), frag, getQuery());
	}

	public URISpec redirect(String host) {
		return new URISpec(getScheme(), opaque, username, password, host, defPort, getPathFile(), frag, getQuery());
	}

	public URISpec reauth(String username, String password) {
		return new URISpec(getScheme(), opaque, username, password, getHost(), defPort, getPathFile(), frag, getQuery());
	}

	private Pair<String, String> split2last(String spec, char split) {
		int pos = spec.lastIndexOf(split);
		return pos > 0 ? new Pair<>(spec.substring(0, pos), spec.substring(pos + 1)) : new Pair<>(spec, null);
	}

	private String ornull(String spec) {
		return "".equals(spec) ? null : spec;
	}

	public static void main(String... args) throws URISyntaxException {
		URISpec u = new URISpec("s1:s2:s3://hello:world@host1:80,host2:81,host3:82/p1/p2/p3/file.ext?q=v#ref");
		u.getAuthority();
		URISpec u1 = u.redirect("localhost");
		System.out.println(u + "\n" + u1);
		u = new URISpec("file:///./hello.txt");
		u1 = new URISpec("file://C:/hello.txt");
		System.out.println(u + "\n" + u1);
	}
}