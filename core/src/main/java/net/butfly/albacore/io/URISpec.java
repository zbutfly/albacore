package net.butfly.albacore.io;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.Texts;

/**
 * Parse URI string like:
 * <ul>
 * <li>schema1:schema2:schema3://hello:world@host1:80,host2:81,host3:82/p1/p2/p3/file.ext?q1=v1&q2=v2#ref</li>
 * <li>schema1:schema2:schema3:username@host1,host2,host3/p1/p2/p3/file.ext?q1=v1&q2=v2#ref</li>
 * </ul>
 * 
 * @author zx
 */
public final class URISpec implements Serializable {
	private static final long serialVersionUID = -2912181622902556535L;
	private static final char SLASH = '/';
	private static final String SLASHS = "/";
	private final String[] schemes;
	private final boolean opaque;
	private final String username;
	private final String password;
	private final List<Pair<String, Integer>> hosts;
	private final String[] paths;
	private final String file;

	private final Map<String, String> query;
	private final String frag;
	private final int defPort;

	public URISpec(String str) {
		this(str, -1);
	}

	private URISpec(String scheme, boolean opaque, String username, String password, String host, int defPort, String pathfile, String frag,
			String queryString) {
		super();
		this.opaque = opaque;
		schemes = parseScheme(scheme);
		this.username = username;
		this.password = password;
		hosts = parseHostPort(host, defPort);
		if (pathfile == null) {
			paths = new String[0];
			file = null;
		} else {
			Pair<String[], String> pf = parsePathFile(pathfile);
			paths = pf.v1();
			file = pf.v2();
		}
		query = parseQueryMap(queryString);
		this.frag = frag;
		this.defPort = defPort;
	}

	private String[] parseScheme(String scheme) {
		return Arrays.stream(scheme.split(":")).filter(Texts::notEmpty).toArray(i -> new String[i]);
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
			schemes = parseScheme(segs[0]);
			remain = segs[1];
		} else {
			opaque = true;
			remain = (divs = split2last(remain, ':')).v1();
			if (divs.v2() != null) {
				schemes = parseScheme(remain);
				remain = divs.v2();
			} else schemes = new String[0];
		}

		if ((segs = remain.split(SLASHS, 2)).length == 2) {
			Pair<String[], String> pf = parsePathFile(segs[1]);
			paths = pf.v1();
			file = pf.v2();
		} else {
			file = null;
			paths = new String[0];
		}

		if ((segs = segs[0].split("@", 2)).length == 2) {
			remain = segs[1];
			password = (segs = segs[0].split(":", 2)).length == 2 ? segs[1] : null;
			username = Texts.orNull(segs[0]);
		} else {
			remain = segs[0];
			password = null;
			username = null;
		}
		hosts = parseHostPort(remain, defaultPort);
		defPort = defaultPort;
	}

	private Pair<String[], String> parsePathFile(String pathfile) {
		Pair<String, String> divs = split2last(pathfile, SLASH);
		String f = Texts.orNull(divs.v2() == null ? divs.v1() : divs.v2());
		String[] ps = divs.v2() == null ? new String[0] : divs.v1().split(SLASHS);
		return new Pair<>(ps, f);
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
				h = Texts.orNull(hp[0]);
			} catch (NumberFormatException e) {
				p = defaultPort;
				h = Texts.orNull(s);
			}
			return new Pair<>(h, p < 0 ? null : p);
		}).collect(Collectors.toList());
	}

	public String getScheme() {
		return schemes.length == 0 ? null : Arrays.stream(schemes).collect(Collectors.joining(":"));
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
		return hosts.stream().map(p -> {
			boolean nop = p.v2() == null || p.v2() < 0;
			if (p.v1() == null && nop) return null;
			if (p.v1() == null) return ":" + p.v2();
			if (nop) return p.v1();
			return p.v1() + ":" + p.v2();
		}).filter(Streams.NOT_NULL).collect(Collectors.joining(","));
	}

	public String[] getPaths() {
		return Arrays.copyOf(paths, paths.length);
	}

	private String join(String[] segs) {
		return segs == null || segs.length == 0 ? null : Joiner.on('/').join(segs);
	}

	public String getPathAt(int index, String... defaults) {
		return index >= 0 && index < paths.length ? paths[index] : defaults == null || defaults.length <= 0 ? null : join(defaults);
	}

	public String getPathAtLast(int index, String... defaults) {
		return getPathAt(paths.length - index - 1, defaults);
	}

	public String getPath(int segs) {
		return join(segs > paths.length ? paths : Arrays.copyOf(paths, segs));
	}

	public String getPathSkip(int segs) {
		return join(segs >= paths.length ? null : Arrays.copyOfRange(paths, segs, paths.length));
	}

	private String paths() {
		return join(paths) + SLASHS;
	}

	private String pathfile() {
		if (paths.length == 0 && file == null) return null;
		if (paths.length == 0) return file;
		if (file == null) return paths();
		return paths() + file;
	}

	public String getPath() {
		String p = pathfile();
		return null == p ? p : SLASHS + p;
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
		String s = getScheme();
		if (s != null) sb.append(s).append(':');
		if (!opaque) sb.append("//");
		sb.append(getAuthority());
		String pf = getPath();
		sb.append(null == pf ? SLASHS : pf);
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
		return new URISpec(getScheme(), opaque, username, password, getHost(), defPort, pathfile(), frag, getQuery());
	}

	public URISpec redirect(String host, int port) {
		String h = host;
		if (port >= 0) h += ":" + port;
		return new URISpec(getScheme(), opaque, username, password, h, defPort, pathfile(), frag, getQuery());
	}

	public URISpec redirect(String host) {
		return new URISpec(getScheme(), opaque, username, password, host, defPort, pathfile(), frag, getQuery());
	}

	public URISpec reauth(String username, String password) {
		if (opaque) throw new IllegalArgumentException("opaque uri could not be reauth since no recoganizable password segment.");
		return new URISpec(getScheme(), opaque, username, password, getHost(), defPort, pathfile(), frag, getQuery());
	}

	private Pair<String, String> split2last(String spec, char split) {
		int pos = spec.lastIndexOf(split);
		return pos > 0 ? new Pair<>(spec.substring(0, pos), spec.substring(pos + 1)) : new Pair<>(spec, null);
	}

	public static void main(String... args) throws URISyntaxException {
		URISpec u;
		// u = new URISpec("mailto:zbutfly@gmail.com");
		// System.out.println(u);
		// u = new
		// URISpec("s1:s2:s3://hello:world@host1:80,host2:81,host3:82/p1/p2/p3/file.ext?q=v#ref");
		// System.out.println(u);
		u = new URISpec("s1:s2:s3://hello:world@host1:80,host2:81,host3:82");
		System.out.println(u + "\n\tAuthority: " + u.getAuthority() + "\n\tPath: " + u.getPath());
		u = new URISpec("s1:s2:s3://hello:world@host1:80,host2:81,host3:82/");
		System.out.println(u + "\n\tAuthority: " + u.getAuthority() + "\n\tPath: " + u.getPath());
		u = new URISpec("s1:s2:s3://hello:world@host1:80,host2:81,host3:82/p1/");
		System.out.println(u + "\n\tAuthority: " + u.getAuthority() + "\n\tPath: " + u.getPath());
		u = new URISpec("s1:s2:s3://hello:world@host1:80,host2:81,host3:82/file.ext?q=v#ref");
		System.out.println(u + "\n\tAuthority: " + u.getAuthority() + "\n\tPath: " + u.getPath());
		u = u.redirect("redirected");
		System.out.println(u + "\n\tAuthority: " + u.getAuthority() + "\n\tPath: " + u.getPath());
		u = new URISpec("file://./hello.txt");
		System.out.println(u + "\n\tAuthority: " + u.getAuthority() + "\n\tPath: " + u.getPath());
		u = new URISpec("file:///C:/hello.txt");
		System.out.println(u + "\n\tAuthority: " + u.getAuthority() + "\n\tPath: " + u.getPath());
		u = u.redirect("redirected");
		System.out.println(u + "\n\tAuthority: " + u.getAuthority() + "\n\tPath: " + u.getPath());
	}
}