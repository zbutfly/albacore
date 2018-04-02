package net.butfly.albacore.io;

import static net.butfly.albacore.paral.Sdream.of;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableMap;

import net.butfly.albacore.utils.Objects;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Maps;

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
	public static final int UNDEFINED_DEFAULT_PORT = -1;
	private static final String SLASHS = "/";
	private final String[] schemes;
	private final boolean opaque;
	private final String username;
	private final String password;
	private final InetSocketAddress[] hosts;
	private final String[] paths;
	private final String file;

	private final Map<String, String> query;
	private final String frag;
	private int defPort;

	@Override
	public boolean equals(Object obj) {
		if (null == obj || !URISpec.class.isAssignableFrom(obj.getClass())) return false;
		URISpec uri = (URISpec) obj;
		if (!eq(schemes, uri.schemes)) return false;
		if (opaque != uri.opaque) return false;
		if (!eq(username, uri.username)) return false;
		if (!eq(password, uri.password)) return false;
		if (!eq(hosts, uri.hosts)) return false;
		if (!eq(paths, uri.paths)) return false;
		if (!eq(file, uri.file)) return false;
		if (!eq(query, uri.query)) return false;
		if (!eq(frag, uri.frag)) return false;
		return true;
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	private <T> boolean eq(T v1, T v2) {
		if (null == v1 && null == v2) return true;
		if (null == v1 || null == v2) return false;
		return v1.equals(v2);
	}

	private <T> boolean eq(T[] v1, T[] v2) {
		if (null == v1 && null == v2) return true;
		if (null == v1 || null == v2) return false;
		if (v1.length != v2.length) return false;
		for (int i = 0; i < v1.length; i++)
			if (!eq(v1[i], v2[i])) return false;
		return true;
	}

	private boolean eq(Map<String, String> v1, Map<String, String> v2) {
		if (null == v1 && null == v2) return true;
		if (null == v1 || null == v2) return false;
		if (v1.size() != v2.size()) return false;
		for (String k : v1.keySet())
			if (!eq(v1.get(k), v2.get(k))) return false;
		return true;
	}

	public URISpec(String str) {
		this(str, UNDEFINED_DEFAULT_PORT);
	}

	public URISpec(String spec, int defaultPort) {
		super();
		String remain = java.util.Objects.requireNonNull(spec, "URISpec should not be constructed by null spec");
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
			Pair<List<String>, String> pf = parsePathFile(segs[1]);
			paths = pf.v1().toArray(new String[pf.v1().size()]);
			file = pf.v2();
		} else {
			file = null;
			paths = new String[0];
		}

		String temp = segs[0];
		int position = temp.lastIndexOf("@");
		if (position == -1) {
			remain = temp;
			password = null;
			username = null;
		} else {
			segs = new String[2];
			segs[1] = temp.substring(position + 1, temp.length());
			segs[0] = temp.substring(0, position);
			remain = segs[1];
			password = (segs = segs[0].split(":", 2)).length == 2 ? segs[1] : null;
			username = Texts.orNull(segs[0]);
		}

		hosts = parseHostPort(remain, defaultPort);
		defPort = defaultPort;
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
			Pair<List<String>, String> pf = parsePathFile(pathfile);
			paths = pf.v1().toArray(new String[pf.v1().size()]);
			file = pf.v2();
		}
		query = parseQueryMap(queryString);
		this.frag = frag;
		this.defPort = defPort;
	}

	private String[] parseScheme(String scheme) {
		return of(scheme.split(":")).filter(Texts::notEmpty).array(i -> new String[i]);
	}

	private Pair<List<String>, String> parsePathFile(String pathfile) {
		String[] segs = pathfile.split(SLASHS + "+");
		if (pathfile.endsWith("/")) {
			segs = Arrays.copyOf(segs, segs.length + 1);
			segs[segs.length - 1] = "";
		}
		List<String> paths = new ArrayList<>();
		if (segs.length == 0) return new Pair<>(paths, null);
		String file = segs[segs.length - 1];
		if (file.isEmpty()) file = null;
		for (int i = 0; i < segs.length - 1; i++)
			if (!segs[i].isEmpty()) paths.add(segs[i]);
		return new Pair<>(paths, file);
	}

	private Map<String, String> parseQueryMap(String query) {
		if (query == null) return Maps.of();
		Map<String, String> m = Maps.of();
		for (String param : query.split("&")) {
			String[] kv = param.split("=", 2);
			m.put(kv[0], kv.length > 1 ? kv[1] : "");
		}
		return m;
	}

	private InetSocketAddress[] parseHostPort(String remain, int defaultPort) {
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
			return null == h ? null : new InetSocketAddress(h, p < 0 ? 0 : p);
		}).filter(o -> null != o).toArray(i -> new InetSocketAddress[i]);
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

	@Deprecated
	public List<Pair<String, Integer>> getHosts() {
		return Arrays.stream(hosts).map(a -> new Pair<>(a.getHostName(), a.getPort())).collect(Collectors.toList());
	}

	public InetSocketAddress[] getInetAddrs() {
		return Arrays.copyOf(hosts, hosts.length);
	}

	public String getHost() {
		return Arrays.stream(hosts).map(a -> a.getHostName() + (a.getPort() >= 0 ? ":" + a.getPort() : "")).collect(Collectors.joining(
				","));
	}

	public String[] getPaths() {
		return Arrays.copyOf(paths, paths.length);
	}

	private String join(String[] segs) {
		return segs == null || segs.length == 0 ? "" : String.join(SLASHS, segs);
	}

	public String getPathAt(int index, String... defaults) {
		if (Math.abs(index) > paths.length) return null;
		if (index < 0) return getPathAtLast(-index, defaults);
		if (index < paths.length) return paths[index];
		String v = getFile();
		if (null != v) return v;
		else return defaults == null || defaults.length <= 0 ? null : join(defaults);
	}

	public String getPathAtLast(int index, String... defaults) {
		return getPathAt(paths.length - index - 1, defaults);
	}

	public String getPath(int segs) {
		return join(segs > paths.length ? paths : Arrays.copyOf(paths, segs));
	}

	public String getPathSkip(int segs) {
		return segs < 0 ? getPathSkip(-segs) : join(segs >= paths.length ? null : Arrays.copyOfRange(paths, segs, paths.length));
	}

	public String getPathOnly() {
		return paths.length == 0 ? SLASHS : SLASHS + join(paths) + SLASHS;
	}

	/**
	 * @return Full path and file, start with "/", end with "/" if no file.
	 */
	public String getPath() {
		if (paths.length == 0 && file == null) return SLASHS;
		String p = getPathOnly();
		if (null != file) p += file;
		return p;
	}

	public String getQuery() {
		return query.isEmpty() ? null
				: query.entrySet().parallelStream().map(e -> e.getKey().toString() + "=" + e.getValue().toString()).collect(Collectors
						.joining("&"));
	}

	public Map<String, String> getParameters(String... excludeKey) {
		Stream<Entry<String, String>> s = query.entrySet().parallelStream();
		if (null != excludeKey && excludeKey.length > 0) {
			Set<String> ks = new HashSet<>(Arrays.asList(excludeKey));
			s = s.filter(e -> !ks.contains(e.getKey()));
		}
		return ImmutableMap.copyOf(s.collect(Collectors.toList()));
	}

	public String getParameter(String name) {
		return query.get(name);
	}

	public String getParameter(String name, String defaultValue) {
		return query.getOrDefault(name, defaultValue);
	}

	public String getParameter(String name, String defaultValue, String other) {
		return Objects.or(query.getOrDefault(name, defaultValue), other);
	}

	public String fetchParameter(String name, String... defaultValue) {
		return Objects.or(query.remove(name), defaultValue);
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

	public void setDefaultPort(int port) {
		defPort = port;
	}

	public String getRoot() {
		StringBuilder sb = new StringBuilder();
		String s = getScheme();
		if (s != null) sb.append(s).append(':');
		if (!opaque) sb.append("//");
		return sb.append(getAuthority()).append("/").toString();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(getRoot());
		sb.deleteCharAt(sb.length() - 1); // remove last "/" from root;
		String pf = getPath();
		if (null != pf) sb.append(pf);
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
		return new URISpec(getScheme(), opaque, username, password, getHost(), defPort, getPath(), frag, getQuery());
	}

	public URISpec redirect(String host, int port) {
		String h = host;
		if (port >= 0) h += ":" + port;
		return new URISpec(getScheme(), opaque, username, password, h, defPort, getPath(), frag, getQuery());
	}

	public URISpec redirect(String host) {
		return new URISpec(getScheme(), opaque, username, password, host, defPort, getPath(), frag, getQuery());
	}

	public URISpec reauth(String username) {
		if (opaque) throw new IllegalArgumentException("opaque uri [" + toString()
				+ "] could not be reauth since no recoganizable password segment.");
		return new URISpec(getScheme(), opaque, username, null, getHost(), defPort, getPath(), frag, getQuery());
	}

	public URISpec reauth(String username, String password) {
		if (opaque) throw new IllegalArgumentException("opaque uri [" + toString()
				+ "] could not be reauth since no recoganizable password segment.");
		return new URISpec(getScheme(), opaque, username, password, getHost(), defPort, getPath(), frag, getQuery());
	}

	public URISpec resolve(String rel) {
		if (rel == null) return this;
		Path p = Paths.get(rel);
		// Path f = null;
		if (!rel.endsWith(SLASHS) && !rel.endsWith("..") && !rel.endsWith("."))
			// f = p.getFileName();
			p = p.getParent();
		Path np = Paths.get(getPathOnly());
		if (null != p) np = np.resolve(p);
		// if (null != f) np = np.resolve(f);
		// else if (null != file) np = np.resolve(file);
		String pp = join(StreamSupport.stream(np.normalize().spliterator(), false).map(s -> s.toString()).toArray(i -> new String[i]))
				+ SLASHS;
		return new URISpec(getScheme(), opaque, username, password, getHost(), defPort, pp, frag, getQuery());
	}

	public URISpec setFile(String file) {
		return new URISpec(getScheme(), opaque, username, password, getHost(), defPort, (null == file ? getPathOnly()
				: getPathOnly() + file), frag, getQuery());
	}

	private Pair<String, String> split2last(String spec, char split) {
		if (null == spec || spec.isEmpty()) return new Pair<>(spec, null);
		while (!spec.isEmpty() && spec.charAt(0) == split)
			spec = spec.substring(1);
		int pos = spec.lastIndexOf(split);
		return pos > 0 ? new Pair<>(spec.substring(0, pos), spec.substring(pos + 1)) : new Pair<>(spec, null);
	}

	public static void main(String... args) throws URISyntaxException {
		URISpec u;
		u = new URISpec("mongodb://root:r@@t001!@172.30.10.101:22001/admin");
		System.out.println(u + "\n\tAuthority: " + u.getAuthority() + "\n\tPath: " + u.getPath());
		u = new URISpec("s1:s2:s3://hello:world@host1:80,host2:81,host3:82");
		System.out.println(u + "\n\tAuthority: " + u.getAuthority() + "\n\tPath: " + u.getPath());
		u = new URISpec("s1:s2:s3://hello:world@host1:80,host2:81,host3:82/");
		System.out.println(u + "\n\tAuthority: " + u.getAuthority() + "\n\tPath: " + u.getPath());
		u = new URISpec("s1:s2:s3://hello:world@host1:80,host2:81,host3:82/p1/");
		System.out.println(u + "\n\tAuthority: " + u.getAuthority() + "\n\tPath: " + u.getPath());
		System.out.println(u.resolve("/h/a/b/c/d/text.cmd").resolve("../").resolve("../"));
		u = new URISpec("s1:s2:s3://hello:world@host1:80,host2:81,host3:82/file.ext?q=v#ref");
		System.out.println(u + "\n\tAuthority: " + u.getAuthority() + "\n\tPath: " + u.getPath());
		System.out.println(u.resolve("/h/a/b/c/d/text.cmd").resolve("../").resolve("../"));
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