package net.butfly.albacore.io;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

import net.butfly.albacore.io.utils.URISpec;

public class URITest {
	@Test
	public void URITestDB() throws MalformedURLException, URISyntaxException {
		URI uri;
		// uri = new URI("mongodb://yhsb:yhsb1234@10.81.193.181:22/yhsb");
		uri = new URI("elasticsearch://clustername@10.81.193.181:22,10.81.193.181:22/index/type?a=b");
		// uri = new URI("jdbc:mysql://127.0.0.1:3306/world");
		String a = uri.getScheme();

		System.out.println(a);
		String b = uri.getHost();
		System.out.println(b);
		String c = uri.getPath();
		System.out.println(c);
		String e = uri.getUserInfo();
		System.out.println(e);
		int f = uri.getPort();
		System.out.println(f);
	}

	@Test
	public void URISpecTest() {
		String uri = "mongodb://people:people5678@10.118.159.106:30012,10.118.159.107:30012/people/abc";
		URISpec uriSpec = new URISpec(uri);
		System.out.print(uriSpec);
	}
}
