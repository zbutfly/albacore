package net.butfly.albacore.utils;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public final class JNDIUtils {
	private JNDIUtils() {};

	public final static void putDataSourceIntoJNDI(String jndiName, Object ds) {
		System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
		System.setProperty(Context.URL_PKG_PREFIXES, "org.apache.naming");
		InitialContext ic;
		try {
			ic = new InitialContext();
			ic.createSubcontext("java:");
			ic.createSubcontext("java:/comp");
			ic.createSubcontext("java:/comp/env");
			ic.createSubcontext("java:/comp/env/jdbc");
			ic.bind("java:/comp/env/" + jndiName, ds);
		} catch (NamingException e) {
			throw new RuntimeException(e);
		}
	}
}
