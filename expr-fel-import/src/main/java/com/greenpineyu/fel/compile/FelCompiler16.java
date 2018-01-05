package com.greenpineyu.fel.compile;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.greenpineyu.fel.Expression;
import com.greenpineyu.fel.exception.CompileException;

public class FelCompiler16<T> implements FelCompiler {
	private static final Logger logger = LoggerFactory.getLogger(FelCompiler16.class);
	private final FelCompilerClassloader classLoader;
	private final JavaCompiler compiler;
	private final List<String> options;
	private DiagnosticCollector<JavaFileObject> diagnostics;
	private final JavaFileManager javaFileManager;

	public FelCompiler16() {
		compiler = ToolProvider.getSystemJavaCompiler();

		if (compiler == null) throw new IllegalStateException("Cannot find the system Java compiler. "
				+ "Check that your class path includes tools.jar");

		this.classLoader = new FelCompilerClassloader(this.getClass().getClassLoader());
		diagnostics = new DiagnosticCollector<JavaFileObject>();
		final StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);

		ClassLoader loader = this.classLoader.getParent();
		List<String> paths = CompileService.getClassPath(loader);
		List<File> cpFiles = new ArrayList<File>();
		if (paths != null && (!paths.isEmpty())) for (String file : paths)
			cpFiles.add(new File(file));

		try {
			fileManager.setLocation(StandardLocation.CLASS_PATH, cpFiles);
		} catch (IOException e) {
			logger.error("", e);
		}

		javaFileManager = new ForwardingJavaFileManager<JavaFileManager>(fileManager) {
			@Override
			public JavaFileObject getJavaFileForOutput(Location location, String qualifiedName, Kind kind, FileObject outputFile)
					throws IOException {
				// 由于编译成功后的bytecode需要放到file中，所以需要将file放到classloader中，以便读取bytecode生成Class对象.
				classLoader.add(qualifiedName, outputFile);
				logger.trace("Dynamic code [" + qualifiedName + "] compiled and append to classpath.");
				return (JavaFileObject) outputFile;
			}
		};
		this.options = options();
	}

	private List<String> options() {
		final List<String> options = new ArrayList<String>();
		options.add("-classpath");
		final StringBuilder builder = new StringBuilder();

		String cp;
		cp = System.getProperty("java.class.path");
		if (null != cp) builder.append(cp).append(File.pathSeparator);
		cp = System.getProperty("sun.boot.class.path", "");
		if (null != cp) builder.append(cp).append(File.pathSeparator);
		// final URLClassLoader urlClassLoader = (URLClassLoader) ClassLoaderResolver.getClassLoader();
		// for (final URL url : urlClassLoader.getURLs()) {
		// builder.append(url.getFile()).append(File.pathSeparator);
		// }
		final int lastIndexOfColon = builder.lastIndexOf(File.pathSeparator);
		builder.replace(lastIndexOfColon, lastIndexOfColon + 1, "");
		options.add(builder.toString());
		// this.options.add("-O");
		return options;
	}

	@Override
	public Expression compile(JavaSource src) {
		Class<T> compile = compileToClass(src);
		try {
			return (Expression) compile.getConstructor().newInstance();
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			logger.error("", e);
		}
		return null;
	}

	public synchronized Class<T> compileToClass(final JavaSource src) {
		List<JavaFileObject> compileSrcs = new ArrayList<JavaFileObject>();
		String className = src.getSimpleName();
		final FelJavaFileObject compileSrc = new FelJavaFileObject(className, src.getSource());
		compileSrcs.add(compileSrc);
		final CompilationTask task = compiler.getTask(null, javaFileManager, diagnostics, options, null, compileSrcs);
		final Boolean result = task.call();
		if (result == null || !result.booleanValue()) // 编译失败
			throw new CompileException(src.getSource() + "\n" + diagnostics.getDiagnostics().toString());
		else logger.trace("Dynamic source [" + src.getName() + "] compiled successfully: \n" + src.getSource());
		try {
			return loadClass(src.getName());
		} catch (ClassNotFoundException e) {
			logger.error("", e);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public Class<T> loadClass(final String qualifiedClassName) throws ClassNotFoundException {
		return (Class<T>) classLoader.loadClass(qualifiedClassName);
	}

	static URI toUri(String name) {
		try {
			return new URI(name);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}
}
