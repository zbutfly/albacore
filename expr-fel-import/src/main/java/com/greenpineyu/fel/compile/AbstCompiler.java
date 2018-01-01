package com.greenpineyu.fel.compile;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.greenpineyu.fel.Expression;
import com.greenpineyu.fel.common.StringUtils;

public abstract class AbstCompiler implements FelCompiler {
	/**
	 * class文件夹
	 */
	static final String CLASS_DIR;

	private String classpath4compile;

	private static final String BASE_DIR;
	static ClassLoader loader;
	static {
		String userDir = System.getProperty("user.dir");
		BASE_DIR = userDir + File.separator + "fel" + File.separator;
		CLASS_DIR = BASE_DIR + "classes" + File.separator;
		loader = new FileClassLoader(AbstCompiler.class.getClassLoader(), CLASS_DIR);
		createClassDir();
	}

	{
		classpath4compile = classPathToString();
	}

	/**
	 * Class文件所在文件夹，包含包名
	 */
	static String getClassPackageDir(String pack) {
		return CLASS_DIR + packageToPath(pack) + File.separator;
	}

	// protected abstract List<String> getClassPath(ClassLoader cl);

	protected String classPathToString() {
		List<String> paths = CompileService.getClassPath(this.getClass().getClassLoader());
		StringBuilder cpStr = new StringBuilder();
		for (String c : paths) {
			cpStr.append(c + File.pathSeparator);
		}
		return cpStr.toString();
	}

	static String getSrcPackageDir(String pack) {
		return BASE_DIR + "src" + File.separator + packageToPath(pack) + File.separator;
	}

	@Override
	public Expression compile(JavaSource src) {
		Class<Expression> compile;
		try {
			compile = this.compileToClass(src);
			return compile.getConstructor().newInstance();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException | NoSuchMethodException | SecurityException e) {
			return null;
		} finally {
			String className = src.getName();
			String pack = src.getPackageName();
			String srcPackageDir = getSrcPackageDir(pack);
			clean(srcPackageDir, getClassPackageDir(pack), className);
		}
	}

	abstract Class<Expression> compileToClass(JavaSource expr) throws ClassNotFoundException;

	static void createClassDir() {
		new File(CLASS_DIR).mkdirs();
	}

	private static ExecutorService exeService = initThreadPool();

	private static ExecutorService initThreadPool() {
		return new ThreadPoolExecutor(0, 10, 5L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
	}

	void clean(final String srcPackageDir, final String classPackageDir, final String fileName) {
		if (exeService.isShutdown()) {
			exeService = initThreadPool();
		}
		exeService.execute(new Runnable() {
			@Override
			public void run() {
				// 优先级设置成最低
				Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
				delFile(srcPackageDir, classPackageDir, fileName);
			}
		});
		// exeService.shutdown();
	}

	void delFile(final String srcPackageDir, final String classPackageDir, final String fileName) {
		String src = srcPackageDir + fileName + ".java";
		deleteFile(src);
		String cls = classPackageDir + fileName + ".class";
		deleteFile(cls);
	}

	void deleteFile(String src) {
		File file = new File(src);
		if (file.exists()) {
			file.delete();
		}
	}

	List<String> getCompileOption() {
		List<String> options = new ArrayList<String>();
		options.add("-encoding");
		options.add("UTF-8");
		options.add("-d");
		options.add(CLASS_DIR);

		if (StringUtils.isNotEmpty(classpath4compile)) {
			options.add("-classpath");
			options.add(classpath4compile);
		}
		return options;
	}

	/**
	 * 将包名转换成包路径
	 * 
	 * @param packageName
	 * @return
	 */
	private static String packageToPath(String packageName) {
		String sep = File.separator;
		// if (sep.equals("\\")) {
		// sep = "\\\\";
		// }
		return StringUtils.replace(packageName, ".", sep);
		// return packageName.replaceAll("\\.", sep);
	}
}
