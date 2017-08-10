import com.google.common.base.Joiner;

import net.butfly.albacore.utils.JVMRunning;

public final class alpp {
	public static void main(String... args) throws Throwable {
		if (null == System.getProperty("exec.mainClass")) System.setProperty("exec.mainClass", alpp.class.getName());
		if (null == System.getProperty("exec.mainArgs")) System.setProperty("exec.mainArgs", Joiner.on(' ').join(args));
		JVMRunning.customize(alpp.class, args).fork(Boolean.parseBoolean(System.getProperty("albacore.app.daemon", "false"))).unwrap();
	}
}
