import com.google.common.base.Joiner;

import net.butfly.albacore.Albacore;
import net.butfly.albacore.utils.JVM;
import net.butfly.albacore.utils.Systems;

public final class alpp {
	public static void main(String... args) throws Throwable {
		if (null == System.getProperty("exec.mainClass")) System.setProperty("exec.mainClass", alpp.class.getName());
		if (null == System.getProperty("exec.mainArgs")) System.setProperty("exec.mainArgs", Joiner.on(' ').join(args));
		Systems.fork(JVM.customize(alpp.class, args), Boolean.parseBoolean(System.getProperty(Albacore.Props.PROP_APP_DAEMON, "false")));
		JVM.current().unwrap();
	}
}
