import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.utils.Systems;

public final class alpp {
	public static void main(String... args) throws Throwable {
		Systems.forkVM(Boolean.parseBoolean(System.getProperty("albacore.app.daemon", "false")));
		if (null == args || args.length == 0) throw new RuntimeException(
				"Wrapper main method need at list 1 argument: actually main class being wrapped.");
		List<String> argl = new ArrayList<>(Arrays.asList(args));
		Systems.wrapMain(Class.forName(argl.remove(0)), argl.toArray(new String[0]));
	}
}
