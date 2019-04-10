import org.slf4j.event.Level;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.logger.Loggers.Logging;

@Logging(Level.TRACE)
public class LoggerTest {
	// private static Logger logger = Logger.getLogger(LoggerTest.class);

	public static void main(String[] args) {
		Logger.logs(Level.ERROR, "info");
		Logger.logs(Level.TRACE, "info");
	}
}
