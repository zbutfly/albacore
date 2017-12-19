import net.butfly.albacore.utils.Config;
import net.butfly.albacore.utils.ConfigSet;
import net.butfly.albacore.utils.Configs;

@Config(value = "test.properties", prefix = "dataggr.migrate")
public class ConfigTest {
	@Config("item1")
	private String item1;

	public static void main(String... args) {
		ConfigTest test = new ConfigTest();
		ConfigSet conf = Configs.of();
		System.out.println("item1: " + conf.get("configs.test.item1"));
		Configs.of(ConfigTest.class);
		System.out.println("item1: " + test.item1);
	}
}
