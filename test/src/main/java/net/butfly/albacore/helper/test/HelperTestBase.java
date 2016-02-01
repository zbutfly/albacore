package net.butfly.albacore.helper.test;

import net.butfly.albacore.test.SpringTestCaseBase;

public abstract class HelperTestBase extends SpringTestCaseBase {
	@Override
	protected String[] getConfiguration() {
		return new String[] { "classpath:/net/butfly/albacore/spring/beans.xml",
				"classpath:/net/butfly/albacore/spring/ds/basic/beans-ds-db2.xml",
				"classpath:/net/butfly/albacore/helper/beans-helper-sqlmap.xml",
				"classpath:/net/butfly/albacore/helper/beans-context.xml" };
	}
}
