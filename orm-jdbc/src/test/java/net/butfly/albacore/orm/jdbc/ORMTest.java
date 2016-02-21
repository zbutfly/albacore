package net.butfly.albacore.orm.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.orm.jdbc.dao.ContactDAO;
import net.butfly.albacore.orm.jdbc.dao.UserDAO;
import net.butfly.albacore.orm.jdbc.mapper.Contact;
import net.butfly.albacore.orm.jdbc.mapper.ContactKey.Type;
import net.butfly.albacore.orm.jdbc.mapper.User;
import net.butfly.albacore.test.SpringCase;

public class ORMTest extends SpringCase {
	private UserDAO userDAO;
	private ContactDAO contactDAO;

	@Override
	protected String[] getConfiguration() {
		return new String[] { "net/butfly/albacore/orm/jdbc/spring/beans.xml" };
	}

	@Override
	protected void initialize() {
		userDAO = getBean(UserDAO.class);
		contactDAO = getBean(ContactDAO.class);
		try {
			this.createTables();
		} catch (SQLException e) {
			logger.error("H2 Database table create failure", e);
		}
	}

	@Test
	public void orm() throws SQLException {
		for (Entry<User, Contact[]> e : INIT_DATA.entrySet()) {
			String id = userDAO.insert(e.getKey());
			for (Contact c : e.getValue())
				c.setUserId(id);
			contactDAO.insert(e.getValue());
		}

		User[] all = userDAO.select(User.class, new Criteria(), Page.ALL_RECORD());
		logger.info("All user records count: " + all.length);
		Contact[] allc = contactDAO.select(Contact.class, new Criteria(), Page.ALL_RECORD());
		logger.info("All contact records count: " + allc.length);
		allc = contactDAO.delete(Contact.class, new Criteria());
		logger.info("Removed contact records count: " + allc.length);
		allc = contactDAO.select(Contact.class, new Criteria(), Page.ALL_RECORD());
		logger.info("Remained contact records count: " + allc.length);
	}

	@Test
	public void color() {
		logger.trace("Colour testing for.");
		logger.debug("Colour testing for.");
		logger.info("Colour testing for.");
		logger.warn("Colour testing for.");
		logger.error("Colour testing for.", new RuntimeException("Test error throwing and logging."));
	}

	private final static Map<User, Contact[]> INIT_DATA = new HashMap<>();

	static {
		INIT_DATA.put(new User("张欣", false, "123456"), new Contact[] { new Contact(null, Type.EMAIL, "zhangx@hzcominfo.com"),
				new Contact(null, Type.MOBILE, "+8613958194045"), new Contact(null, Type.QQ_NUM, "14278797") });
	}

	private void createTables() throws SQLException {
		Connection conn = dataSources[0].getConnection();
		try {
			conn.setAutoCommit(false);
			Statement stmt = conn.createStatement();
			try {
				stmt.execute(
						"CREATE TABLE user (id VARCHAR(255) PRIMARY KEY NOT NULL, name VARCHAR(255), gender BOOLEAN, password VARCHAR(255))");
				stmt.execute(
						"CREATE TABLE contact (user_id VARCHAR(255) NOT NULL, type varchar(255) NOT NULL, content VARCHAR(255), FOREIGN KEY (user_id) REFERENCES user (id), PRIMARY KEY(user_id, type))");
			} finally {
				stmt.close();
			}
		} finally {
			conn.commit();
			conn.close();
		}
	}
}
