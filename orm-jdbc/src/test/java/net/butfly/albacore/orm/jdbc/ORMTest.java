package net.butfly.albacore.orm.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;

import com.jcabi.log.Logger;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.orm.jdbc.dao.ContactDAO;
import net.butfly.albacore.orm.jdbc.dao.UserDAO;
import net.butfly.albacore.orm.jdbc.mapper.Contact;
import net.butfly.albacore.orm.jdbc.mapper.ContactKey.Type;
import net.butfly.albacore.orm.jdbc.mapper.User;

public class ORMTest {
	ApplicationContext spring;
	UserDAO userDAO;
	ContactDAO contactDAO;

	public ORMTest(DataSource datasource) throws SQLException {
		super();
		spring = new GenericXmlApplicationContext("net/butfly/albacore/orm/jdbc/spring/beans.xml");
		userDAO = spring.getBean(UserDAO.class);
		contactDAO = spring.getBean(ContactDAO.class);
		Connection conn = datasource.getConnection();
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

	private final static Map<User, Contact[]> INIT_DATA = new HashMap<>();

	static {
		INIT_DATA.put(new User("张欣", false, "123456"), new Contact[] { new Contact(null, Type.EMAIL, "zhangx@hzcominfo.com"),
				new Contact(null, Type.MOBILE, "+8613958194045"), new Contact(null, Type.QQ_NUM, "14278797") });
	}

	public static void main(String[] args) throws SQLException, NamingException {
		ORMTest test = new ORMTest(jndi());

		for (Entry<User, Contact[]> e : INIT_DATA.entrySet()) {
			String id = test.userDAO.insert(e.getKey());
			for (Contact c : e.getValue())
				c.setUserId(id);
			test.contactDAO.insert(e.getValue());
		}

		User[] all = test.userDAO.select(User.class, new Criteria(), Page.ALL_RECORD());
		Logger.info(test, "All user records count: " + all.length);
		Contact[] allc = test.contactDAO.select(Contact.class, new Criteria(), Page.ALL_RECORD());
		Logger.info(test, "All contact records count: " + allc.length);
		allc = test.contactDAO.delete(Contact.class, new Criteria());
		Logger.info(test, "Removed contact records count: " + allc.length);
		allc = test.contactDAO.select(Contact.class, new Criteria(), Page.ALL_RECORD());
		Logger.info(test, "Remained contact records count: " + allc.length);
	}

	private static DataSource jndi() throws NamingException {
		InitialContext ctxt = new InitialContext();
		DataSource ds = (DataSource) ctxt.lookup("jdbc.orm-jdbc-test");
		// rebind for alias if needed
		ctxt.rebind("java:comp/env/jdbc/orm-jdbc-test", ds);
		ctxt.lookup("java:comp/env/jdbc/orm-jdbc-test");
		return ds;
	}
}
