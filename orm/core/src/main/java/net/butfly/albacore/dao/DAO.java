package net.butfly.albacore.dao;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

import net.butfly.albacore.base.BizUnit;
import net.butfly.albacore.entity.AbstractEntity;

public interface DAO extends BizUnit {
	public static final class SQL<E extends AbstractEntity<?>> {
		enum Verb {
			insert, delete, update, select, count
		}

		private static final String BY_CRITERIA = "ByCriteria";
		private Verb verb;
		private String namespace;
		private Class<E> entityClass;
		private List<String> suffix;

		public static final <E extends AbstractEntity<?>> SQL<E> build(Class<? extends EntityDAO> daoClass, Class<E> entityClass) {
			return new SQL<E>(daoClass, entityClass);
		}

		private SQL(Class<? extends EntityDAO> daoClass, Class<E> entityClass) {
			this.namespace = namespace(daoClass);
			this.entityClass = entityClass;
			this.suffix = new ArrayList<String>();
			this.suffix.add(this.entityClass.getSimpleName());
		}

		SQL(String namespace, Class<E> entityClass) {
			this.namespace = namespace;
			this.entityClass = entityClass;
			this.suffix = new ArrayList<String>();
			this.suffix.add(this.entityClass.getSimpleName());
		}

		public SQL<E> verb(Verb verb) {
			this.verb = verb;
			return this;
		}

		public SQL<E> suffix(String... suffix) {
			for (String s : suffix)
				this.suffix.add(s);
			return this;
		}

		public String toString() {
			return namespace + verb + Joiner.on("").join(suffix.toArray(new String[suffix.size()]));
		}

		public String toCriteriaString() {
			return toString() + BY_CRITERIA;
		}

		Class<E> entityClass() {
			return this.entityClass;
		}

		private static String namespace(Class<? extends EntityDAO> daoClass) {
			String namespace = daoClass.getName().replaceAll("(?i)dao", "").replaceAll("(?i)impl", ".") + ".";
			namespace = namespace.replaceAll("\\.+", ".");
			return namespace;
		}
	}
}
