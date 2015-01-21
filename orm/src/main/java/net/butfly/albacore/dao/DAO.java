package net.butfly.albacore.dao;

import net.butfly.albacore.base.BizUnit;

public interface DAO extends BizUnit {
	enum Verb {
		insert, delete, update, select, count
	}
}
