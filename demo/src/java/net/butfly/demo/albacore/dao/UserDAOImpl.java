package net.butfly.demo.albacore.dao;

import net.butfly.albacore.dao.base.EntityDAOBase;
import net.butfly.albacore.entity.support.EntityOPStub;
import net.butfly.demo.albacore.mapper.User;

public class UserDAOImpl extends EntityDAOBase<User, String> implements UserDAO {
	private static final long serialVersionUID = -3623907116411729163L;
	@Override
	protected EntityOPStub<String> getCurrentOPStub() {
		return null;
	}
}
