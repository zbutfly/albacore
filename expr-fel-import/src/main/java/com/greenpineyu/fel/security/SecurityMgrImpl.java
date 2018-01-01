package com.greenpineyu.fel.security;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

public class SecurityMgrImpl implements SecurityMgr {
	private Set<Method> callableSet;
	private Set<Method> uncallableSet;

	{
		callableSet = new HashSet<Method>();
		uncallableSet = new HashSet<Method>();
	}

	/**
	 * 判断原则，以可访问方法列表为首要依据（包含目标方法表示允许访问，否则不允许），不可访问方法列表为次要依据 当允许访问方法列表为空时，以不可访问方法列表为依据。
	 * 
	 * @see com.greenpineyu.fel.security.ReflectMgr#isCallable(java.lang.reflect.Method)
	 */
	@Override
	public boolean isCallable(Method m) {
		if (callableSet.isEmpty()) return !uncallableSet.contains(m);
		return callableSet.contains(m);
	}
}
