package com.greenpineyu.fel.context;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ArrayCtxImpl implements ArrayCtx {

	/**
	 * 保存对象类型的变量,包含数值类型的变量
	 */
	private Var[] vars = new Var[10];

	private Map<String, Integer> indexMap = new HashMap<String, Integer>();

	public ArrayCtxImpl() {}

	public ArrayCtxImpl(Map<String, Object> vars) {
		if (vars != null) {
			for (Map.Entry<String, Object> e : vars.entrySet()) {
				this.set(e.getKey(), e.getValue());
			}
		}
	}

	@Override
	public Object get(int i) {
		return vars[i].getValue();
	}

	@Override
	public int getIndex(String name) {
		Integer i = indexMap.get(name);
		return i != null ? i : -1;
	}

	@Override
	public Object get(String name) {
		Var var = getVar(name);
		return var != null ? var.getValue() : null;
	}

	@Override
	public Var getVar(String name) {
		int index = getIndex(name);
		if (index == -1) return null;
		return vars[index];
	}

	/**
	 * 获取变量，如果变量不存在，就添加一个新的变量。
	 * 
	 * @param name
	 * @return
	 */
	private Var getVarWithoutNull(String name) {
		Var var = getVar(name);
		if (var == null) {
			var = new Var(name, null);
			setVar(var);
		}
		return var;
	}

	/**
	 * 确保vars.length>minCapacity
	 * 
	 * @param minCapacity
	 */
	public void ensureCapacity(int minCapacity) {
		int oldCapacity = vars.length;
		if (minCapacity > oldCapacity) {
			int newCapacity = (oldCapacity * 3) / 2 + 1;
			if (newCapacity < minCapacity) newCapacity = minCapacity;
			// minCapacity is usually close to size, so this is a win:
			vars = Arrays.copyOf(vars, newCapacity);
		}
	}

	@Override
	public void set(String name, Object value) {
		getVarWithoutNull(name).setValue(value);
	}

	@Override
	public void setVar(Var var) {
		int i = addToIndexMap(var.getName());
		if (i < vars.length) {} else {
			ensureCapacity(i + 1);
		}
		vars[i] = var;
	}

	private int addToIndexMap(String name) {
		synchronized (indexMap) {
			Integer i = indexMap.get(name);
			if (i != null) return i;
			int newIndex = indexMap.size();
			indexMap.put(name, newIndex);
			return newIndex;
		}
	}
}
