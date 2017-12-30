package com.hzcominfo.fel;

import com.greenpineyu.fel.FelEngine;
import com.greenpineyu.fel.FelEngineImpl;
import com.greenpineyu.fel.function.Function;

@Deprecated
public class FelBuilderOld {
	public FelEngine engine;

	public FelBuilderOld() {
		engine = new FelEngineImpl();
	}

	public void addFuncToFel(Function fun) {
		engine.addFun(fun);
	}
}
