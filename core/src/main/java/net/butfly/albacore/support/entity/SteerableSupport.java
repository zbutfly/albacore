package net.butfly.albacore.support.entity;

import java.io.Serializable;

public interface SteerableSupport<K extends Serializable> extends CreateSupport<K>, UpdateSupport<K>, DeleteSupport<K> {}
