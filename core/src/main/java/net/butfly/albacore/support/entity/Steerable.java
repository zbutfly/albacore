package net.butfly.albacore.support.entity;

import java.io.Serializable;

public interface Steerable<K extends Serializable> extends Created<K>, Updatable<K>, Deletable<K> {}
