package net.butfly.albacore.serder;

import net.butfly.albacore.serder.support.ByteArray;

public interface ArrableBinarySerder<PRESENT> extends ArrableSerder<PRESENT, ByteArray>, BinarySerder<PRESENT> {}
