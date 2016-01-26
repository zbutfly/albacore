package net.butfly.albacore.helper.encrypt;

import net.butfly.albacore.helper.EncryptHelper;
import net.butfly.albacore.helper.HelperBase;
import net.butfly.albacore.utils.encrypt.Algorithm;

public class EncryptHelperImpl extends HelperBase implements EncryptHelper {
	private static final long serialVersionUID = 6716198640704602577L;
	protected String key;
	protected Algorithm algorithm;

	public void setKey(String key) {
		this.key = key;
	}

	public String getKey() {
		return this.key;
	}

	public Algorithm getAlgorithm() {
		return algorithm;
	}

	public void setAlgorithm(Algorithm algorithm) {
		this.algorithm = algorithm;
	}

	@Override
	public String encrypt(String src) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String decrypt(String src) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] encrypt(byte[] src) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] decrypt(byte[] src) {
		// TODO Auto-generated method stub
		return null;
	}
}
