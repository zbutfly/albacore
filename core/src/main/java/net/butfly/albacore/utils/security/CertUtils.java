package net.butfly.albacore.utils.security;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Date;

import javax.crypto.Cipher;

import net.butfly.albacore.utils.Utils;

/**
 * 证书组件
 * 
 * @author 梁栋
 * @version 1.0
 * @since 1.0
 */
public class CertUtils extends Utils {
	/**
	 * Java密钥库(Java Key Store，JKS)KEY_STORE
	 */
	public static final String KEY_STORE = "JKS";

	public static final String X509 = "X.509";

	/**
	 * 由 KeyStore获得私钥
	 * 
	 * @param keyStorePath
	 * @param keyStorePassword
	 * @param alias
	 * @param aliasPassword
	 * @return
	 * @throws Exception
	 */
	private static PrivateKey getPrivateKey(String keyStorePath, String keyStorePassword, String alias, String aliasPassword)
			throws Exception {
		KeyStore ks = getKeyStore(keyStorePath, keyStorePassword);
		PrivateKey key = (PrivateKey) ks.getKey(alias, aliasPassword.toCharArray());
		return key;
	}

	/**
	 * 由 Certificate获得公钥
	 * 
	 * @param certificatePath
	 * @return
	 * @throws Exception
	 */
	private static PublicKey getPublicKey(String certificatePath) throws Exception {
		Certificate certificate = getCertificate(certificatePath);
		PublicKey key = certificate.getPublicKey();
		return key;
	}

	/**
	 * 获得Certificate
	 * 
	 * @param certificatePath
	 * @return
	 * @throws Exception
	 */
	private static Certificate getCertificate(String certificatePath) throws Exception {
		CertificateFactory certificateFactory = CertificateFactory.getInstance(X509);
		FileInputStream in = new FileInputStream(certificatePath);

		Certificate certificate = certificateFactory.generateCertificate(in);
		in.close();

		return certificate;
	}

	/**
	 * 获得Certificate
	 * 
	 * @param keyStorePath
	 * @param keyStorePassword
	 * @param alias
	 * @return
	 * @throws Exception
	 */
	private static Certificate getCertificate(String keyStorePath, String keyStorePassword, String alias) throws Exception {
		KeyStore ks = getKeyStore(keyStorePath, keyStorePassword);
		Certificate certificate = ks.getCertificate(alias);

		return certificate;
	}

	/**
	 * 获得KeyStore
	 * 
	 * @param keyStorePath
	 * @param password
	 * @return
	 * @throws Exception
	 */
	private static KeyStore getKeyStore(String keyStorePath, String password) throws Exception {
		FileInputStream is = new FileInputStream(keyStorePath);
		KeyStore ks = KeyStore.getInstance(KEY_STORE);
		ks.load(is, password.toCharArray());
		is.close();
		return ks;
	}

	/**
	 * 私钥加密
	 * 
	 * @param data
	 * @param keyStorePath
	 * @param keyStorePassword
	 * @param alias
	 * @param aliasPassword
	 * @return
	 * @throws Exception
	 */
	public static byte[] encryptByPrivateKey(byte[] data, String keyStorePath, String keyStorePassword, String alias, String aliasPassword)
			throws Exception {
		// 取得私钥
		PrivateKey privateKey = getPrivateKey(keyStorePath, keyStorePassword, alias, aliasPassword);

		// 对数据加密
		Cipher cipher = Cipher.getInstance(privateKey.getAlgorithm());
		cipher.init(Cipher.ENCRYPT_MODE, privateKey);

		return cipher.doFinal(data);

	}

	/**
	 * 私钥解密
	 * 
	 * @param data
	 * @param keyStorePath
	 * @param alias
	 * @param keyStorePassword
	 * @param aliasPassword
	 * @return
	 * @throws Exception
	 */
	public static byte[] decryptByPrivateKey(byte[] data, String keyStorePath, String alias, String keyStorePassword, String aliasPassword)
			throws Exception {
		// 取得私钥
		PrivateKey privateKey = getPrivateKey(keyStorePath, keyStorePassword, alias, aliasPassword);

		// 对数据加密
		Cipher cipher = Cipher.getInstance(privateKey.getAlgorithm());
		cipher.init(Cipher.DECRYPT_MODE, privateKey);

		return cipher.doFinal(data);

	}

	/**
	 * 公钥加密
	 * 
	 * @param data
	 * @param certificatePath
	 * @return
	 * @throws Exception
	 */
	public static byte[] encryptByPublicKey(byte[] data, String certificatePath) throws Exception {

		// 取得公钥
		PublicKey publicKey = getPublicKey(certificatePath);
		// 对数据加密
		Cipher cipher = Cipher.getInstance(publicKey.getAlgorithm());
		cipher.init(Cipher.ENCRYPT_MODE, publicKey);

		return cipher.doFinal(data);

	}

	/**
	 * 公钥解密
	 * 
	 * @param data
	 * @param certificatePath
	 * @return
	 * @throws Exception
	 */
	public static byte[] decryptByPublicKey(byte[] data, String certificatePath) throws Exception {
		// 取得公钥
		PublicKey publicKey = getPublicKey(certificatePath);

		// 对数据加密
		Cipher cipher = Cipher.getInstance(publicKey.getAlgorithm());
		cipher.init(Cipher.DECRYPT_MODE, publicKey);

		return cipher.doFinal(data);

	}

	/**
	 * 验证Certificate
	 * 
	 * @param certificatePath
	 * @return
	 */
	public static boolean verifyCertificate(String certificatePath) {
		return verifyCertificate(new Date(), certificatePath);
	}

	/**
	 * 验证Certificate是否过期或无效
	 * 
	 * @param date
	 * @param certificatePath
	 * @return
	 */
	public static boolean verifyCertificate(Date date, String certificatePath) {
		boolean status = true;
		try {
			// 取得证书
			Certificate certificate = getCertificate(certificatePath);
			// 验证证书是否过期或无效
			status = verifyCertificate(date, certificate);
		} catch (Exception e) {
			status = false;
		}
		return status;
	}

	/**
	 * 验证证书是否过期或无效
	 * 
	 * @param date
	 * @param certificate
	 * @return
	 */
	private static boolean verifyCertificate(Date date, Certificate certificate) {
		boolean status = true;
		try {
			X509Certificate x509Certificate = (X509Certificate) certificate;
			x509Certificate.checkValidity(date);
		} catch (Exception e) {
			status = false;
		}
		return status;
	}

	/**
	 * 签名
	 * 
	 * @param keyStorePath
	 * @param alias
	 * @param keyStorePassword
	 * @param aliasPassword
	 * @return
	 * @throws Exception
	 */
	public static byte[] sign(byte[] sign, String keyStorePath, String alias, String keyStorePassword, String aliasPassword)
			throws Exception {
		// 获得证书
		X509Certificate x509Certificate = (X509Certificate) getCertificate(keyStorePath, keyStorePassword, alias);

		// 取得私钥
		PrivateKey privateKey = getPrivateKey(keyStorePath, keyStorePassword, alias, aliasPassword);

		// 构建签名
		Signature signature = Signature.getInstance(x509Certificate.getSigAlgName());
		signature.initSign(privateKey);
		signature.update(sign);
		return signature.sign();
	}

	/**
	 * 验证签名
	 * 
	 * @param data
	 * @param sign
	 * @param certificatePath
	 * @return
	 * @throws Exception
	 */
	public static boolean verify(byte[] data, byte[] sign, String certificatePath) throws Exception {
		// 获得证书
		X509Certificate x509Certificate = (X509Certificate) getCertificate(certificatePath);
		// 获得公钥
		PublicKey publicKey = x509Certificate.getPublicKey();
		// 构建签名
		Signature signature = Signature.getInstance(x509Certificate.getSigAlgName());
		signature.initVerify(publicKey);
		signature.update(data);

		return signature.verify(sign);

	}

	/**
	 * 验证Certificate
	 * 
	 * @param keyStorePath
	 * @param keyStorePassword
	 * @param alias
	 * @return
	 */
	public static boolean verifyCertificate(Date date, String keyStorePath, String keyStorePassword, String alias) {
		boolean status = true;
		try {
			Certificate certificate = getCertificate(keyStorePath, keyStorePassword, alias);
			status = verifyCertificate(date, certificate);
		} catch (Exception e) {
			status = false;
		}
		return status;
	}

	/**
	 * 验证Certificate
	 * 
	 * @param keyStorePath
	 * @param keyStorePassword
	 * @param alias
	 * @return
	 */
	public static boolean verifyCertificate(String keyStorePath, String keyStorePassword, String alias) {
		return verifyCertificate(new Date(), keyStorePath, keyStorePassword, alias);
	}

}
