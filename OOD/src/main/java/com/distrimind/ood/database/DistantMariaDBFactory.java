package com.distrimind.ood.database;
/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

jason.mahdjoub@distri-mind.fr

This software (Object Oriented Database (OOD)) is a computer program 
whose purpose is to manage a local database with the object paradigm 
and the java langage 

This software is governed by the CeCILL-C license under French law and
abiding by the rules of distribution of free software.  You can  use, 
modify and/ or redistribute the software under the terms of the CeCILL-C
license as circulated by CEA, CNRS and INRIA at the following URL
"http://www.cecill.info". 

As a counterpart to the access to the source code and  rights to copy,
modify and redistribute granted by the license, users are provided only
with a limited warranty  and the software's author,  the holder of the
economic rights,  and the successive licensors  have only  limited
liability. 

In this respect, the user's attention is drawn to the risks associated
with loading,  using,  modifying and/or developing or reproducing the
software by the user in light of its specific status of free software,
that may mean  that it is complicated to manipulate,  and  that  also
therefore means  that it is reserved for developers  and  experienced
professionals having in-depth computer knowledge. Users are therefore
encouraged to load and test the software's suitability as regards their
requirements in conditions enabling the security of their systems and/or 
data to be ensured and,  more generally, to use and operate it in the 
same conditions as regards security. 

The fact that you are presently reading this means that you have had
knowledge of the CeCILL-C license and that you accept its terms.
 */

import com.distrimind.ood.database.exceptions.DatabaseException;

import java.io.File;
import java.nio.charset.Charset;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.5.0
 */
public class DistantMariaDBFactory extends CommonMySQLDatabaseFactory<DistantMariaDBWrapper> {
	private boolean useSSL=false;
	private boolean trustServerCertificate=false;
	private String enabledSslProtocolSuites="TLSv1.2,TLSv1.3";
	private String enabledSslCipherSuites="TLS_DHE_RSA_WITH_AES_256_GCM_SHA256,TLS_DHE_DSS_WITH_AES_256_GCM_SHA256,TLS_DHE_RSA_WITH_AES_256_GCM_SHA384,TLS_DHE_DSS_WITH_AES_256_GCM_SHA384,TLS_DHE_RSA_WITH_AES_256_GCM_SHA512,TLS_DHE_DSS_WITH_AES_256_GCM_SHA512";
	private File serverSslCert=null;

	/**
	 *
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 */

	public DistantMariaDBFactory(String urlLocation, int port, String databaseName, String user, String password) {
		super(urlLocation, port, databaseName, user, password);
	}

	/**
	 *
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 * @param connectTimeInMillis Timeout for socket connect (in milliseconds), with 0 being no timeout. Defaults to '0'.
	 * @param socketTimeOutMillis Timeout (in milliseconds) on network socket operations (0, the default means no timeout).
	 * @param useCompression Use zlib compression when communicating with the server (true/false)? Defaults to 'false'.
	 * @param characterEncoding The character encoding to use for sending and retrieving TEXT, MEDIUMTEXT and LONGTEXT values instead of the configured connection characterEncoding
	 * @param autoReconnect Should the driver try to re-establish stale and/or dead connections? If enabled the driver will throw an exception for a queries issued on a stale or dead connection, which belong to the current transaction, but will attempt reconnect before the next query issued on the connection in a new transaction. The use of this feature is not recommended, because it has side effects related to session state and data consistency when applications don't handle SQLExceptions properly, and is only designed to be used when you are unable to configure your application to handle SQLExceptions resulting from dead and stale connections properly. Alternatively, as a last option, investigate setting the MySQL server variable "wait_timeout" to a high value, rather than the default of 8 hours.
	 * @param useSSL force SSL/TLS on connection
	 * @param trustServerCertificate When using SSL/TLS, do not check server's certificate.
	 * @param enabledSslProtocolSuites Force TLS/SSL protocol to a specific set of TLS versions (comma separated list).
	 * Example : "TLSv1, TLSv1.1, TLSv1.2"
	 * @param enabledSslCipherSuites Force TLS/SSL cipher (comma separated list).
	 * Example : "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384, TLS_DHE_DSS_WITH_AES_256_GCM_SHA384"
	 * @param serverSslCert Permits providing server's certificate in DER form, or server's CA certificate. The server will be added to trustStor. This permits a self-signed certificate to be trusted.
	 * Can be used in one of 3 forms :
	 * serverSslCert=/path/to/cert.pem (full path to certificate)
	 * serverSslCert=classpath:relative/cert.pem (relative to current classpath)
	 * or as verbatim DER-encoded certificate string "------BEGIN CERTIFICATE-----" .
	 *
	 */
	public DistantMariaDBFactory(String urlLocation, int port, String databaseName, String user, String password, int connectTimeInMillis, int socketTimeOutMillis, boolean useCompression, Charset characterEncoding, boolean autoReconnect,
								 boolean useSSL,
								 boolean trustServerCertificate,
								 String enabledSslProtocolSuites,
								 String enabledSslCipherSuites,
								 File serverSslCert) {
		super(urlLocation, port, databaseName, user, password, connectTimeInMillis, socketTimeOutMillis, useCompression, characterEncoding, autoReconnect);
		this.useSSL=useSSL;
		this.trustServerCertificate=trustServerCertificate;
		this.enabledSslCipherSuites=enabledSslCipherSuites;
		this.enabledSslProtocolSuites=enabledSslProtocolSuites;
		this.serverSslCert=serverSslCert;
	}
	/**
	 *
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 * @param additionalMariaDBParams additional MariaDB params
	 *
	 */
	public DistantMariaDBFactory(String urlLocation, int port, String databaseName, String user, String password, String additionalMariaDBParams) {
		super(urlLocation, port, databaseName, user, password, additionalMariaDBParams);
	}

	public DistantMariaDBFactory() {
	}

	@Override
	protected DistantMariaDBWrapper newWrapperInstance() throws DatabaseException {
		Charset cs=getCharacterEncoding();
		String css=null;
		if (cs.name().contains("-")) {
			for (String s : cs.aliases()) {
				if (!s.contains("-")) {
					css = s;
					break;
				}
			}
			if (css==null)
				css=cs.name();
		}
		else
			css=cs.name();
		if (additionalParams ==null)
			return new DistantMariaDBWrapper(urlLocation, port, databaseName, user, password, connectTimeInMillis, socketTimeOutMillis, useCompression, css, useSSL, trustServerCertificate, enabledSslProtocolSuites, enabledSslCipherSuites, serverSslCert, autoReconnect);
		else
			return new DistantMariaDBWrapper(urlLocation, port, databaseName, user, password, connectTimeInMillis, socketTimeOutMillis, useCompression, css, useSSL, trustServerCertificate, enabledSslProtocolSuites, enabledSslCipherSuites, serverSslCert, autoReconnect, additionalParams);
	}

	/**
	 *
	 * @return true if force SSL/TLS on connection
	 */
	public boolean isUseSSL() {
		return useSSL;
	}

	/**
	 *
	 * @param useSSL force SSL/TLS on connection
	 */
	public void setUseSSL(boolean useSSL) {
		this.useSSL = useSSL;
	}

	/**
	 *
	 * @return true when using SSL/TLS, do not check server's certificate.
	 */
	public boolean isTrustServerCertificate() {
		return trustServerCertificate;
	}

	/**
	 *
	 * @param trustServerCertificate When using SSL/TLS, do not check server's certificate.
	 */
	public void setTrustServerCertificate(boolean trustServerCertificate) {
		this.trustServerCertificate = trustServerCertificate;
	}

	/**
	 *
	 * @return TLS/SSL protocols (comma separated list).
	 */
	public String getEnabledSslProtocolSuites() {
		return enabledSslProtocolSuites;
	}

	/**
	 *
	 * @param enabledSslProtocolSuites Force TLS/SSL protocol to a specific set of TLS versions (comma separated list).
	 * 	 * Example : "TLSv1, TLSv1.1, TLSv1.2"
	 */
	public void setEnabledSslProtocolSuites(String enabledSslProtocolSuites) {
		this.enabledSslProtocolSuites = enabledSslProtocolSuites;
	}

	/**
	 *
	 * @return TLS/SSL ciphers (comma separated list).
	 */
	public String getEnabledSslCipherSuites() {
		return enabledSslCipherSuites;
	}

	/**
	 *
	 * @param enabledSslCipherSuites Force TLS/SSL cipher (comma separated list).
	 * 	 * Example : "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384, TLS_DHE_DSS_WITH_AES_256_GCM_SHA384"
	 */
	public void setEnabledSslCipherSuites(String enabledSslCipherSuites) {
		this.enabledSslCipherSuites = enabledSslCipherSuites;
	}

	/**
	 *
	 * @return server's certificate in DER form, or server's CA certificate.
	 */
	public File getServerSslCert() {
		return serverSslCert;
	}

	/**
	 *
	 * @param serverSslCert Permits providing server's certificate in DER form, or server's CA certificate. The server will be added to trustStor. This permits a self-signed certificate to be trusted.
	 * 	 * Can be used in one of 3 forms :
	 * 	 * serverSslCert=/path/to/cert.pem (full path to certificate)
	 * 	 * serverSslCert=classpath:relative/cert.pem (relative to current classpath)
	 * 	 * or as verbatim DER-encoded certificate string "------BEGIN CERTIFICATE-----" .
	 */
	public void setServerSslCert(File serverSslCert) {
		this.serverSslCert = serverSslCert;
	}
}
