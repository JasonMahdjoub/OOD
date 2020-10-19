package com.distrimind.ood.database;
/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

jason.mahdjoub@distri-mind.fr

This software (Object Oriented Database (OOD)) is a computer program 
whose purpose is to manage a local database with the object paradigm 
and the java language

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

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 3.0.0
 */
public class DistantPostgreSQLDatabaseFactory extends DatabaseFactory<DistantPostgreSQLWrapper>{
	/**
	 * By default, network connections are SSL encrypted; this property permits secure connections to be turned off, or a different levels of security to be chosen.
	 */
	public enum SSLMode
	{
		/**
		 * I don't care about security and don't want to pay the overhead for encryption
		 */
		DISABLED,
		/**
		 *  I don't care about security but will pay the overhead for encryption if the server insists on it
		 */
		ALLOW,
		/**
		 * I don't care about encryption but will pay the overhead of encryption if the server supports it
		 */
		PREFER,
		/**
		 * I want my data to be encrypted, and I accept the overhead. I trust that the network will make sure I always connect to the server I want.
		 */
		REQUIRED,
		/**
		 * I want my data encrypted, and I accept the overhead. I want to be sure that I connect to a server that I trust.
		 */
		VERIFY_CA,
		/**
		 *  I want my data encrypted, and I accept the overhead. I want to be sure that I connect to a server I trust, and that it's the one I specify.
		 */
		VERIFY_FULL;

		String mode()
		{
			return name().replace("_", "-").toLowerCase();
		}
	}

	protected String urlLocation;
	protected int port;
	protected String databaseName;
	protected String user;
	protected String password;
	protected int loginTimeOutInSeconds=0;
	protected int connectTimeOutInSeconds=0;
	protected int socketTimeOutSeconds=0;
	protected String additionalParams =null;
	protected SSLMode sslMode= SSLMode.REQUIRED;
	protected String sslFactory ="org.postgresql.ssl.DefaultJavaSSLFactory";
	protected File sslKey =null;
	protected File sslCert =null;
	protected File sslRootCert =null;
	protected String sslHostNameVerifier ="org.postgresql.ssl.PGjdbcHostnameVerifier";
	protected String sslPasswordCallBack ="org.postgresql.ssl.jdbc4.LibPQFactory.ConsoleCallbackHandler";
	protected String sslPassword=null;
	protected int databaseMetadataCacheFields=65536;
	protected int databaseMetadataCacheFieldsMiB=5;
	protected int prepareThreshold=5;
	protected int preparedStatementCacheQueries=256;
	protected int preparedStatementCacheSizeMiB=5;
	protected int defaultRowFetchSize=0;
	/**
	 *
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 */
	public DistantPostgreSQLDatabaseFactory(String urlLocation, int port, String databaseName, String user, String password) {
		if (urlLocation==null)
			throw new NullPointerException();
		if (databaseName==null)
			throw new NullPointerException();
		if (user==null)
			throw new NullPointerException();
		if (password==null)
			throw new NullPointerException();
		this.urlLocation = urlLocation;
		this.port = port;
		this.databaseName = databaseName.toLowerCase();
		this.user = user;
		this.password = password;
		this.additionalParams =null;
	}

	/**
	 *
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 * @param additionalParams additional MySQL params
	 */
	public DistantPostgreSQLDatabaseFactory(String urlLocation, int port, String databaseName, String user, String password, String additionalParams) {
		this(urlLocation, port, databaseName, user, password);
		if (additionalParams ==null)
			throw new NullPointerException();
		this.additionalParams = additionalParams;
	}



	public DistantPostgreSQLDatabaseFactory() {
	}

	@Override
	protected DistantPostgreSQLWrapper newWrapperInstance() throws DatabaseException {
		return new DistantPostgreSQLWrapper(urlLocation, port, databaseName, user, password, loginTimeOutInSeconds, connectTimeOutInSeconds,
				socketTimeOutSeconds, additionalParams, sslMode, sslFactory,
				sslKey, sslCert, sslRootCert, sslHostNameVerifier, sslPasswordCallBack,
				sslPassword, databaseMetadataCacheFields, databaseMetadataCacheFieldsMiB,
				prepareThreshold, preparedStatementCacheQueries, preparedStatementCacheSizeMiB,
				defaultRowFetchSize );
	}

	/**
	 *
	 * @return the ssl mode
	 */
	public SSLMode getSslMode() {
		return sslMode;
	}

	/**
	 *
	 * @param sslMode the ssl mode
	 */
	public void setSslMode(SSLMode sslMode) {
		if (sslMode==null)
			throw new NullPointerException();
		this.sslMode = sslMode;
	}

	/**
	 *
	 * @return the url location
	 */
	public String getUrlLocation() {
		return urlLocation;
	}

	/**
	 *
	 * @param urlLocation the url location
	 */
	public void setUrlLocation(String urlLocation) {
		if (urlLocation==null)
			throw new NullPointerException();
		this.urlLocation = urlLocation;
	}

	/**
	 *
	 * @return the port to connect
	 */
	public int getPort() {
		return port;
	}

	/**
	 *
	 * @param port the port to connect
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 *
	 * @return the database name
	 */
	public String getDatabaseName() {
		return databaseName;
	}

	/**
	 *
	 * @param databaseName the database name
	 */
	public void setDatabaseName(String databaseName) {
		if (databaseName==null)
			throw new NullPointerException();
		this.databaseName = databaseName.toLowerCase();
	}

	/**
	 *
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 *
	 * @param user the user
	 */
	public void setUser(String user) {
		if (user==null)
			throw new NullPointerException();
		this.user = user;
	}

	/**
	 *
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 *
	 * @param password the password
	 */
	public void setPassword(String password) {
		if (password==null)
			throw new NullPointerException();
		this.password = password;
	}


	@Override
	public void deleteDatabase()  {
		throw new UnsupportedOperationException();
	}

	/**
	 *
	 * @return the personalized sql params for JDBC connection
	 */
	public String getAdditionalParams() {
		return additionalParams;
	}

	/**
	 *
	 * @param additionalParams the personalized sql params for JDBC connection
	 */
	public void setAdditionalParams(String additionalParams) {
		this.additionalParams = additionalParams;
	}


	/**
	 * Specify how long to wait for establishment of a database connection.
	 * @return  The timeout is specified in seconds.
	 */
	public int getLoginTimeOutInSeconds() {
		return loginTimeOutInSeconds;
	}

	/**
	 * Specify how long to wait for establishment of a database connection.
	 * @param loginTimeOutInSeconds  The timeout is specified in seconds.
	 */
	public void setLoginTimeOutInSeconds(int loginTimeOutInSeconds) {
		this.loginTimeOutInSeconds = loginTimeOutInSeconds;
	}

	/**
	 *
	 * @return The timeout value used for socket connect operations. If connecting to the server takes longer than this value, the connection is broken. The timeout is specified in seconds and a value of zero means that it is disabled.
	 */
	public int getConnectTimeOutInSeconds() {
		return connectTimeOutInSeconds;
	}

	/**
	 * The timeout value used for socket connect operations. If connecting to the server takes longer than this value, the connection is broken.
	 * @param connectTimeOutInSeconds The timeout is specified in seconds and a value of zero means that it is disabled.
	 */
	public void setConnectTimeOutInSeconds(int connectTimeOutInSeconds) {
		this.connectTimeOutInSeconds = connectTimeOutInSeconds;
	}

	/**
	 * The timeout value used for socket read operations. If reading from the server takes longer than this value, the connection is closed. This can be used as both a brute force global query timeout and a method of detecting network problems.
	 * @return  The timeout is specified in seconds and a value of zero means that it is disabled.
	 */
	public int getSocketTimeOutSeconds() {
		return socketTimeOutSeconds;
	}

	/**
	 * The timeout value used for socket read operations. If reading from the server takes longer than this value, the connection is closed. This can be used as both a brute force global query timeout and a method of detecting network problems.
	 * @param socketTimeOutSeconds The timeout is specified in seconds and a value of zero means that it is disabled.
	 */
	public void setSocketTimeOutSeconds(int socketTimeOutSeconds) {
		this.socketTimeOutSeconds = socketTimeOutSeconds;
	}


	/**
	 *
	 * @return The provided value is a class name to use as the SSLSocketFactory when establishing a SSL connection.
	 */
	public String getSslFactory() {
		return sslFactory;
	}

	/**
	 *
	 * @param sslFactory The provided value is a class name to use as the SSLSocketFactory when establishing a SSL connection.
	 */
	public void setSslFactory(String sslFactory) {
		if (sslFactory==null)
			throw new NullPointerException();
		this.sslFactory = sslFactory;
	}

	/**
	 * Provide the full path for the key file. Defaults to /defaultdir/postgresql.pk8.
	 *
	 * Note: The key file must be in PKCS-8 DER format. A PEM key can be converted to DER format using the openssl command:
	 *
	 * openssl pkcs8 -topk8 -inform PEM -in postgresql.key -outform DER -out postgresql.pk8 -v1 PBE-MD5-DES
	 *
	 * If your key has a password, provide it using the sslpassword connection parameter described below. Otherwise, you can add the flag -nocrypt to the above command to prevent the driver from requesting a password.
	 *
	 * Note: The use of -v1 PBE-MD5-DES might be inadequate in environments where high level of security is needed and the key is not protected by other means (e.g. access control of the OS), or the key file is transmitted in untrusted channels. We are depending on the cryptography providers provided by the java runtime. The solution documented here is known to work at the time of writing. If you have stricter security needs, please see https://stackoverflow.com/questions/58488774/configure-tomcat-hibernate-to-have-a-cryptographic-provider-supporting-1-2-840-1 for a discussion of the problem and information on choosing a better cipher suite.
	 * @return Provide the full path for the key file. Defaults to /defaultdir/postgresql.pk8.
	 */
	public File getSslKey() {
		return sslKey;
	}

	/**
	 * Provide the full path for the key file. Defaults to /defaultdir/postgresql.pk8.
	 *
	 * Note: The key file must be in PKCS-8 DER format. A PEM key can be converted to DER format using the openssl command:
	 *
	 * openssl pkcs8 -topk8 -inform PEM -in postgresql.key -outform DER -out postgresql.pk8 -v1 PBE-MD5-DES
	 *
	 * If your key has a password, provide it using the sslpassword connection parameter described below. Otherwise, you can add the flag -nocrypt to the above command to prevent the driver from requesting a password.
	 *
	 * Note: The use of -v1 PBE-MD5-DES might be inadequate in environments where high level of security is needed and the key is not protected by other means (e.g. access control of the OS), or the key file is transmitted in untrusted channels. We are depending on the cryptography providers provided by the java runtime. The solution documented here is known to work at the time of writing. If you have stricter security needs, please see https://stackoverflow.com/questions/58488774/configure-tomcat-hibernate-to-have-a-cryptographic-provider-supporting-1-2-840-1 for a discussion of the problem and information on choosing a better cipher suite.
	 * @param sslKey Provide the full path for the key file. Defaults to /defaultdir/postgresql.pk8.
	 */
	public void setSslKey(File sslKey) {
		if (sslKey ==null)
			throw new NullPointerException();
		this.sslKey = sslKey;
	}

	/**
	 *
	 * @return Provide the full path for the certificate file. Defaults to /defaultdir/postgresql.crt
	 *
	 * It can be a PEM encoded X509v3 certificate
	 */
	public File getSslCert() {
		return sslCert;
	}

	/**
	 *
	 * @param sslCert Provide the full path for the certificate file. Defaults to /defaultdir/postgresql.crt
	 *
	 * It can be a PEM encoded X509v3 certificate
	 */
	public void setSslCert(File sslCert) {
		if (sslCert ==null)
			throw new NullPointerException();
		this.sslCert = sslCert;
	}

	/**
	 *
	 * @return File name of the SSL root certificate. Defaults to defaultdir/root.crt
	 *
	 * It can be a PEM encoded X509v3 certificate
	 */
	public File getSslRootCert() {
		return sslRootCert;
	}

	/**
	 *
	 * @param sslRootCert File name of the SSL root certificate. Defaults to defaultdir/root.crt
	 *
	 * It can be a PEM encoded X509v3 certificate
	 */
	public void setSslRootCert(File sslRootCert) {
		if (sslRootCert ==null)
			throw new NullPointerException();
		this.sslRootCert = sslRootCert;
	}

	/**
	 *
	 * @return Class name of hostname verifier. Defaults to using org.postgresql.ssl.PGjdbcHostnameVerifier
	 */
	public String getSslHostNameVerifier() {
		return sslHostNameVerifier;
	}

	/**
	 *
	 * @param sslHostNameVerifier Class name of hostname verifier. Defaults to using org.postgresql.ssl.PGjdbcHostnameVerifier
	 */
	public void setSslHostNameVerifier(String sslHostNameVerifier) {
		if (sslHostNameVerifier==null)
			throw new NullPointerException();
		this.sslHostNameVerifier = sslHostNameVerifier;
	}

	/**
	 *
	 * @return Class name of the SSL password provider. Defaults to org.postgresql.ssl.jdbc4.LibPQFactory.ConsoleCallbackHandler
	 */
	public String getSslPasswordCallBack() {
		return sslPasswordCallBack;
	}

	/**
	 *
	 * @param sslPasswordCallBack Class name of the SSL password provider. Defaults to org.postgresql.ssl.jdbc4.LibPQFactory.ConsoleCallbackHandler
	 */
	public void setSslPasswordCallBack(String sslPasswordCallBack) {
		if (sslPasswordCallBack==null)
			throw new NullPointerException();
		this.sslPasswordCallBack = sslPasswordCallBack;
	}

	/**
	 *
	 * @return If provided will be used by ConsoleCallbackHandler
	 */
	public String getSslPassword() {
		return sslPassword;
	}

	/**
	 *
	 * @param sslPassword If provided will be used by ConsoleCallbackHandler
	 */
	public void setSslPassword(String sslPassword) {
		if (sslPassword==null)
			throw new NullPointerException();
		this.sslPassword = sslPassword;
	}

	/**
	 *
	 * @return Specifies the maximum number of fields to be cached per connection. A value of 0 disables the cache.
	 *
	 * Defaults to 65536.
	 */
	public int getDatabaseMetadataCacheFields() {
		return databaseMetadataCacheFields;
	}

	/**
	 *
	 * @param databaseMetadataCacheFields Specifies the maximum number of fields to be cached per connection. A value of 0 disables the cache.
	 *
	 * Defaults to 65536.
	 */
	public void setDatabaseMetadataCacheFields(int databaseMetadataCacheFields) {
		this.databaseMetadataCacheFields = databaseMetadataCacheFields;
	}

	/**
	 *
	 * @return Specifies the maximum size (in megabytes) of fields to be cached per connection. A value of 0 disables the cache.
	 *
	 * Defaults to 5.
	 */
	public int getDatabaseMetadataCacheFieldsMiB() {
		return databaseMetadataCacheFieldsMiB;
	}

	/**
	 *
	 * @param databaseMetadataCacheFieldsMiB Specifies the maximum size (in megabytes) of fields to be cached per connection. A value of 0 disables the cache.
	 *
	 * Defaults to 5.
	 */
	public void setDatabaseMetadataCacheFieldsMiB(int databaseMetadataCacheFieldsMiB) {
		this.databaseMetadataCacheFieldsMiB = databaseMetadataCacheFieldsMiB;
	}

	/**
	 *
	 * @return Determine the number of PreparedStatement executions required before switching over to use server side prepared statements. The default is five, meaning start using server side prepared statements on the fifth execution of the same PreparedStatement object. More information on server side prepared statements is available in the section called “Server Prepared Statements”.
	 */
	public int getPrepareThreshold() {
		return prepareThreshold;
	}

	/**
	 *
	 * @param prepareThreshold Determine the number of PreparedStatement executions required before switching over to use server side prepared statements. The default is five, meaning start using server side prepared statements on the fifth execution of the same PreparedStatement object. More information on server side prepared statements is available in the section called “Server Prepared Statements”.
	 */
	public void setPrepareThreshold(int prepareThreshold) {
		this.prepareThreshold = prepareThreshold;
	}

	/**
	 *
	 * @return Determine the number of queries that are cached in each connection. The default is 256, meaning if you use more than 256 different queries in prepareStatement() calls, the least recently used ones will be discarded. The cache allows application to benefit from “Server Prepared Statements” (see prepareThreshold) even if the prepared statement is closed after each execution. The value of 0 disables the cache.
	 */
	public int getPreparedStatementCacheQueries() {
		return preparedStatementCacheQueries;
	}

	/**
	 *
	 * @param preparedStatementCacheQueries Determine the number of queries that are cached in each connection. The default is 256, meaning if you use more than 256 different queries in prepareStatement() calls, the least recently used ones will be discarded. The cache allows application to benefit from “Server Prepared Statements” (see prepareThreshold) even if the prepared statement is closed after each execution. The value of 0 disables the cache.
	 */
	public void setPreparedStatementCacheQueries(int preparedStatementCacheQueries) {
		this.preparedStatementCacheQueries = preparedStatementCacheQueries;
	}

	/**
	 *
	 * @return Determine the maximum size (in mebibytes) of the prepared queries cache (see preparedStatementCacheQueries). The default is 5, meaning if you happen to cache more than 5 MiB of queries the least recently used ones will be discarded. The main aim of this setting is to prevent OutOfMemoryError. The value of 0 disables the cache.
	 */
	public int getPreparedStatementCacheSizeMiB() {
		return preparedStatementCacheSizeMiB;
	}

	/**
	 *
	 * @param preparedStatementCacheSizeMiB Determine the maximum size (in mebibytes) of the prepared queries cache (see preparedStatementCacheQueries). The default is 5, meaning if you happen to cache more than 5 MiB of queries the least recently used ones will be discarded. The main aim of this setting is to prevent OutOfMemoryError. The value of 0 disables the cache.
	 */
	public void setPreparedStatementCacheSizeMiB(int preparedStatementCacheSizeMiB) {
		this.preparedStatementCacheSizeMiB = preparedStatementCacheSizeMiB;
	}

	/**
	 *
	 * @return Determine the number of rows fetched in ResultSet by one fetch with trip to the database. Limiting the number of rows are fetch with each trip to the database allow avoids unnecessary memory consumption and as a consequence OutOfMemoryException.
	 *
	 * The default is zero, meaning that in ResultSet will be fetch all rows at once. Negative number is not available.
	 */
	public int getDefaultRowFetchSize() {
		return defaultRowFetchSize;
	}

	/**
	 * Determine the number of rows fetched in ResultSet by one fetch with trip to the database. Limiting the number of rows are fetch with each trip to the database allow avoids unnecessary memory consumption and as a consequence OutOfMemoryException.
	 *
	 *
	 * @param defaultRowFetchSize  The default is zero, meaning that in ResultSet will be fetch all rows at once. Negative number is not available.
	 */
	public void setDefaultRowFetchSize(int defaultRowFetchSize) {
		this.defaultRowFetchSize = defaultRowFetchSize;
	}
}
