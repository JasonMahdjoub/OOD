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
import com.distrimind.util.crypto.WrappedPassword;

import java.io.File;
import java.nio.charset.Charset;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 2.5.0
 */
public class DistantMySQLDatabaseFactory extends CommonMySQLDatabaseFactory<DistantMySQLDBWrapper>{
	/**
	 * By default, network connections are SSL encrypted; this property permits secure connections to be turned off, or a different levels of security to be chosen.
	 */
	public enum SSLMode
	{
		/**
		 * Establish unencrypted connections
		 */
		DISABLED,
		/**
		 *  Establish encrypted connections if the server enabled them, otherwise fall back to unencrypted connections
		 */
		PREFERRED,
		/**
		 * Establish secure connections if the server enabled them, fail otherwise (default)
		 */
		REQUIRED,
		/**
		 * Like "REQUIRED" but additionally verify the server TLS certificate against the configured Certificate Authority (CA) certificates
		 */
		VERIFY_CA,
		/**
		 *  Like "VERIFY_CA", but additionally verify that the server certificate matches the host to which the connection is attempted
		 */
		VERIFY_IDENTITY
	}

	protected SSLMode sslMode=SSLMode.REQUIRED;
	protected boolean paranoid=false;
	protected File serverRSAPublicKeyFile=null;
	protected int prefetchNumberRows=0;
	protected boolean noCache=true;

	/**
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 */
	public DistantMySQLDatabaseFactory(String urlLocation, int port, String databaseName, String user, WrappedPassword password) throws DatabaseException {
		this(null, urlLocation, port, databaseName, user, password);
	}

	/**
	 * @param databaseConfigurations the database configurations
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 */
	public DistantMySQLDatabaseFactory(DatabaseConfigurations databaseConfigurations, String urlLocation, int port, String databaseName, String user, WrappedPassword password) throws DatabaseException {
		super(databaseConfigurations, urlLocation, port, databaseName, user, password);
	}
	/**
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 * @param connectTimeInMillis Timeout for socket connect (in milliseconds), with 0 being no timeout. Defaults to '0'.
	 * @param socketTimeOutMillis Timeout (in milliseconds) on network socket operations (0, the default means no timeout).
	 * @param useCompression Use zlib compression when communicating with the server (true/false)? Defaults to 'false'.
	 * @param characterEncoding The character encoding to use for sending and retrieving TEXT, MEDIUMTEXT and LONGTEXT values instead of the configured connection characterEncoding
	 * @param sslMode  the ssl mode
	 * @param paranoid Take measures to prevent exposure sensitive information in error messages and clear data structures holding sensitive data when possible? (defaults to 'false')
	 * @param serverRSAPublicKeyFile File path to the server RSA public key file for sha256_password authentication. If not specified, the public key will be retrieved from the server.
	 * @param autoReconnect Should the driver try to re-establish stale and/or dead connections? If enabled the driver will throw an exception for a queries issued on a stale or dead connection, which belong to the current transaction, but will attempt reconnect before the next query issued on the connection in a new transaction. The use of this feature is not recommended, because it has side effects related to session state and data consistency when applications don't handle SQLExceptions properly, and is only designed to be used when you are unable to configure your application to handle SQLExceptions resulting from dead and stale connections properly. Alternatively, as a last option, investigate setting the MySQL server variable "wait_timeout" to a high value, rather than the default of 8 hours.
	 * @param prefetchNumberRows When set to a non-zero value N, causes all queries in the connection to return N rows at a time rather than the entire result set. Useful for queries against very large tables where it is not practical to retrieve the whole result set at once. You can scroll through the result set, N records at a time.
	 * This option works only with forward-only cursors. It does not work when the option parameter MULTI_STATEMENTS is set. It can be used in combination with the option parameter NO_CACHE. Its behavior in ADO applications is undefined: the prefetching might or might not occur. Added in 5.1.11.
	 * @param noCache Do not cache the results locally in the driver, instead read from server (mysql_use_result()). This works only for forward-only cursors. This option is very important in dealing with large tables when you do not want the driver to cache the entire result set.
	 */
	public DistantMySQLDatabaseFactory(String urlLocation, int port, String databaseName, String user, WrappedPassword password, int connectTimeInMillis,
									   int socketTimeOutMillis, boolean useCompression, Charset characterEncoding, SSLMode sslMode, boolean paranoid,
									   File serverRSAPublicKeyFile, boolean autoReconnect, int prefetchNumberRows, boolean noCache) throws DatabaseException {
		this(null, urlLocation, port, databaseName, user, password, connectTimeInMillis, socketTimeOutMillis, useCompression, characterEncoding, sslMode,
				paranoid, serverRSAPublicKeyFile, autoReconnect, prefetchNumberRows, noCache);
	}

	/**
	 * @param databaseConfigurations the database configurations
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 * @param connectTimeInMillis Timeout for socket connect (in milliseconds), with 0 being no timeout. Defaults to '0'.
	 * @param socketTimeOutMillis Timeout (in milliseconds) on network socket operations (0, the default means no timeout).
	 * @param useCompression Use zlib compression when communicating with the server (true/false)? Defaults to 'false'.
	 * @param characterEncoding The character encoding to use for sending and retrieving TEXT, MEDIUMTEXT and LONGTEXT values instead of the configured connection characterEncoding
	 * @param sslMode  the ssl mode
	 * @param paranoid Take measures to prevent exposure sensitive information in error messages and clear data structures holding sensitive data when possible? (defaults to 'false')
	 * @param serverRSAPublicKeyFile File path to the server RSA public key file for sha256_password authentication. If not specified, the public key will be retrieved from the server.
	 * @param autoReconnect Should the driver try to re-establish stale and/or dead connections? If enabled the driver will throw an exception for a queries issued on a stale or dead connection, which belong to the current transaction, but will attempt reconnect before the next query issued on the connection in a new transaction. The use of this feature is not recommended, because it has side effects related to session state and data consistency when applications don't handle SQLExceptions properly, and is only designed to be used when you are unable to configure your application to handle SQLExceptions resulting from dead and stale connections properly. Alternatively, as a last option, investigate setting the MySQL server variable "wait_timeout" to a high value, rather than the default of 8 hours.
	 * @param prefetchNumberRows When set to a non-zero value N, causes all queries in the connection to return N rows at a time rather than the entire result set. Useful for queries against very large tables where it is not practical to retrieve the whole result set at once. You can scroll through the result set, N records at a time.
	 * This option works only with forward-only cursors. It does not work when the option parameter MULTI_STATEMENTS is set. It can be used in combination with the option parameter NO_CACHE. Its behavior in ADO applications is undefined: the prefetching might or might not occur. Added in 5.1.11.
	 * @param noCache Do not cache the results locally in the driver, instead read from server (mysql_use_result()). This works only for forward-only cursors. This option is very important in dealing with large tables when you do not want the driver to cache the entire result set.
	 */
	public DistantMySQLDatabaseFactory(DatabaseConfigurations databaseConfigurations, String urlLocation, int port, String databaseName, String user, WrappedPassword password, int connectTimeInMillis, int socketTimeOutMillis, boolean useCompression, Charset characterEncoding, SSLMode sslMode, boolean paranoid, File serverRSAPublicKeyFile, boolean autoReconnect, int prefetchNumberRows, boolean noCache) throws DatabaseException {
		super(databaseConfigurations, urlLocation, port, databaseName, user, password, connectTimeInMillis, socketTimeOutMillis, useCompression, characterEncoding, autoReconnect);
		if (sslMode==null)
			throw new NullPointerException();
		if (characterEncoding==null)
			throw new NullPointerException();
		this.sslMode = sslMode;
		this.paranoid = paranoid;
		this.serverRSAPublicKeyFile = serverRSAPublicKeyFile;
		this.prefetchNumberRows = prefetchNumberRows;
		this.noCache = noCache;
	}
	/**
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 * @param additionalMysqlParams additional MySQL params
	 */
	public DistantMySQLDatabaseFactory(String urlLocation, int port, String databaseName, String user, WrappedPassword password, String additionalMysqlParams) throws DatabaseException {
		super(null, urlLocation, port, databaseName, user, password, additionalMysqlParams);
	}
	/**
	 * @param databaseConfigurations the database configurations
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 * @param additionalMysqlParams additional MySQL params
	 */
	public DistantMySQLDatabaseFactory(DatabaseConfigurations databaseConfigurations, String urlLocation, int port, String databaseName, String user, WrappedPassword password, String additionalMysqlParams) throws DatabaseException {
		super(databaseConfigurations, urlLocation, port, databaseName, user, password, additionalMysqlParams);
	}

	public DistantMySQLDatabaseFactory() throws DatabaseException {
		super();
	}

	@Override
	protected DistantMySQLDBWrapper newWrapperInstanceImpl() throws DatabaseException {
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
			return new DistantMySQLDBWrapper(getDefaultPoolExecutor(), getContext(), databaseName, urlLocation, databaseConfigurations, getDatabaseLifeCycles(),
					signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup==null?null:signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
					encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup==null?null:encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
					protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages==null?null:protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages.getEncryptionProfileProviderSingleton(),
					getSecureRandom(), isCreateDatabasesIfNecessaryAndCheckIt(), port, user, password, connectTimeInMillis, socketTimeOutMillis, useCompression, css, sslMode, paranoid, serverRSAPublicKeyFile, autoReconnect, prefetchNumberRows, noCache);
		else
			return new DistantMySQLDBWrapper(getDefaultPoolExecutor(), getContext(), databaseName, urlLocation, databaseConfigurations, getDatabaseLifeCycles(),
					signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup==null?null:signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
					encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup==null?null:encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
					protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages==null?null:protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages.getEncryptionProfileProviderSingleton(),
					getSecureRandom(), isCreateDatabasesIfNecessaryAndCheckIt(), port, user, password, connectTimeInMillis, socketTimeOutMillis, useCompression, css, sslMode, paranoid, serverRSAPublicKeyFile, autoReconnect, prefetchNumberRows, noCache, additionalParams);
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
	 * @return true if take measures to prevent exposure sensitive information in error messages and clear data structures holding sensitive data when possible? (defaults to 'false')
	 */
	public boolean isParanoid() {
		return paranoid;
	}

	/**
	 *
	 * @param paranoid Take measures to prevent exposure sensitive information in error messages and clear data structures holding sensitive data when possible? (defaults to 'false')
	 */
	public void setParanoid(boolean paranoid) {
		this.paranoid = paranoid;
	}

	/**
	 *
	 * @return File path to the server RSA public key file for sha256_password authentication. If not specified, the public key will be retrieved from the server.
	 */
	public File getServerRSAPublicKeyFile() {
		return serverRSAPublicKeyFile;
	}

	/**
	 *
	 * @param serverRSAPublicKeyFile File path to the server RSA public key file for sha256_password authentication. If not specified, the public key will be retrieved from the server.
	 */
	public void setServerRSAPublicKeyFile(File serverRSAPublicKeyFile) {

		this.serverRSAPublicKeyFile = serverRSAPublicKeyFile;
	}
	/**
	 *
	 * @return prefetch
	 */
	public int getPrefetchNumberRows() {
		return prefetchNumberRows;
	}

	/**
	 *
	 * @param prefetchNumberRows When set to a non-zero value N, causes all queries in the connection to return N rows at a time rather than the entire result set. Useful for queries against very large tables where it is not practical to retrieve the whole result set at once. You can scroll through the result set, N records at a time.
	 * 	 *
	 * 	 * This option works only with forward-only cursors. It does not work when the option parameter MULTI_STATEMENTS is set. It can be used in combination with the option parameter NO_CACHE. Its behavior in ADO applications is undefined: the prefetching might or might not occur. Added in 5.1.11.
	 */
	public void setPrefetchNumberRows(int prefetchNumberRows) {
		this.prefetchNumberRows = prefetchNumberRows;
	}

	/**
	 *
	 * @return true if do not cache the results locally in the driver.
	 */
	public boolean isNoCache() {
		return noCache;
	}

	/**
	 *
	 * @param noCache Do not cache the results locally in the driver, instead read from server (mysql_use_result()). This works only for forward-only cursors. This option is very important in dealing with large tables when you do not want the driver to cache the entire result set.
	 */
	public void setNoCache(boolean noCache) {
		this.noCache = noCache;
	}

	@Override
	public void deleteDatabase()  {
		throw new UnsupportedOperationException();
	}

}
