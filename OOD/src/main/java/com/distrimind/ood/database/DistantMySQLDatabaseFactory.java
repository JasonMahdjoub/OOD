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
import java.nio.charset.StandardCharsets;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 2.5.0
 */
public class DistantMySQLDatabaseFactory extends DatabaseFactory<DistantMySQLDBWrapper>{
	private String urlLocation;
	private int port;
	private String databaseName;
	private String user;
	private String password;
	private int connectTimeInMillis=0;
	private int socketTimeOutMillis=0;
	private boolean useCompression=false;
	private String characterEncoding= StandardCharsets.UTF_8.name();
	private DistantMySQLDBWrapper.SSLMode sslMode=DistantMySQLDBWrapper.SSLMode.REQUIRED;
	private boolean paranoid=false;
	private File serverRSAPublicKeyFile=null;
	private boolean autoReconnect=false;
	private int prefetchNumberRows=0;
	private boolean noCache=true;
	private String mysqlParams=null;

	/**
	 *
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 */
	public DistantMySQLDatabaseFactory(String urlLocation, int port, String databaseName, String user, String password) {
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
		this.databaseName = databaseName;
		this.user = user;
		this.password = password;
		this.mysqlParams=null;
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
	 * @param sslMode  the ssl mode
	 * @param paranoid Take measures to prevent exposure sensitive information in error messages and clear data structures holding sensitive data when possible? (defaults to 'false')
	 * @param serverRSAPublicKeyFile File path to the server RSA public key file for sha256_password authentication. If not specified, the public key will be retrieved from the server.
	 * @param autoReconnect Should the driver try to re-establish stale and/or dead connections? If enabled the driver will throw an exception for a queries issued on a stale or dead connection, which belong to the current transaction, but will attempt reconnect before the next query issued on the connection in a new transaction. The use of this feature is not recommended, because it has side effects related to session state and data consistency when applications don't handle SQLExceptions properly, and is only designed to be used when you are unable to configure your application to handle SQLExceptions resulting from dead and stale connections properly. Alternatively, as a last option, investigate setting the MySQL server variable "wait_timeout" to a high value, rather than the default of 8 hours.
	 * @param prefetchNumberRows When set to a non-zero value N, causes all queries in the connection to return N rows at a time rather than the entire result set. Useful for queries against very large tables where it is not practical to retrieve the whole result set at once. You can scroll through the result set, N records at a time.
	 *
	 * This option works only with forward-only cursors. It does not work when the option parameter MULTI_STATEMENTS is set. It can be used in combination with the option parameter NO_CACHE. Its behavior in ADO applications is undefined: the prefetching might or might not occur. Added in 5.1.11.
	 * @param noCache Do not cache the results locally in the driver, instead read from server (mysql_use_result()). This works only for forward-only cursors. This option is very important in dealing with large tables when you do not want the driver to cache the entire result set.
	 */
	public DistantMySQLDatabaseFactory(String urlLocation, int port, String databaseName, String user,
									   String password, int connectTimeInMillis, int socketTimeOutMillis,
									   boolean useCompression, Charset characterEncoding,
									   DistantMySQLDBWrapper.SSLMode sslMode, boolean paranoid,
									   File serverRSAPublicKeyFile, boolean autoReconnect, int prefetchNumberRows, boolean noCache) {
		this(urlLocation, port, databaseName, user, password);
		if (sslMode==null)
			throw new NullPointerException();
		if (characterEncoding==null)
			throw new NullPointerException();
		this.connectTimeInMillis = connectTimeInMillis;
		this.socketTimeOutMillis = socketTimeOutMillis;
		this.useCompression = useCompression;
		this.characterEncoding = characterEncoding.name();
		this.sslMode = sslMode;
		this.paranoid = paranoid;
		this.serverRSAPublicKeyFile = serverRSAPublicKeyFile;
		this.autoReconnect = autoReconnect;
		this.prefetchNumberRows = prefetchNumberRows;
		this.noCache = noCache;
		this.mysqlParams=null;
	}

	/**
	 *
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 * @param mysqlParams the personalized sql params for JDBC connection
	 */
	public DistantMySQLDatabaseFactory(String urlLocation, int port, String databaseName, String user, String password, String mysqlParams) {
		if (mysqlParams==null)
			throw new NullPointerException();
		this.mysqlParams = mysqlParams;
	}

	public DistantMySQLDatabaseFactory() {
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
		this.databaseName = databaseName;
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

	/**
	 *
	 * @return Timeout for socket connect (in milliseconds), with 0 being no timeout. Defaults to '0'.
	 */
	public int getConnectTimeInMillis() {
		return connectTimeInMillis;
	}

	/**
	 *
	 * @param connectTimeInMillis Timeout for socket connect (in milliseconds), with 0 being no timeout. Defaults to '0'.
	 */
	public void setConnectTimeInMillis(int connectTimeInMillis) {
		this.connectTimeInMillis = connectTimeInMillis;
	}

	/**
	 *
	 * @return Timeout (in milliseconds) on network socket operations (0, the default means no timeout).
	 */
	public int getSocketTimeOutMillis() {
		return socketTimeOutMillis;
	}

	/**
	 *
	 * @param socketTimeOutMillis Timeout (in milliseconds) on network socket operations (0, the default means no timeout).
	 */
	public void setSocketTimeOutMillis(int socketTimeOutMillis) {
		this.socketTimeOutMillis = socketTimeOutMillis;
	}

	/**
	 *
	 * @return Use zlib compression when communicating with the server (true/false)? Defaults to 'false'.
	 */
	public boolean isUseCompression() {
		return useCompression;
	}

	/**
	 *
	 * @param useCompression Use zlib compression when communicating with the server (true/false)? Defaults to 'false'.
	 */
	public void setUseCompression(boolean useCompression) {
		this.useCompression = useCompression;
	}

	/**
	 *
	 * @return The character encoding to use for sending and retrieving TEXT, MEDIUMTEXT and LONGTEXT values instead of the configured connection characterEncoding
	 */
	public Charset getCharacterEncoding() {
		return Charset.forName(characterEncoding);
	}

	/**
	 *
	 * @param characterEncoding The character encoding to use for sending and retrieving TEXT, MEDIUMTEXT and LONGTEXT values instead of the configured connection characterEncoding
	 */
	public void setCharacterEncoding(Charset characterEncoding) {
		this.characterEncoding = characterEncoding.name();
	}

	/**
	 *
	 * @return the ssl mode
	 */
	public DistantMySQLDBWrapper.SSLMode getSslMode() {
		return sslMode;
	}

	/**
	 *
	 * @param sslMode the ssl mode
	 */
	public void setSslMode(DistantMySQLDBWrapper.SSLMode sslMode) {
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
	 * @return Should the driver try to re-establish stale and/or dead connections? If enabled the driver will throw an exception for a queries issued on a stale or dead connection, which belong to the current transaction, but will attempt reconnect before the next query issued on the connection in a new transaction. The use of this feature is not recommended, because it has side effects related to session state and data consistency when applications don't handle SQLExceptions properly, and is only designed to be used when you are unable to configure your application to handle SQLExceptions resulting from dead and stale connections properly. Alternatively, as a last option, investigate setting the MySQL server variable "wait_timeout" to a high value, rather than the default of 8 hours.
	 */
	public boolean isAutoReconnect() {
		return autoReconnect;
	}

	/**
	 *
	 * @param autoReconnect true if the driver should try re-establish stale and/or dead connections? If enabled the driver will throw an exception for a queries issued on a stale or dead connection, which belong to the current transaction, but will attempt reconnect before the next query issued on the connection in a new transaction. The use of this feature is not recommended, because it has side effects related to session state and data consistency when applications don't handle SQLExceptions properly, and is only designed to be used when you are unable to configure your application to handle SQLExceptions resulting from dead and stale connections properly. Alternatively, as a last option, investigate setting the MySQL server variable "wait_timeout" to a high value, rather than the default of 8 hours.
	 */
	public void setAutoReconnect(boolean autoReconnect) {
		this.autoReconnect = autoReconnect;
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

	/**
	 *
	 * @return the personalized sql params for JDBC connection
	 */
	public String getMysqlParams() {
		return mysqlParams;
	}

	/**
	 *
	 * @param mysqlParams the personalized sql params for JDBC connection
	 */
	public void setMysqlParams(String mysqlParams) {
		this.mysqlParams = mysqlParams;
	}

	@Override
	protected DistantMySQLDBWrapper newWrapperInstance() throws DatabaseException {
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
		if (mysqlParams==null)
			return new DistantMySQLDBWrapper(urlLocation, port, databaseName, user, password, connectTimeInMillis, socketTimeOutMillis, useCompression, css, sslMode, paranoid, serverRSAPublicKeyFile, autoReconnect, prefetchNumberRows, noCache);
		else
			return new DistantMySQLDBWrapper(urlLocation, port, databaseName, user, password, mysqlParams);
	}
}
