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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 2.5.0
 */
public abstract class CommonMySQLDatabaseFactory<T extends DatabaseWrapper> extends DatabaseFactory<T>{
	protected String urlLocation;
	protected int port;
	protected String databaseName;
	protected String user;
	protected String password;
	protected int connectTimeInMillis=0;
	protected int socketTimeOutMillis=0;
	protected boolean useCompression=false;
	protected String characterEncoding= StandardCharsets.UTF_8.name();

	protected boolean autoReconnect=false;

	protected String additionalParams =null;

	/**
	 *
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 */
	public CommonMySQLDatabaseFactory(String urlLocation, int port, String databaseName, String user, String password) {
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
		this.additionalParams =null;
	}

	public CommonMySQLDatabaseFactory(String urlLocation, int port, String databaseName, String user,
									  String password, int connectTimeInMillis, int socketTimeOutMillis,
									  boolean useCompression, Charset characterEncoding,
									   boolean autoReconnect) {
		this(urlLocation, port, databaseName, user, password);
		if (characterEncoding==null)
			throw new NullPointerException();
		this.connectTimeInMillis = connectTimeInMillis;
		this.socketTimeOutMillis = socketTimeOutMillis;
		this.useCompression = useCompression;
		this.characterEncoding = characterEncoding.name();
		this.autoReconnect = autoReconnect;
		this.additionalParams =null;
	}

	/**
	 *
	 * @param urlLocation the url location
	 * @param port the port to connect
	 * @param databaseName the database name
	 * @param user the user
	 * @param password the password
	 * @param additionalParams the personalized sql params for JDBC connection
	 */
	public CommonMySQLDatabaseFactory(String urlLocation, int port, String databaseName, String user, String password, String additionalParams) {
		this(urlLocation, port, databaseName, user, password);
		if (additionalParams ==null)
			throw new NullPointerException();
		this.additionalParams = additionalParams;
	}

	public CommonMySQLDatabaseFactory() {
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


}
