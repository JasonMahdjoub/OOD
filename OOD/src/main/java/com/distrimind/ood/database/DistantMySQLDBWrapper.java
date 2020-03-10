package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;

import java.io.File;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.5.0
 */
public class DistantMySQLDBWrapper extends CommonMySQLWrapper{
	protected DistantMySQLDBWrapper(String urlLocation, int port, String _database_name, String user, String password, int connectTimeInMillis, int socketTimeOutMillis, boolean useCompression, String characterEncoding, SSLMode sslMode, boolean paranoid, File serverRSAPublicKeyFile, boolean autoReconnect, int prefetchNumberRows, boolean noCache) throws DatabaseException {
		super(urlLocation, port, _database_name, user, password, connectTimeInMillis, socketTimeOutMillis, useCompression, characterEncoding, sslMode, paranoid, serverRSAPublicKeyFile, autoReconnect, prefetchNumberRows, noCache);
	}

	protected DistantMySQLDBWrapper(String urlLocation, int port, String _database_name, String user, String password, String mysqlParams) throws DatabaseException {
		super(urlLocation, port, _database_name, user, password, mysqlParams);
	}
}
