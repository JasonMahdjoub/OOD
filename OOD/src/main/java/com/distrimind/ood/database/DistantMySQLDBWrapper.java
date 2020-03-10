package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;

import java.io.File;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.5.0
 */
public class DistantMySQLDBWrapper extends CommonMySQLWrapper{

	protected DistantMySQLDBWrapper(String urlLocation, int port, String _database_name, String user, String password, int connectTimeInMillis, int socketTimeOutMillis, boolean useCompression, String characterEncoding, DistantMySQLDatabaseFactory.SSLMode sslMode, boolean paranoid, File serverRSAPublicKeyFile, boolean autoReconnect, int prefetchNumberRows, boolean noCache, String additionalParams) throws DatabaseException {
		super(urlLocation, port, _database_name, user, password,
				getURL(urlLocation, port, _database_name, connectTimeInMillis, socketTimeOutMillis, useCompression, characterEncoding, sslMode, paranoid, serverRSAPublicKeyFile, autoReconnect, prefetchNumberRows, noCache, additionalParams), characterEncoding);
		if (additionalParams==null)
			throw new NullPointerException();
	}

	protected DistantMySQLDBWrapper(String urlLocation, int port, String _database_name, String user, String password, int connectTimeInMillis, int socketTimeOutMillis, boolean useCompression, String characterEncoding, DistantMySQLDatabaseFactory.SSLMode sslMode, boolean paranoid, File serverRSAPublicKeyFile, boolean autoReconnect, int prefetchNumberRows, boolean noCache) throws DatabaseException {
		super(urlLocation, port, _database_name, user, password, getURL(urlLocation, port, _database_name, connectTimeInMillis, socketTimeOutMillis, useCompression, characterEncoding, sslMode, paranoid, serverRSAPublicKeyFile, autoReconnect, prefetchNumberRows, noCache, null));
	}

	private static String getURL(String urlLocation, int port,
								 String _database_name,
								 int connectTimeInMillis,
								 int socketTimeOutMillis,
								 boolean useCompression,
								 String characterEncoding,
								 DistantMySQLDatabaseFactory.SSLMode sslMode,
								 boolean paranoid,
								 File serverRSAPublicKeyFile,
								 boolean autoReconnect,
								 int prefetchNumberRows,
								 boolean noCache,
								 String additionalParams)
	{

		return "jdbc:mysql://"+urlLocation+":"+port+"/"+_database_name+"?"+"connectTimeout="+connectTimeInMillis+"&socketTimeout="+socketTimeOutMillis+
				"&useCompression="+useCompression+"&characterEncoding="+characterEncoding+
				"&sslMode="+sslMode.name()+
				"&paranoid="+paranoid+
				(serverRSAPublicKeyFile!=null?"&serverRSAPublicKeyFile="+serverRSAPublicKeyFile.toURI().toString():"")+
				"&autoReconnect="+autoReconnect
				+"&prefetch="+prefetchNumberRows
				+"&NO_CACHE="+(noCache?"1":"0")
				+"&disableMariaDbDriver"
				+(additionalParams==null?"":additionalParams);
	}
}
