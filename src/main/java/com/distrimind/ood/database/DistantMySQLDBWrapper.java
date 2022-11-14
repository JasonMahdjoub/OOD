package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseLoadingException;
import com.distrimind.util.concurrent.ScheduledPoolExecutor;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.crypto.WrappedPassword;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.5.0
 */
public class DistantMySQLDBWrapper extends CommonMySQLWrapper{

	protected DistantMySQLDBWrapper(ScheduledPoolExecutor defaultPoolExecutor, String _database_name, String urlLocation,
									DatabaseConfigurations databaseConfigurations,
									DatabaseLifeCycles databaseLifeCycles,
									EncryptionProfileProvider signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,
									EncryptionProfileProvider encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup,
									EncryptionProfileProvider protectedEncryptionProfileProviderForAuthenticatedP2PMessages,
									AbstractSecureRandom secureRandom,
									boolean createDatabasesIfNecessaryAndCheckIt, int port, String user, WrappedPassword password, int connectTimeInMillis, int socketTimeOutMillis, boolean useCompression, String characterEncoding, DistantMySQLDatabaseFactory.SSLMode sslMode, boolean paranoid, File serverRSAPublicKeyFile, boolean autoReconnect, int prefetchNumberRows, boolean noCache, String additionalParams) throws DatabaseException {
		super(defaultPoolExecutor, _database_name, urlLocation, databaseConfigurations, databaseLifeCycles, signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,
				encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup, protectedEncryptionProfileProviderForAuthenticatedP2PMessages, secureRandom, createDatabasesIfNecessaryAndCheckIt, port, user, password,
				getURL(urlLocation, port, _database_name, connectTimeInMillis, socketTimeOutMillis, useCompression, characterEncoding, sslMode, paranoid, serverRSAPublicKeyFile, autoReconnect, prefetchNumberRows, noCache, additionalParams), characterEncoding);
		if (additionalParams==null)
			throw new NullPointerException();
	}

	protected DistantMySQLDBWrapper(ScheduledPoolExecutor defaultPoolExecutor, String _database_name, String urlLocation,
									DatabaseConfigurations databaseConfigurations,
									DatabaseLifeCycles databaseLifeCycles,
									EncryptionProfileProvider signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,
									EncryptionProfileProvider encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup,
									EncryptionProfileProvider protectedEncryptionProfileProviderForAuthenticatedP2PMessages,
									AbstractSecureRandom secureRandom,
									boolean createDatabasesIfNecessaryAndCheckIt, int port, String user, WrappedPassword password, int connectTimeInMillis, int socketTimeOutMillis, boolean useCompression, String characterEncoding, DistantMySQLDatabaseFactory.SSLMode sslMode, boolean paranoid, File serverRSAPublicKeyFile, boolean autoReconnect, int prefetchNumberRows, boolean noCache) throws DatabaseException {
		super(defaultPoolExecutor, _database_name, urlLocation, databaseConfigurations, databaseLifeCycles, signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,
				encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup, protectedEncryptionProfileProviderForAuthenticatedP2PMessages, secureRandom,
				createDatabasesIfNecessaryAndCheckIt, port, user, password, getURL(urlLocation, port, _database_name, connectTimeInMillis, socketTimeOutMillis, useCompression, characterEncoding, sslMode, paranoid, serverRSAPublicKeyFile, autoReconnect, prefetchNumberRows, noCache, null), characterEncoding);
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
				(serverRSAPublicKeyFile!=null?"&serverRSAPublicKeyFile="+ serverRSAPublicKeyFile.toURI() :"")+
				"&autoReconnect="+autoReconnect
				+"&prefetch="+prefetchNumberRows
				+"&NO_CACHE="+(noCache?"1":"0")
				+"&disableMariaDbDriver"
				+(additionalParams==null?"":additionalParams);
	}
	@Override
	protected Connection reopenConnectionImpl() throws DatabaseLoadingException {

		try  {
			Connection conn = DriverManager.getConnection(url, user, password.toString());
			if (conn==null)
				throw new DatabaseLoadingException("Failed to make connection!");

			return conn;

		} catch (Exception e) {
			throw new DatabaseLoadingException("Failed to make connection ! (user="+user+")", e);
		}
	}
	@Override
	protected Savepoint savePoint(Connection openedConnection, String savePoint) throws SQLException {
		return openedConnection.setSavepoint(savePoint);
	}

	@Override
	protected void releasePoint(Connection openedConnection, String _savePointName, Savepoint savepoint) throws SQLException {
		openedConnection.releaseSavepoint(savepoint);
	}
	@Override
	protected void rollback(Connection openedConnection, String savePointName, Savepoint savePoint) throws SQLException {
		openedConnection.rollback(savePoint);
	}
	@Override
	protected boolean supportSavePoint(Connection openedConnection)  {
		return true;
	}

	@Override
	protected boolean supportSingleAutoPrimaryKeys() {
		return false;
	}
}
