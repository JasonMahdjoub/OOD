package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.5.0
 */
public class DistantMySQLDBWrapper extends DatabaseWrapper{
	private final String user;
	private final String password;
	private final String url;


	protected DistantMySQLDBWrapper(String urlLocation, int port, String _database_name, String user, String password,
									int connectTimeInMillis,
									int socketTimeOutMillis,
									boolean useCompression,
									Charset characterEncoding,
									SSLMode sslMode,
									boolean paranoid,
									File serverRSAPublicKeyFile,
									boolean autoReconnect,
									int prefetchNumberRows,
									boolean noCache) throws DatabaseException {
		super(_database_name, new File(getURL(urlLocation, port, _database_name, connectTimeInMillis, socketTimeOutMillis,useCompression, characterEncoding, sslMode, paranoid, serverRSAPublicKeyFile, autoReconnect, prefetchNumberRows, noCache)), false, false);
		url=getURL(urlLocation, port, _database_name, connectTimeInMillis, socketTimeOutMillis,useCompression, characterEncoding, sslMode, paranoid, serverRSAPublicKeyFile, autoReconnect, prefetchNumberRows, noCache);
		this.user=user;
		this.password=password;
	}
	protected DistantMySQLDBWrapper(String urlLocation, int port, String _database_name, String user, String password, String mysqlParams) throws DatabaseException {
		super(_database_name, new File(getURL(urlLocation, port, _database_name, mysqlParams)), false, false);
		url=getURL(urlLocation, port, _database_name, mysqlParams);
		this.user=user;
		this.password=password;
	}
	private static boolean driverLoaded=false;
	private void ensureJDBCDriverLoaded() throws DatabaseLoadingException {
		synchronized (DistantMySQLDBWrapper.class) {
			if (!driverLoaded) {
				try {
					Class.forName("com.mysql.jdbc.Driver");
					driverLoaded=true;
				} catch (ClassNotFoundException e) {
					throw new DatabaseLoadingException("",e);
				}
			}
		}
	}

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
	private static String getURL(String urlLocation, int port,
								 String _database_name,
								 String params)
	{

		return "jdbc:mysql://"+urlLocation+":"+port+"/"+_database_name+"?"+params;
	}
	private static String getURL(String urlLocation, int port,
								 String _database_name,
								 int connectTimeInMillis,
								 int socketTimeOutMillis,
								 boolean useCompression,
								 Charset characterEncoding,
								 SSLMode sslMode,
								 boolean paranoid,
								 File serverRSAPublicKeyFile,
								 boolean autoReconnect,
								 int prefetchNumberRows,
								 boolean noCache)
	{

		return "jdbc:mysql://"+urlLocation+":"+port+"/"+_database_name+"?"+"connectTimeout="+connectTimeInMillis+"&socketTimeout="+socketTimeOutMillis+
						"&useCompression="+useCompression+"&characterEncoding="+characterEncoding.name()+
						"&sslMode="+sslMode.name()+
						"&paranoid="+paranoid+
				(serverRSAPublicKeyFile!=null?"&serverRSAPublicKeyFile="+serverRSAPublicKeyFile.getAbsolutePath():"")+
				"&autoReconnect="+autoReconnect
				+"&prefetch="+prefetchNumberRows
				+"&NO_CACHE="+(noCache?"1":"0");
	}

	@Override
	protected Connection reopenConnectionImpl() throws DatabaseLoadingException {
		ensureJDBCDriverLoaded();
		try (Connection conn = DriverManager.getConnection(url, user, password)) {

			if (conn==null)
				throw new DatabaseLoadingException("Failed to make connection!");
			try (Statement s = conn.createStatement()) {
				s.executeQuery("SET autocommit=0;");
			}

			try (Statement s = c.createStatement()) {
				s.executeQuery("COMMIT;");
			}
			return conn;

		} catch (Exception e) {
			throw new DatabaseLoadingException("Failed to make connection!", e);
		}
	}

	@Override
	protected String getCachedKeyword() {
		return "";
	}

	@Override
	protected String getNotCachedKeyword() {
		return "";
	}

	@Override
	public boolean supportCache() {
		return false;
	}

	@Override
	public boolean supportNoCacheParam() {
		return false;
	}
	private final static AtomicBoolean databaseShutdown = new AtomicBoolean(false);

	@Override
	protected void closeConnection(Connection connection, boolean deepClosing) throws SQLException {
		if (databaseShutdown.getAndSet(true)) {
			connection.close();
		}
	}

	@Override
	protected void startTransaction(Session _openedConnection, TransactionIsolation transactionIsolation, boolean write) throws SQLException {
		String isoLevel;
		switch (transactionIsolation) {
			case TRANSACTION_NONE:
				isoLevel = null;
				break;
			case TRANSACTION_READ_COMMITTED:
				isoLevel = "READ COMMITTED";
				break;
			case TRANSACTION_READ_UNCOMMITTED:
				isoLevel = "READ UNCOMMITTED";
				break;
			case TRANSACTION_REPEATABLE_READ:
				isoLevel = "REPEATABLE READ";
				break;
			case TRANSACTION_SERIALIZABLE:
				isoLevel = "SERIALIZABLE";
				break;
			default:
				throw new IllegalAccessError();

		}

		try (Statement s = _openedConnection.getConnection().createStatement()) {
			s.executeQuery("START TRANSACTION" + (isoLevel != null ? (" ISOLATION LEVEL " + isoLevel + ", ") : "")
					+ (write ? "READ WRITE" : "READ ONLY") + getSqlComma());
		}

	}

	@Override
	protected void rollback(Connection openedConnection) throws SQLException {
		try (Statement s = openedConnection.createStatement()) {
			s.execute("ROLLBACK" + getSqlComma());
		}
	}

	@Override
	protected void commit(Connection openedConnection) throws SQLException {
		try (Statement s = openedConnection.createStatement()) {
			s.execute("COMMIT" + getSqlComma());
		}
	}

	@Override
	protected boolean supportSavePoint(Connection openedConnection) throws SQLException {
		return false;
	}

	@Override
	protected void rollback(Connection openedConnection, String savePointName, Savepoint savePoint) throws SQLException {

	}

	@Override
	protected void disableAutoCommit(Connection openedConnection) throws SQLException {

	}

	@Override
	protected Savepoint savePoint(Connection openedConnection, String savePoint) throws SQLException {
		return null;
	}

	@Override
	protected void releasePoint(Connection openedConnection, String _savePointName, Savepoint savepoint) throws SQLException {

	}

	@Override
	protected boolean isSerializationException(SQLException e) throws DatabaseException {
		return false;
	}

	@Override
	protected boolean isDisconnetionException(SQLException e) throws DatabaseException {
		return false;
	}

	@Override
	protected boolean isDuplicateKeyException(SQLException e) {
		return false;
	}

	@Override
	public String getAutoIncrementPart(long startWith) {
		return null;
	}

	@Override
	protected boolean doesTableExists(String tableName) throws Exception {
		return false;
	}

	@Override
	protected Table.ColumnsReadQuerry getColumnMetaData(String tableName, String columnName) throws Exception {
		return null;
	}

	@Override
	protected void checkConstraints(Table<?> table) throws DatabaseException {

	}

	@Override
	protected String getSqlComma() {
		return null;
	}

	@Override
	protected int getVarCharLimit() {
		return 0;
	}

	@Override
	protected boolean isVarBinarySupported() {
		return false;
	}

	@Override
	protected boolean isLongVarBinarySupported() {
		return false;
	}

	@Override
	protected String getSqlNULL() {
		return null;
	}

	@Override
	protected String getSqlNotNULL() {
		return null;
	}

	@Override
	protected String getByteType() {
		return null;
	}

	@Override
	protected String getIntType() {
		return null;
	}

	@Override
	protected String getSerializableType() {
		return null;
	}

	@Override
	protected String getFloatType() {
		return null;
	}

	@Override
	protected String getDoubleType() {
		return null;
	}

	@Override
	protected String getShortType() {
		return null;
	}

	@Override
	protected String getLongType() {
		return null;
	}

	@Override
	protected String getBigDecimalType() {
		return null;
	}

	@Override
	protected String getBigIntegerType() {
		return null;
	}

	@Override
	protected String getDropTableIfExistsKeyWord() {
		return null;
	}

	@Override
	protected String getDropTableCascadeKeyWord() {
		return null;
	}

	@Override
	protected String getOnUpdateCascadeSqlQuerry() {
		return null;
	}

	@Override
	protected String getOnDeleteCascadeSqlQuerry() {
		return null;
	}

	@Override
	protected Blob getBlob(byte[] bytes) throws SQLException {
		return null;
	}

	@Override
	public void nativeBackup(File path) throws DatabaseException {

	}

	@Override
	protected boolean isThreadSafe() {
		return false;
	}

	@Override
	protected boolean supportFullSqlFieldName() {
		return false;
	}
}
