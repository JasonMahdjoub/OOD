package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseVersionException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

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
									String characterEncoding,
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
								 String characterEncoding,
								 SSLMode sslMode,
								 boolean paranoid,
								 File serverRSAPublicKeyFile,
								 boolean autoReconnect,
								 int prefetchNumberRows,
								 boolean noCache)
	{

		return "jdbc:mysql://"+urlLocation+":"+port+"/"+_database_name+"?"+"connectTimeout="+connectTimeInMillis+"&socketTimeout="+socketTimeOutMillis+
						"&useCompression="+useCompression+"&characterEncoding="+characterEncoding+
						"&sslMode="+sslMode.name()+
						"&paranoid="+paranoid+
				(serverRSAPublicKeyFile!=null?"&serverRSAPublicKeyFile="+serverRSAPublicKeyFile.toURI().toString():"")+
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
		openedConnection.rollback();
	}

	@Override
	protected void commit(Connection openedConnection) throws SQLException {
		openedConnection.commit();
	}

	@Override
	protected boolean supportSavePoint(Connection openedConnection)  {
		return true;
	}

	@Override
	protected void rollback(Connection openedConnection, String savePointName, Savepoint savePoint) throws SQLException {
		openedConnection.rollback(savePoint);
	}

	@Override
	protected void disableAutoCommit(Connection openedConnection) throws SQLException {

		try (Statement s = openedConnection.createStatement()) {
			s.executeQuery("SET autocommit=0;");
		}

		try (Statement s = openedConnection.createStatement()) {
			s.executeQuery("COMMIT;");
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
	protected boolean isSerializationException(SQLException e)  {
		return false;
	}

	@Override
	protected boolean isDisconnectionException(SQLException e)  {
		return e.getErrorCode()==1152;
	}

	@Override
	protected boolean isDuplicateKeyException(SQLException e) {
		return e.getErrorCode()==1022;
	}

	@Override
	public String getAutoIncrementPart(long startWith) {
		return "AUTO_INCREMENT="+startWith;
	}

	@Override
	protected boolean doesTableExists(String tableName) throws Exception {
		try(ResultSet rs=getConnectionAssociatedWithCurrentThread().getConnection().getMetaData().getTables(null, null, "%", null)) {
			while (rs.next()) {
				if (rs.getString(3).equals(tableName))
					return true;
			}
			return false;
		}
	}

	static class CReadQuerry extends Table.ColumnsReadQuerry {

		public CReadQuerry(Connection _sql_connection, ResultSet resultSet) {
			super(_sql_connection, resultSet);
			setTableColumnsResultSet(new TCResultSet(this.result_set));
		}
	}

	static class TCResultSet extends TableColumnsResultSet {

		TCResultSet(ResultSet _rs) {
			super(_rs);
		}

		@Override
		public String getColumnName() throws SQLException {
			return resultSet.getString(4);
		}

		@Override
		public String getTypeName() throws SQLException {
			return resultSet.getString(6);
		}

		@Override
		public int getColumnSize() throws SQLException {
			return resultSet.getInt(7);
		}

		@Override
		public boolean isNullable() throws SQLException {
			return resultSet.getInt(11)==1;
		}

		@Override
		public boolean isAutoIncrement() throws SQLException {
			return resultSet.getInt(23)==1;
		}

		@Override
		public int getOrdinalPosition() throws SQLException
		{
			return resultSet.getInt(17);
		}

	}

	@Override
	protected Table.ColumnsReadQuerry getColumnMetaData(String tableName, String columnName) throws Exception {
		Connection c;
		try(ResultSet rs=(c=getConnectionAssociatedWithCurrentThread().getConnection()).getMetaData().getColumns(null, null, tableName, columnName)) {
			while (rs.next()) {
				if (rs.getString(3).equals(tableName) && rs.getString(4).equals(columnName)) {
					return new CReadQuerry(c, rs);
				}
			}
			return null;
		}
	}

	@Override
	protected void checkConstraints(Table<?> table) throws DatabaseException {
		Connection sql_connection = getConnectionAssociatedWithCurrentThread().getConnection();
		try (Table.ReadQuerry rq = new Table.ReadQuerry(sql_connection, new Table.SqlQuerry(
				"select CONSTRAINT_NAME, CONSTRAINT_TYPE from INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME='"
						+ table.getSqlTableName() + "' AND CONSTRAINT_SCHEMA='"+database_name+"';"))) {
			while (rq.result_set.next()) {
				String constraint_name = rq.result_set.getString("CONSTRAINT_NAME");
				String constraint_type = rq.result_set.getString("CONSTRAINT_TYPE");
				switch (constraint_type) {
					case "PRIMARY KEY": {
						if (!constraint_name.equals(table.getSqlPrimaryKeyName()))
							throw new DatabaseVersionException(table, "There a grouped primary key named " + constraint_name
									+ " which should be named " + table.getSqlPrimaryKeyName());
					}
					break;
					case "FOREIGN KEY": {

					}
					break;
					case "UNIQUE": {
						try (Table.ReadQuerry rq2 = new Table.ReadQuerry(sql_connection,
								new Table.SqlQuerry(
										"select COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"
												+ table.getSqlTableName() + "' AND CONSTRAINT_NAME='" + constraint_name + "' AND CONSTRAINT_SCHEMA='"+database_name+"';"))) {
							if (rq2.result_set.next()) {
								String col = (table.getSqlTableName() + "." + rq2.result_set.getString("COLUMN_NAME"))
										.toUpperCase();
								boolean found = false;
								for (FieldAccessor fa : table.getFieldAccessors()) {
									for (SqlField sf : fa.getDeclaredSqlFields()) {
										if (sf.field.equals(col) && fa.isUnique()) {
											found = true;
											break;
										}
									}
									if (found)
										break;
								}
								if (!found)
									throw new DatabaseVersionException(table, "There is a unique sql field " + col
											+ " which does not exists into the OOD database.");
							}
						}

					}
					break;
					case "CHECK":
						break;
					default:
						throw new DatabaseVersionException(table, "Unknow constraint " + constraint_type);
				}
			}
		} catch (SQLException e) {
			throw new DatabaseException("Impossible to check constraints of the table " + table.getClass().getSimpleName(), e);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
		try (Table.ReadQuerry rq = new Table.ReadQuerry(sql_connection, new Table.SqlQuerry(
				"select REFERENCED_TABLE_NAME as PKTABLE_NAME, REFERENCED_COLUMN_NAME as PKCOLUMN_NAME, COLUMN_NAME as FKCOLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"
						+ table.getSqlTableName() + "' AND CONSTRAINT_SCHEMA='"+database_name+"';"))) {
			while (rq.result_set.next()) {
				String pointed_table = rq.result_set.getString("PKTABLE_NAME");
				String pointed_col = pointed_table + "." + rq.result_set.getString("PKCOLUMN_NAME");
				String fk = table.getSqlTableName() + "." + rq.result_set.getString("FKCOLUMN_NAME");
				boolean found = false;
				for (ForeignKeyFieldAccessor fa : table.getForeignKeysFieldAccessors()) {
					for (SqlField sf : fa.getDeclaredSqlFields()) {
						if (sf.field.equals(fk) && sf.pointed_field.equals(pointed_col)
								&& sf.pointed_table.equals(pointed_table)) {
							found = true;
							break;
						}
					}
					if (found)
						break;
				}
				if (!found)
					throw new DatabaseVersionException(table,
							"There is foreign keys defined into the Sql database which have not been found in the OOD database.");
			}
		} catch (SQLException e) {
			throw new DatabaseException("Impossible to check constraints of the table " + table.getClass().getSimpleName(), e);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
		try {
			Pattern col_size_matcher = Pattern.compile("([0-9]+)");
			for (FieldAccessor fa : table.getFieldAccessors()) {
				for (SqlField sf : fa.getDeclaredSqlFields()) {
					Table.ColumnsReadQuerry cols=getColumnMetaData(table.getSqlTableName(), sf.short_field);
					if (cols==null)
						throw new DatabaseVersionException(table,
								"The field " + fa.getFieldName() + " was not found into the database.");
					String type = cols.tableColumnsResultSet.getTypeName().toUpperCase();
					if (!sf.type.toUpperCase().startsWith(type))
						throw new DatabaseVersionException(table, "The type of the field " + sf.field
								+ " should  be " + sf.type + " and not " + type);
					if (col_size_matcher.matcher(sf.type).matches()) {
						int col_size = cols.tableColumnsResultSet.getColumnSize();
						Pattern pattern2 = Pattern.compile("(" + col_size + ")");
						if (!pattern2.matcher(sf.type).matches())
							throw new DatabaseVersionException(table, "The column " + sf.field
									+ " has a size equals to " + col_size + " (expected " + sf.type + ")");
					}
					boolean is_null = cols.tableColumnsResultSet.isNullable();
					if (is_null == sf.not_null)
						throw new DatabaseVersionException(table, "The field " + fa.getFieldName()
								+ " is expected to be " + (fa.isNotNull() ? "not null" : "nullable"));
					boolean is_autoincrement = cols.tableColumnsResultSet.isAutoIncrement();
					if (is_autoincrement != fa.isAutoPrimaryKey())
						throw new DatabaseVersionException(table,
								"The field " + fa.getFieldName() + " is " + (is_autoincrement ? "" : "not ")
										+ "autoincremented into the Sql database where it is "
										+ (is_autoincrement ? "not " : "") + " into the OOD database.");
					sf.sql_position = cols.tableColumnsResultSet.getOrdinalPosition();

					if (fa.isPrimaryKey()) {
						try (Table.ReadQuerry rq = new Table.ReadQuerry(sql_connection,
								new Table.SqlQuerry(
										"select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"
												+ table.getSqlTableName() + "' AND COLUMN_NAME='" + sf.short_field
												+ "' AND CONSTRAINT_NAME='" + table.getSqlPrimaryKeyName() + "' AND CONSTRAINT_SCHEMA='"+database_name+"';"))) {
							if (!rq.result_set.next())
								throw new DatabaseVersionException(table, "The field " + fa.getFieldName()
										+ " is not declared as a primary key into the Sql database.");
						}
					}
					if (fa.isForeignKey()) {
						try (Table.ReadQuerry rq = new Table.ReadQuerry(sql_connection, new Table.SqlQuerry(
								"select REFERENCED_TABLE_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"
										+ table.getSqlTableName() + "' AND REFERENCED_TABLE_NAME='" + sf.pointed_table
										+ "' AND REFERENCED_COLUMN_NAME='" + sf.short_pointed_field + "' AND COLUMN_NAME='"
										+ sf.short_field + "'"))) {
							if (!rq.result_set.next())
								throw new DatabaseVersionException(table,
										"The field " + fa.getFieldName() + " is a foreign key. One of its Sql fields "
												+ sf.field + " is not a foreign key pointing to the table "
												+ sf.pointed_table);
						}
					}
					if (fa.isUnique()) {
						boolean found = false;
						try (Table.ReadQuerry rq = new Table.ReadQuerry(sql_connection, new Table.SqlQuerry(
								"select CONSTRAINT_NAME, CONSTRAINT_TYPE from INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME='"
										+ table.getSqlTableName() + "' AND CONSTRAINT_SCHEMA='"+database_name+"';"))) {
							while (rq.result_set.next()) {
								if (rq.result_set.getString("CONSTRAINT_TYPE").equals("UNIQUE")) {
									String constraint_name = rq.result_set.getString("CONSTRAINT_NAME");
									try (Table.ReadQuerry rq2 = new Table.ReadQuerry(sql_connection, new Table.SqlQuerry(
											"select COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"
													+ table.getSqlTableName() + "' AND CONSTRAINT_NAME='" + constraint_name
													+ "';"))) {
										if (rq2.result_set.next()) {
											String col = table.getSqlTableName() + "."
													+ rq2.result_set.getString("COLUMN_NAME");
											if (col.equals(sf.field)) {
												found = true;
												break;
											}
										}
									}
								}
							}
						}
						if (!found)
							throw new DatabaseVersionException(table, "The OOD field " + fa.getFieldName()
									+ " is a unique key, but it not declared as unique into the Sql database.");
					}
				}
			}
		} catch (SQLException e) {
			throw new DatabaseException("Impossible to check constraints of the table " + table.getClass().getSimpleName(), e);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	@Override
	protected String getSqlComma() {
		return ";";
	}

	@Override
	protected int getVarCharLimit() {
		return 65535;
	}

	@Override
	protected boolean isVarBinarySupported() {
		return true;
	}

	@Override
	protected boolean isLongVarBinarySupported() {
		return true;
	}

	@Override
	protected String getSqlNULL() {
		return "NULL";
	}

	@Override
	protected String getSqlNotNULL() {
		return "NOT NULLL";
	}

	@Override
	protected String getByteType() {
		return "TINYINT";
	}

	@Override
	protected String getIntType() {
		return "INT";
	}

	@Override
	protected String getSerializableType(long limit) {
		if (limit<=255)
			return "TINYBLOB";
		else if (limit<=65535)
			return "BLOB";
		else if (limit<=16777215)
			return "MEDIUMBLOB";
		return "LONGBLOB";
	}

	@Override
	protected String getFloatType() {
		return "FLOAT";
	}

	@Override
	protected String getDoubleType() {
		return "DOUBLE";
	}

	@Override
	protected String getShortType() {
		return "SMALLINT";
	}

	@Override
	protected String getLongType() {
		return "BIGINT";
	}

	@Override
	protected String getBigDecimalType() {
		return "DECIMAL";
	}

	@Override
	protected String getBigIntegerType() {
		return "VARCHAR(16374)";
	}

	@Override
	protected String getDropTableIfExistsKeyWord() {
		return "IF EXISTS";
	}

	@Override
	protected String getDropTableCascadeKeyWord() {
		return "CASCADE";
	}

	@Override
	protected String getOnUpdateCascadeSqlQuery() {
		return "ON UPDATE CASCADE";
	}

	@Override
	protected String getOnDeleteCascadeSqlQuery() {
		return "ON DELETE CASCADE";
	}

	private static final Constructor<? extends Blob> blobConstructor;
	static
	{
		Constructor<? extends Blob> bc=null;
		try {
			//noinspection unchecked
			bc=(Constructor<? extends Blob>)Class.forName("com.mysql.cj.jdbc.Blob").getDeclaredConstructor(byte[].class, Class.forName("com.mysql.cj.exceptions.ExceptionInterceptor"));
		} catch (NoSuchMethodException | ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		blobConstructor=bc;
	}

	@Override
	protected Blob getBlob(byte[] bytes) throws SQLException {
		try {
			return blobConstructor.newInstance(bytes, null);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException e) {
			throw new SQLException(e);
		}
		catch(InvocationTargetException e)
		{
			Throwable t=e.getCause();
			if (t instanceof SQLException)
				throw (SQLException)t;
			else
				throw new SQLException(t);
		}

	}

	@Override
	public void nativeBackup(File path)  {
		throw new UnsupportedOperationException();
	}

	@Override
	protected boolean isThreadSafe() {
		return true;
	}

	@Override
	protected boolean supportFullSqlFieldName() {
		return true;
	}
}
