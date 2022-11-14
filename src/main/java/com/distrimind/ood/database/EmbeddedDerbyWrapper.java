
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
package com.distrimind.ood.database;

import com.distrimind.ood.database.Table.ColumnsReadQuery;
import com.distrimind.ood.database.Table.ReadQuery;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseLoadingException;
import com.distrimind.ood.database.exceptions.DatabaseVersionException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;
import com.distrimind.util.UtilClassLoader;
import com.distrimind.util.concurrent.ScheduledPoolExecutor;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.EncryptionProfileProvider;

import java.io.File;
import java.sql.*;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Sql connection wrapper for Derby DB
 * 
 * @author Jason Mahdjoub
 * @version 1.3
 * @since OOD 1.4
 */
class EmbeddedDerbyWrapper extends DatabaseWrapper {
	private static boolean derby_loaded = false;
	// private final String dbURL;



    private static void ensureDerbyLoading() throws DatabaseLoadingException {
		synchronized (EmbeddedDerbyWrapper.class) {
			if (!derby_loaded) {
				try {
					UtilClassLoader.getLoader().loadClass("org.apache.derby.jdbc.EmbeddedDriver").getDeclaredConstructor().newInstance();
					derby_loaded = true;
				} catch (Exception e) {
					throw new DatabaseLoadingException("Impossible to load Derby ", e);
				}
			}
		}
	}
	private static final class Finalizer extends DatabaseWrapper.Finalizer
	{

		private Finalizer(String databaseName, boolean loadToMemory, File databaseDirectory) {
			super(databaseName, loadToMemory, databaseDirectory);
		}

		@Override
		protected void closeConnection(Connection connection, boolean deepClose) throws SQLException {
			connection.close();
			/*
			 * try { //DriverManager.getConnection(dbURL + ";shutdown=true"); } catch
			 * (SQLNonTransientConnectionException e) {
			 *
			 * } finally { connection.close(); }
			 */
		}
	}
	@Override
	protected boolean supportMultipleAutoPrimaryKeys() {
		return true;
	}
	EmbeddedDerbyWrapper(ScheduledPoolExecutor defaultPoolExecutor, String databaseName, boolean loadToMemory,
						 DatabaseConfigurations databaseConfigurations,
						 DatabaseLifeCycles databaseLifeCycles,
						 EncryptionProfileProvider signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,
						 EncryptionProfileProvider encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup,
						 EncryptionProfileProvider protectedEncryptionProfileProviderForAuthenticatedP2PMessages,
						 AbstractSecureRandom secureRandom,
						 boolean createDatabasesIfNecessaryAndCheckIt) throws IllegalArgumentException, DatabaseException {
		super(new Finalizer(databaseName, false, null),  defaultPoolExecutor, true, databaseConfigurations, databaseLifeCycles,
				signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,
				encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup, protectedEncryptionProfileProviderForAuthenticatedP2PMessages,
				secureRandom, createDatabasesIfNecessaryAndCheckIt);
		if (!loadToMemory)
			throw new IllegalArgumentException();

	}

	@Override
	protected boolean supportSingleAutoPrimaryKeys()
	{
		return true;
	}

	EmbeddedDerbyWrapper(ScheduledPoolExecutor defaultPoolExecutor, File _directory,
						 DatabaseConfigurations databaseConfigurations,
						 DatabaseLifeCycles databaseLifeCycles,
						 EncryptionProfileProvider signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,
						 EncryptionProfileProvider encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup,
						 EncryptionProfileProvider protectedEncryptionProfileProviderForAuthenticatedP2PMessages,
						 AbstractSecureRandom secureRandom,
						 boolean createDatabasesIfNecessaryAndCheckIt, boolean alwaysDisconnectAfterOnTransaction) throws IllegalArgumentException, DatabaseException {
		super(new Finalizer(/* getConnection(_directory), */"Database from file : " + _directory.getAbsolutePath(), false, _directory),
				defaultPoolExecutor,
				alwaysDisconnectAfterOnTransaction,
				databaseConfigurations, databaseLifeCycles,
				signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,
				encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup, protectedEncryptionProfileProviderForAuthenticatedP2PMessages,
				secureRandom, createDatabasesIfNecessaryAndCheckIt);
		// dbURL = getDBUrl(_directory);

	}

	@Override
	public boolean supportTransactions() {
		return false;

	}


	private static String getDBUrl(File _directory) {
		return "jdbc:derby:" + _directory.getAbsolutePath();
	}

	private static Connection getConnection(String databaseName, File _directory, boolean loadToMemory) throws DatabaseLoadingException {

		System.gc();
		ensureDerbyLoading();
		try {
			Connection c;
			if (loadToMemory)
				c = DriverManager.getConnection("jdbc:derby:memory:" +(databaseName==null?"":databaseName)+";create=true");
			else {
				if (_directory == null)
					throw new NullPointerException("The parameter _file_name is a null pointer !");
				if (_directory.exists() && !_directory.isDirectory())
					throw new IllegalArgumentException("The given file name must be directory !");

				c = DriverManager.getConnection(getDBUrl(_directory) + ";create=true");
			}

			c.setAutoCommit(false);
			return c;
		} catch (Exception e) {
			throw new DatabaseLoadingException("Impossible to create the database " + databaseName
					+ " into the directory " + (_directory==null?null:_directory.getAbsolutePath()), e);
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
	public boolean supportNoCacheParam()
	{
		return true;
	}




	@Override
	protected boolean doesTableExists(String tableName) throws Exception {
		Connection sql_connection = getConnectionAssociatedWithCurrentThread().getConnection();
		DatabaseMetaData dbmd = sql_connection.getMetaData();
		ResultSet rs = dbmd.getTables(null, null, null, null);
		while (rs.next()) {
			if (rs.getString("TABLE_NAME").equals(tableName))
				return true;
		}
		return false;
	}

	@Override
	protected ColumnsReadQuery getColumnMetaData(String tableName, String columnName) throws Exception {
		Connection sql_connection = getConnectionAssociatedWithCurrentThread().getConnection();
		return new CReadQuery(sql_connection, sql_connection.getMetaData().getColumns(null, null, tableName, columnName));
	}

	@Override
	protected void checkConstraints(Table<?> table) throws DatabaseException {
		Connection sql_connection = getConnectionAssociatedWithCurrentThread().getConnection();
		try (ReadQuery rq = new ReadQuery(sql_connection,
				sql_connection.getMetaData().getPrimaryKeys(null, null, table.getSqlTableName()))) {
			while (rq.result_set.next()) {
				String pkName = rq.result_set.getString("PK_NAME");
				if (pkName != null && !pkName.equals(table.getSqlPrimaryKeyName()))
					throw new DatabaseVersionException(table,
							"There a grouped primary key named " + rq.result_set.getString("PK_NAME")
									+ " which should be named " + table.getSqlPrimaryKeyName());
			}
			try (ReadQuery rq2 = new ReadQuery(sql_connection,
					sql_connection.getMetaData().getIndexInfo(null, null, table.getSqlTableName(), false, false))) {
				while (rq2.result_set.next()) {
					String colName = rq2.result_set.getString("COLUMN_NAME");
					if (colName != null) {
						boolean found = false;
						for (FieldAccessor fa : table.getFieldAccessors()) {
							for (SqlField sf : fa.getDeclaredSqlFields()) {
								if (sf.shortFieldWithoutQuote.equals(colName)) {
									if (fa.isUnique() == rq2.result_set.getBoolean("NON_UNIQUE")
											&& fa.isPrimaryKey() == rq2.result_set.getBoolean("NON_UNIQUE")) {
										throw new DatabaseVersionException(table, "The field " + colName
												+ " has not the same unique constraint into the OOD database.");
									}
									found = true;
									break;
								}
							}
							if (found)
								break;
						}
						if (!found)
							throw new DatabaseVersionException(table, "There is a unique sql field " + colName
									+ " which does not exists into the OOD database.");
					}
				}
			}

		} catch (SQLException e) {
			throw new DatabaseException("Impossible to check constraints of the table " + table.getSqlTableName(), e);
		} catch (Exception e) {
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}
		for (Table<?> t : getListTables(table.getClass().getPackage(), getCurrentDatabaseVersion(table.getClass().getPackage()))) {
			try (ReadQuery rq = new ReadQuery(sql_connection,
					sql_connection.getMetaData().getExportedKeys(null, null, t.getSqlTableName()))) {
				while (rq.result_set.next()) {
					String fk_table_name = rq.result_set.getString("FKTABLE_NAME");
					if (fk_table_name.equals(table.getSqlTableName())) {
						String pointed_table = rq.result_set.getString("PKTABLE_NAME");
						String pointed_col = pointed_table + "." + rq.result_set.getString("PKCOLUMN_NAME");
						String fk = rq.result_set.getString("FKCOLUMN_NAME");
						boolean found = false;
						for (ForeignKeyFieldAccessor fa : table.getForeignKeysFieldAccessors()) {

							for (SqlField sf : fa.getDeclaredSqlFields()) {

								if (sf.shortFieldWithoutQuote.equals(fk) && sf.pointedField.equals(pointed_col)
										&& sf.pointedTable.equals(pointed_table)) {

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
				}
			} catch (SQLException e) {
				throw new DatabaseException("Impossible to check constraints of the table " + table.getSqlTableName(), e);
			} catch (Exception e) {
				throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
			}
		}
		try {
			Pattern col_size_matcher = Pattern.compile("([0-9]+)");
			for (FieldAccessor fa : table.getFieldAccessors()) {
				for (SqlField sf : fa.getDeclaredSqlFields()) {

					try (ColumnsReadQuery rq = getColumnMetaData(table.getSqlTableName(), sf.shortFieldWithoutQuote)) {
						if (rq.result_set.next()) {
							String type = rq.tableColumnsResultSet.getTypeName().toUpperCase();
							if (!sf.type.toUpperCase().startsWith(type))
								throw new DatabaseVersionException(table, "The type of the field " + sf.field
										+ " should  be " + sf.type + " and not " + type);
							if (col_size_matcher.matcher(sf.type).matches()) {
								int col_size = rq.tableColumnsResultSet.getColumnSize();
								Pattern pattern2 = Pattern.compile("(" + col_size + ")");
								if (!pattern2.matcher(sf.type).matches())
									throw new DatabaseVersionException(table, "The column " + sf.field
											+ " has a size equals to " + col_size + " (expected " + sf.type + ")");
							}
							boolean is_null = rq.tableColumnsResultSet.isNullable();
							if (is_null == sf.notNull)
								throw new DatabaseVersionException(table, "The field " + fa.getFieldName()
										+ " is expected to be " + (fa.isNotNull() ? "not null" : "nullable"));
							boolean is_autoincrement = rq.tableColumnsResultSet.isAutoIncrement();
							if (supportSingleAutoPrimaryKeys() && is_autoincrement != fa.isAutoPrimaryKey())
								throw new DatabaseVersionException(table,
										"The field " + fa.getFieldName() + " is " + (is_autoincrement ? "" : "not ")
												+ "autoincremented into the Sql database where it is "
												+ (is_autoincrement ? "not " : "") + " into the OOD database.");
							sf.sqlPosition = rq.tableColumnsResultSet.getOrdinalPosition();
						} else
							throw new DatabaseVersionException(table,
									"The field " + fa.getFieldName() + " was not found into the database.");
					}
					if (fa.isPrimaryKey()) {
						try (ReadQuery rq = new ReadQuery(sql_connection,
								sql_connection.getMetaData().getPrimaryKeys(null, null, table.getSqlTableName()))) {
							boolean found = false;
							while (rq.result_set.next()) {
								if (rq.result_set.getString("COLUMN_NAME").equals(sf.shortFieldWithoutQuote)
										&& rq.result_set.getString("PK_NAME").equals(table.getSqlPrimaryKeyName()))
									found = true;
							}
							if (!found)
								throw new DatabaseVersionException(table, "The field " + fa.getFieldName()
										+ " is not declared as a primary key into the Sql database.");
						}
					}
					if (fa.isForeignKey()) {
						boolean found = false;
						for (Table<?> t : getListTables(table.getClass().getPackage(), getCurrentDatabaseVersion(table.getClass().getPackage()))) {
							try (ReadQuery rq = new ReadQuery(sql_connection,
									sql_connection.getMetaData().getExportedKeys(null, null, t.getSqlTableName()))) {
								while (rq.result_set.next()) {
									String fk_table_name = rq.result_set.getString("FKTABLE_NAME");
									if (fk_table_name.equals(table.getSqlTableName())) {
										if (rq.result_set.getString("FKCOLUMN_NAME").equals(sf.shortFieldWithoutQuote))
											found = true;
									}
									if (found)
										break;
								}
							}
							if (found)
								break;
						}
						if (!found)
							throw new DatabaseVersionException(table,
									"The field " + fa.getFieldName() + " is a foreign key one of its Sql fields "
											+ sf.field + " is not a foreign key pointing to the table "
											+ sf.pointedTable);

					}
					//TODO check unique columns
					/*if (fa.isUnique()) {
						boolean found = false;
						try (ReadQuerry rq = new ReadQuerry(sql_connection,
								sql_connection.getMetaData().getIndexInfo(null, null, table.getSqlTableName(), false, false))) {
							while (rq.result_set.next()) {
								
								String columnName = rq.result_set.getString("COLUMN_NAME");
								if (columnName.equals(sf.short_field)) {
									
									if (!rq.result_set.getBoolean("NON_UNIQUE"))
										found = true;
									break;
								}
							}
						}
						if (!found)
							throw new DatabaseVersionException(table, "The OOD field " + fa.getFieldName()
									+ " is a unique key, but it not declared as unique into the Sql database.");
					}*/
				}
			}
		} catch (SQLException e) {
			throw new DatabaseException("Impossible to check constraints of the table " + table.getSqlTableName(), e);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	static class CReadQuery extends ColumnsReadQuery {

		public CReadQuery(Connection _sql_connection, ResultSet resultSet) {
			super(_sql_connection, resultSet);
			setTableColumnsResultSet(new TCResultSet(this.result_set));
		}

	}

	@Override
	protected boolean isDuplicateKeyException(SQLException e)
	{
		return e instanceof SQLIntegrityConstraintViolationException;
	}


	@Override
	protected String getAutoIncrementPart(String sqlTableName, String sqlFieldName, long startWith)
	{
		return "GENERATED BY DEFAULT AS IDENTITY(START WITH "+startWith+")";
	}

	protected String getSequenceQueryCreation(String sqlTableName, String sqlFieldName, long startWith)
	{
		return null;
	}

	@Override
	protected boolean autoPrimaryKeyIndexStartFromOne()
	{
		return false;
	}

	static class TCResultSet extends TableColumnsResultSet {

		TCResultSet(ResultSet _rs) {
			super(_rs);
		}

		@Override
		public String getColumnName() throws SQLException {
			return resultSet.getString("COLUMN_NAME");
		}

		@Override
		public String getTypeName() throws SQLException {
			return resultSet.getString("TYPE_NAME");
		}

		@Override
		public int getColumnSize() throws SQLException {
			return resultSet.getInt("COLUMN_SIZE");
		}

		@Override
		public boolean isNullable() throws SQLException {
			return resultSet.getString("IS_NULLABLE").equals("YES");
		}

		@Override
		public boolean isAutoIncrement() throws SQLException {
			return resultSet.getString("IS_AUTOINCREMENT").equals("YES");
		}

		@Override
		public int getOrdinalPosition() throws SQLException
		{
			return resultSet.getInt("ORDINAL_POSITION");
		}

	}

	@Override
	protected String getSqlComma() {
		return "";
	}

	@Override
	protected int getVarCharLimit() {
		return 32672;
	}

	@Override
	protected boolean isVarBinarySupported() {
		return false;
	}

	@Override
	protected boolean isLongVarBinarySupported() {
		return false;
	}
	protected String getBinaryBaseWord()
	{
		return null;
	}
	protected String getBlobBaseWord()
	{
		return "BLOB";
	}

	protected String getVarBinaryType(long limit)
	{
		return null;
	}

	protected String getLongVarBinaryType(long limit)
	{
		return null;
	}

	@Override
	protected String getSqlNULL() {
		return "";
	}

	@Override
	protected String getSqlNotNULL() {
		return "NOT NULL";
	}

	@Override
	protected String getByteType() {
		return "INTEGER";
	}

	@Override
	protected String getIntType() {
		return "INTEGER";
	}

	@Override
	protected String getBlobType(long limit) {
		return "BLOB("+limit+")";
	}

	@Override
	protected String getTextType(long limit)
	{
		return "CLOB(" + limit + ")";
	}
	@Override
	protected String getFloatType() {
		return "REAL";
	}

	@Override
	protected String getDoubleType() {
		return "DOUBLE";
	}

	@Override
	protected String getLongType() {
		return "BIGINT";
	}

	@Override
	protected String getShortType() {
		return "SMALLINT";
	}

	@Override
	protected String getBigDecimalType(long limit) {
		if (limit<=0)
			return "VARCHAR(1024)";
		else
			return "VARCHAR("+limit+")";
	}

	@Override
	public boolean supportsItalicQuotesWithTableAndFieldNames() {
		return false;
	}

	@Override
	protected String getBigIntegerType(long limit) {
		if (limit<=0)
			return "VARCHAR(1024)";
		else
			return "VARCHAR("+limit+")";
	}


	protected boolean useGetBigDecimalInResultSet()
	{
		return false;
	}

	@Override
	protected String getDateTimeType()
	{
		return "TIMESTAMP";
	}

	@Override
	protected String getLimitSqlPart(long startPosition, long rowLimit)
	{
		StringBuilder limit=new StringBuilder();
		if (rowLimit>=0)
		{
			limit.append(" { LIMIT ");
			limit.append(rowLimit);
			if (startPosition>0)
			{
				limit.append(" OFFSET ");
				limit.append(startPosition);
			}
			limit.append("}");
		}
		return limit.toString();
	}
	/*@Override
	protected String getSqlQuerryToGetLastGeneratedID() {
		return "values IDENTITY_VAL_LOCAL()";
	}*/

	@Override
	protected String getOnUpdateCascadeSqlQuery() {
		return "";
	}

	@Override
	protected String getOnDeleteCascadeSqlQuery() {
		return "ON DELETE CASCADE";
	}

	@Override
	protected Blob getBlob(byte[] _bytes) {
		return null;
	}

	/**
	 * Backup the database into the given directory.
	 * 
	 * @param directory
	 *            the directory where to save the database.
	 * @throws DatabaseException
	 *             if a problem occurs
	 */
	@Override
	public void nativeBackup(File directory) throws DatabaseException {
		if (directory == null)
			throw new NullPointerException("file");

		if (directory.exists()) {
			if (!directory.isDirectory())
				throw new IllegalArgumentException(
						"The given path (" + directory.getAbsolutePath() + ") must be a directory !");
		}
		final String f = directory.getAbsolutePath();

		final String querry = "CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)";
		this.runTransaction(new Transaction() {

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_READ_COMMITTED;
			}

			@Override
			public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
				try {
					finalizer.lockRead();
					Connection sql_connection = getConnectionAssociatedWithCurrentThread().getConnection();
                    try (PreparedStatement preparedStatement = sql_connection.prepareStatement(querry)) {
                        preparedStatement.setString(1, f);
                        preparedStatement.execute();
                    }
					return null;
				} catch (Exception e) {
					throw new DatabaseException("", e);
				} finally {
					finalizer.unlockRead();
				}
			}

			@Override
			public boolean doesWriteData() {
				return false;
			}

			@Override
			public void initOrReset() {
				
			}
		}, true);
	}

	@Override
	protected Connection reopenConnectionImpl() throws DatabaseLoadingException {
		return getConnection(finalizer.databaseName, getDatabaseDirectory(), isLoadedToMemory());
	}

	@Override
	protected void rollback(Connection openedConnection) throws SQLException {
		openedConnection.rollback();
	}

	@Override
	protected void commit(Connection openedConnection) throws SQLException, DatabaseException {
		getConnectionAssociatedWithCurrentThread().getConnection().commit();
	}

	@Override
	protected boolean supportSavePoint(Connection openedConnection) {
		return true;
	}

	@Override
	protected void rollback(Connection openedConnection, String _savePointName, Savepoint savepoint)
			throws SQLException {
		openedConnection.rollback(savepoint);

	}

	@Override
	protected void disableAutoCommit(Connection openedConnection) throws SQLException {
		openedConnection.setAutoCommit(false);
	}

	@Override
	protected Savepoint savePoint(Connection _openedConnection, String _savePoint) throws SQLException {
		return _openedConnection.setSavepoint(_savePoint);
	}

	@Override
	protected boolean isThreadSafe() {
		return false;
	}

	@Override
	protected void releasePoint(Connection _openedConnection, String _savePointName, Savepoint savepoint) {
		// _openedConnection.releaseSavepoint(savepoint);

	}

	@Override
	protected boolean supportFullSqlFieldName() {
		return false;
	}

	@SuppressWarnings("MagicConstant")
    @Override
	protected void startTransaction(Session _openedConnection, TransactionIsolation transactionIsolation, boolean write)
			throws SQLException 	{
		_openedConnection.getConnection().setTransactionIsolation(transactionIsolation.getCode());
	}


	@Override
	protected boolean isSerializationException(SQLException e) {
		return e.getSQLState().equals("40001");
	}

	@Override
	protected boolean isDisconnectionException(SQLException e) {
		return e.getErrorCode()==402 || e.getErrorCode()==1002;
	}

	@Override
	protected boolean isTransactionDeadLockException(SQLException e)
	{
		return false;
	}
	@Override
	protected String getDropTableCascadeQuery(Table<?> table)
	{
		return "DROP TABLE " + table.getSqlTableName() +
				" IF EXISTS CASCADE";
	}

	@Override
	protected boolean supportForeignKeys() {
		return true;
	}

}
