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
import com.distrimind.ood.database.exceptions.DatabaseVersionException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.crypto.WrappedPassword;

import java.io.File;
import java.sql.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class DistantPostgreSQLWrapper extends DatabaseWrapper{
	protected final String urlLocation;
	protected final int port;
	protected final String user;
	protected final WrappedPassword password;
	private final String url;


	protected DistantPostgreSQLWrapper(String databaseName,String urlLocation,
									   DatabaseConfigurations databaseConfigurations,
									   DatabaseLifeCycles databaseLifeCycles,
									   EncryptionProfileProvider encryptionProfileProviderForCentralDatabaseBackup,
									   EncryptionProfileProvider protectedEncryptionProfileProviderForAuthenticatedP2PMessages,
									   AbstractSecureRandom secureRandom,
									   boolean createDatabasesIfNecessaryAndCheckIt,
									   int port,
									   String user,
									   WrappedPassword password,
									   int loginTimeOutInSeconds,
									   int connectTimeOutInSeconds,
									   int socketTimeOutSeconds,
									   String additionalParams,
									   DistantPostgreSQLDatabaseFactory.SSLMode sslMode,
									   String sslFactory,
									   File sslKey,
									   File sslCert,
									   File sslRootCert,
									   String sslHostNameVerifier,
									   String sslPasswordCallBack,
									   WrappedPassword sslPassword,
									   int databaseMetadataCacheFields,
									   int databaseMetadataCacheFieldsMiB,
									   int prepareThreshold,
									   int preparedStatementCacheQueries,
									   int preparedStatementCacheSizeMiB,
									   int defaultRowFetchSize) throws DatabaseException {
		super(databaseName, new File(urlLocation), false, false, databaseConfigurations, databaseLifeCycles, encryptionProfileProviderForCentralDatabaseBackup, protectedEncryptionProfileProviderForAuthenticatedP2PMessages,
				secureRandom, createDatabasesIfNecessaryAndCheckIt);
		url=getURL(urlLocation, port, databaseName, loginTimeOutInSeconds, connectTimeOutInSeconds, socketTimeOutSeconds, additionalParams, sslMode, sslFactory, sslKey, sslCert, sslRootCert, sslHostNameVerifier, sslPasswordCallBack, sslPassword, databaseMetadataCacheFields, databaseMetadataCacheFieldsMiB, prepareThreshold, preparedStatementCacheQueries, preparedStatementCacheSizeMiB, defaultRowFetchSize);
		this.urlLocation = urlLocation;
		this.port = port;
		this.user = user;
		this.password = password;
	}



	private static String getURL(String urlLocation,
								 int port,
								 String databaseName,
								 int loginTimeOutInSeconds,
								 int connectTimeOutInSeconds,
								 int socketTimeOutSeconds,
								 String additionalParams,
								 DistantPostgreSQLDatabaseFactory.SSLMode sslMode,
								 String sslFactory,
								 File sslKey,
								 File sslCert,
								 File sslRootCert,
								 String sslHostNameVerifier,
								 String sslPasswordCallBack,
								 WrappedPassword sslPassword,
								 int databaseMetadataCacheFields,
								 int databaseMetadataCacheFieldsMiB,
								 int prepareThreshold,
								 int preparedStatementCacheQueries,
								 int preparedStatementCacheSizeMiB,
								 int defaultRowFetchSize)
	{

		return "jdbc:postgresql://"+urlLocation+":"+port+"/"+databaseName+"?"+"loginTimeout="+loginTimeOutInSeconds+"&connectTimeout="+connectTimeOutInSeconds+
				"&socketTimeout="+socketTimeOutSeconds+
				"&sslMode="+sslMode.mode()+
				"&sslFactory="+sslFactory+
				(sslKey==null?"":"&sslKey="+sslKey.toURI().toString())+
				(sslCert==null?"":"&sslCert="+sslCert.toURI().toString())+
				(sslRootCert==null?"":"&sslRootCert="+sslRootCert.toURI().toString())+
				"&sslHostNameVerifier="+sslHostNameVerifier+
				"&sslPasswordCallBack="+sslPasswordCallBack+
				((sslPassword==null || sslPassword.toString()==null)?"":"&sslPassword="+sslPassword)+
				"&databaseMetadataCacheFields="+databaseMetadataCacheFields+
				"&databaseMetadataCacheFieldsMiB="+databaseMetadataCacheFieldsMiB+
				"&prepareThreshold="+prepareThreshold+
				"&preparedStatementCacheQueries="+preparedStatementCacheQueries+
				"&preparedStatementCacheSizeMiB="+preparedStatementCacheSizeMiB+
				"&defaultRowFetchSize="+defaultRowFetchSize+
				"&sslPasswordCallBack="+sslPasswordCallBack+
				(additionalParams==null?"":additionalParams);
	}
	@Override
	protected Connection reopenConnectionImpl() throws DatabaseLoadingException {

		try  {
			Connection conn = DriverManager.getConnection(url, user, password.toString());
			if (conn==null)
				throw new DatabaseLoadingException("Failed to make connection!");
			return conn;

		} catch (Exception e) {
			throw new DatabaseLoadingException("Failed to make connection!", e);
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
		_openedConnection.getConnection().setReadOnly(!write);
		//noinspection MagicConstant
		_openedConnection.getConnection().setTransactionIsolation(transactionIsolation.getCode());
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
	protected boolean mustReleaseSavepointAfterRollBack()
	{
		return false;
	}

	@Override
	protected boolean mustReleaseSavepointAfterCommit()
	{
		return false;
	}

	@Override
	protected void disableAutoCommit(Connection openedConnection) throws SQLException {
		openedConnection.setAutoCommit(false);
	}



	@Override
	protected boolean isSerializationException(SQLException e)  {
		return e.getSQLState()!=null && e.getSQLState().equals("40001");
	}

	@Override
	protected boolean isTransactionDeadLockException(SQLException e)
	{
		return e.getSQLState()!=null && e.getSQLState().equals("40P01");
	}

	@Override
	protected boolean isDisconnectionException(SQLException e)  {
		return e.getSQLState()!=null && e.getSQLState().startsWith("80");
	}

	@Override
	protected boolean isDuplicateKeyException(SQLException e) {
		return e.getSQLState()!=null && e.getSQLState().equals("23505");
	}

	private String getSequenceName(String sqlTableName, String sqlFieldName)
	{
		return sqlTableName+"_"+sqlFieldName+"_seq";
	}

	@Override
	public String getAutoIncrementPart(String sqlTableName, String sqlFieldName, long startWith) {
		return "DEFAULT nextval('"+getSequenceName(sqlTableName, sqlFieldName)+"')";
	}

	public String getSequenceQueryCreation(String sqlTableName, String sqlFieldName, long startWith)
	{
		return "CREATE SEQUENCE "+getSequenceName(sqlTableName, sqlFieldName)+" START WITH "+startWith;
	}

	@Override
	protected boolean doesTableExists(String tableName) throws Exception {
		try(ResultSet rs=getConnectionAssociatedWithCurrentThread().getConnection().getMetaData().getTables(databaseName, null, tableName.toLowerCase(), null)) {
			return rs.next();
			/*while (rs.next()) {
				if (rs.getString(3).equals(tableName) && rs.getString().equals(database_name))
					return true;
			}
			return false;*/
		}
	}

	static class CReadQuery extends Table.ColumnsReadQuery {

		public CReadQuery(Connection _sql_connection, ResultSet resultSet) {
			super(_sql_connection, resultSet);
			setTableColumnsResultSet(new TCResultSet(resultSet));
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
			return !resultSet.getString(23).equals("NO");
		}

		@Override
		public int getOrdinalPosition() throws SQLException
		{
			return resultSet.getInt(17);
		}

	}

	@Override
	protected Table.ColumnsReadQuery getColumnMetaData(String tableName, String columnName) throws Exception {
		Connection c;
		ResultSet rs=(c=getConnectionAssociatedWithCurrentThread().getConnection()).getMetaData().getColumns(databaseName, null, tableName==null?null:tableName.toLowerCase(), columnName==null?null:columnName.toLowerCase());
		return new CReadQuery(c, rs);

	}

	@Override
	protected boolean autoPrimaryKeyIndexStartFromOne()
	{
		return false;
	}

	@Override
	protected void checkConstraints(Table<?> table) throws DatabaseException {
		Connection sql_connection = getConnectionAssociatedWithCurrentThread().getConnection();
		try (Table.ReadQuery rq = new Table.ReadQuery(sql_connection, new Table.SqlQuery(
				"select CONSTRAINT_NAME, CONSTRAINT_TYPE from INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME='"
						+ table.getSqlTableName() + "' AND CONSTRAINT_SCHEMA='"+ databaseName +"';"))) {
			while (rq.result_set.next()) {
				String constraint_name = rq.result_set.getString("CONSTRAINT_NAME");
				String constraint_type = rq.result_set.getString("CONSTRAINT_TYPE");
				switch (constraint_type) {
					case "PRIMARY KEY": {
						if (!constraint_name.equals("PRIMARY"))
							throw new DatabaseVersionException(table, "There a grouped primary key named " + constraint_name
									+ " which should be named PRIMARY");
					}
					break;
					case "FOREIGN KEY": {

					}
					break;
					case "UNIQUE": {
						try (Table.ReadQuery rq2 = new Table.ReadQuery(sql_connection,
								new Table.SqlQuery(
										"select COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"
												+ table.getSqlTableName() + "' AND CONSTRAINT_NAME='" + constraint_name + "' AND CONSTRAINT_SCHEMA='"+ databaseName +"';"))) {
							if (rq2.result_set.next()) {
								String col = (table.getSqlTableName() + "." + rq2.result_set.getString("COLUMN_NAME"))
										.toUpperCase();
								boolean found = false;
								for (FieldAccessor fa : table.getFieldAccessors()) {
									for (SqlField sf : fa.getDeclaredSqlFields()) {
										if (sf.field_without_quote.equals(col) && fa.isUnique()) {
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
						throw new DatabaseVersionException(table, "Unknown constraint " + constraint_type);
				}
			}
		} catch (SQLException e) {
			throw new DatabaseException("Impossible to check constraints of the table " + table.getClass().getSimpleName(), e);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
		try (Table.ReadQuery rq = new Table.ReadQuery(sql_connection, new Table.SqlQuery(
				"select COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"
						+ table.getSqlTableName() + "' AND CONSTRAINT_SCHEMA='"+ databaseName +"';"))) {
			while (rq.result_set.next()) {
				/*String pointed_table = rq.result_set.getString("REFERENCED_TABLE_NAME");
				String pointed_col = pointed_table + "." + rq.result_set.getString("REFERENCED_COLUMN_NAME");*/
				String fk = table.getSqlTableName() + "." + rq.result_set.getString("COLUMN_NAME");
				/*if (pointed_table==null)
					continue;*/
				boolean found = false;
				for (ForeignKeyFieldAccessor fa : table.getForeignKeysFieldAccessors()) {
					for (SqlField sf : fa.getDeclaredSqlFields()) {
						if (sf.field_without_quote.equals(fk)/* && sf.pointed_field_without_quote.equals(pointed_col)
								&& sf.pointed_table_without_quote.equals(pointed_table)*/) {
							found = true;
							break;
						}
					}
					if (found)
						break;
				}
				if (!found)
					throw new DatabaseVersionException(table,
							"There is foreign keys defined into the Sql database which have not been found in the OOD database");
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
					Table.ColumnsReadQuery cols=getColumnMetaData(table.getSqlTableName(), sf.short_field_without_quote);
					if (cols==null || !cols.tableColumnsResultSet.next())
						throw new DatabaseVersionException(table,
								"The field " + fa.getFieldName() + " was not found into the database.");
					String type = cols.tableColumnsResultSet.getTypeName().toUpperCase();
					if (!sf.type.toUpperCase().startsWith(type) && !(type.equals("BIT") && sf.type.equals("BOOLEAN")))
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
					if (supportSingleAutoPrimaryKeys() && is_autoincrement != fa.isAutoPrimaryKey())
						throw new DatabaseVersionException(table,
								"The field " + fa.getFieldName() + " is " + (is_autoincrement ? "" : "not ")
										+ "autoincremented into the Sql database where it is "
										+ (is_autoincrement ? "not " : "") + " into the OOD database.");
					sf.sql_position = cols.tableColumnsResultSet.getOrdinalPosition();

					if (fa.isPrimaryKey()) {
						try (Table.ReadQuery rq = new Table.ReadQuery(sql_connection,
								new Table.SqlQuery(
										"select COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"
												+ table.getSqlTableName().toLowerCase() + "' AND COLUMN_NAME='" + sf.short_field_without_quote.toLowerCase()
												+ "' AND CONSTRAINT_NAME='"+table.getSqlPrimaryKeyName().toLowerCase()+"';"))) {
							if (!rq.result_set.next())
								throw new DatabaseVersionException(table, "The field " + fa.getFieldName()
										+ " is not declared as a primary key into the Sql database.");
						}
					}
					if (fa.isForeignKey()) {
						String constraintName=table.getSqlTableName().toLowerCase()+"_%"+sf.short_field_without_quote.toLowerCase()+"%_fkey";
						constraintName=constraintName.replace("_", "!_");
						try (Table.ReadQuery rq = new Table.ReadQuery(sql_connection, new Table.SqlQuery(
								"select COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"
										+ table.getSqlTableName().toLowerCase()
										+ "' AND CONSTRAINT_NAME LIKE '" + constraintName+"' ESCAPE '!'"
										+ " AND COLUMN_NAME='"+ sf.short_field_without_quote.toLowerCase() + "';"))) {
							if (!rq.result_set.next())
								throw new DatabaseVersionException(table,
										"The field " + fa.getFieldName() + " is a foreign key. One of its Sql fields "
												+ sf.field + " is not a foreign key pointing to the table "
												+ sf.pointed_table);
						}
					}
					if (fa.isUnique()) {
						boolean found = false;
						try (Table.ReadQuery rq = new Table.ReadQuery(sql_connection, new Table.SqlQuery(
								"select CONSTRAINT_NAME, CONSTRAINT_TYPE from INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME='"
										+ table.getSqlTableName().toLowerCase() + "';"))) {
							while (rq.result_set.next()) {
								if (rq.result_set.getString("CONSTRAINT_TYPE").equals("UNIQUE")) {
									String constraint_name = rq.result_set.getString("CONSTRAINT_NAME");
									if (constraint_name.equals(table.getSqlTableName().toLowerCase()+"_"+sf.short_field_without_quote.toLowerCase()+"_key"))
									{
										found=true;
										break;
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
	protected boolean supportMultipleAutoPrimaryKeys() {
		return false;
	}

	@Override
	protected boolean supportSingleAutoPrimaryKeys()
	{
		return false;
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


	protected String getBinaryBaseWord()
	{
		return "BYTEA";
	}
	protected String getBlobBaseWord()
	{
		return "BYTEA";
	}

	protected String getVarBinaryType(long limit)
	{
		return "BYTEA";
	}

	protected String getLongVarBinaryType(long limit)
	{
		return "BYTEA";
	}


	@Override
	protected String getLimitSqlPart(long startPosition, long rowLimit)
	{
		StringBuilder limit=new StringBuilder();
		if (rowLimit>=0)
		{
			limit.append(" LIMIT ");
			limit.append(rowLimit);
			if (startPosition>0)
			{
				limit.append(" OFFSET ");
				limit.append(startPosition);
			}
		}
		return limit.toString();
	}

	@Override
	protected String getSqlNULL() {
		return "NULL";
	}

	@Override
	protected String getSqlNotNULL() {
		return "NOT NULL";
	}

	@Override
	protected String getByteType() {
		return "INT2";
	}

	@Override
	protected String getIntType() {
		return "INT4";
	}

	@Override
	protected String getBlobType(long limit) {
		return "BYTEA";
	}

	@Override
	protected String getTextType(long limit)
	{
		return "TEXT";
	}

	@Override
	protected int getMaxKeySize()
	{
		return 3072;
	}

	@Override
	protected String getFloatType() {
		return "DOUBLE PRECISION";
	}

	@Override
	protected String getDoubleType() {
		return "DOUBLE PRECISION";
	}

	@Override
	protected String getShortType() {
		return "INT2";
	}

	@Override
	protected String getLongType() {
		return "INT8";
	}

	@Override
	protected String getBigDecimalType(long limit) {
		return "DECIMAL";
		/*if (limit<=0)
			return "VARCHAR(1024) CHARACTER SET latin1";
		else
			return "VARCHAR("+limit+") CHARACTER SET latin1";*/
	}

	@Override
	protected String getBigIntegerType(long limit) {
		return "DECIMAL";
		/*if (limit<=0)
			return "VARCHAR(1024) CHARACTER SET latin1";
		else
			return "VARCHAR("+limit+") CHARACTER SET latin1";*/
	}
	@Override
	protected String getDateTimeType()
	{
		return "TIMESTAMP";
	}



	@Override
	protected String getDropTableCascadeQuery(Table<?> table)
	{
		return "DROP TABLE IF EXISTS " + table.getSqlTableName()
				+ " CASCADE";
	}


	@Override
	protected String getOnUpdateCascadeSqlQuery() {
		return "ON UPDATE CASCADE";
	}

	@Override
	protected String getOnDeleteCascadeSqlQuery() {
		return "ON DELETE CASCADE";
	}


	@Override
	protected Blob getBlob(byte[] bytes) {
		return null;
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
		return false;
	}

	@Override
	protected boolean supportForeignKeys() {
		return true;
	}

	@Override
	protected boolean supportsItalicQuotesWithTableAndFieldNames()
	{
		return false;
	}
}
