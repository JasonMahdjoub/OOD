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
package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseVersionException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 2.0
 */
public class EmbeddedH2DatabaseWrapper extends CommonHSQLH2DatabaseWrapper{

	private static boolean hsql_loaded = false;

	private static Constructor<? extends Blob> H2BlobConstructor=null;
	private static Method H2ValueMethod=null;

	/**
	 * Constructor
	 *
	 * @param _file_name
	 *            The file which contains the database. If this file does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 *
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 * @throws DatabaseException if a problem occurs
	 */
	public EmbeddedH2DatabaseWrapper(File _file_name) throws DatabaseException {
		this(_file_name, false);
	}
	/**
	 * Constructor
	 *
	 * @param _directory_name
	 *            The directory which contains the database. If this directory does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 * @param alwaysDeconectAfterOnTransaction true if the database must always be connected and detected during one transaction
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 * @throws DatabaseException if a problem occurs
	 */
	public EmbeddedH2DatabaseWrapper(File _directory_name, boolean alwaysDeconectAfterOnTransaction) throws DatabaseException {
		super("Database from file : " + getH2DataFileName(getDatabaseFileName(_directory_name)), _directory_name, alwaysDeconectAfterOnTransaction);
	}

	private static File getDatabaseFileName(File directoryName)
	{
		return new File(directoryName, "data.db");
	}


	private static void ensureH2Loading() throws DatabaseLoadingException {
		synchronized (EmbeddedHSQLDBWrapper.class) {
			if (!hsql_loaded) {
				try {
					Class.forName("org.h2.Driver");

					//noinspection SingleStatementInBlock,unchecked
					H2BlobConstructor=(Constructor<? extends Blob>)Class.forName("org.h2.jdbc.JdbcBlob").getDeclaredConstructor(Class.forName("org.h2.jdbc.JdbcConnection"), Class.forName("org.h2.value.Value"), int.class);
					H2ValueMethod=Class.forName("org.h2.value.ValueBytes").getDeclaredMethod("get", byte[].class);
					//DbBackupMain=Class.forName("org.hsqldb.lib.tar.DbBackupMain").getDeclaredMethod("main", (new String[0]).getClass());
					hsql_loaded = true;
				} catch (Exception e) {
					throw new DatabaseLoadingException("Impossible to load H2 ", e);
				}
			}
		}
	}
	private static Connection getConnection(File _file_name)
			throws DatabaseLoadingException {
		if (_file_name == null)
			throw new NullPointerException("The parameter _file_name is a null pointer !");
		if (_file_name.isDirectory())
			throw new IllegalArgumentException("The given file name is a directory !");
		ensureH2Loading();
		try {
			Connection c = DriverManager
					.getConnection("jdbc:h2:file:" + getH2DataFileName(_file_name), "SA", "");
			databaseShutdown.set(false);

			return c;
		} catch (Exception e) {
			throw new DatabaseLoadingException("Impossible to create the database into the file " + _file_name, e);
		}
	}

	private static String getH2DataFileName(File _file_name) {
		if (_file_name.isDirectory())
			throw new IllegalArgumentException();

		String s = _file_name.getAbsolutePath();
		if (s.toLowerCase().endsWith(".data"))
			return s.substring(0, s.length() - 5);
		else
			return s;
	}
	private final static AtomicBoolean databaseShutdown = new AtomicBoolean(false);

	@Override
	public boolean supportNoCacheParam()
	{
		return true;
	}

	@Override
	protected void closeConnection(Connection connection, boolean deepClose) throws SQLException {
		if (!deepClose || databaseShutdown.getAndSet(true)) {
			connection.close();
		} else {
			try (Statement s = connection.createStatement()) {
				s.execute("SHUTDOWN" + getSqlComma());
			} finally {
				connection.close();
			}
		}

	}

	@Override
	protected Connection reopenConnectionImpl() throws DatabaseLoadingException {
		return getConnection(getDatabaseFileName(super.getDatabaseDirectory()));

	}

	@Override
	protected boolean isDuplicateKeyException(SQLException e)
	{
		return e.getErrorCode()==23505;
	}


	@Override
	protected Blob getBlob(final byte[] bytes) throws SQLException {
		try {
			return (Blob)runTransaction(new Transaction() {
				@Override
				public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
					try {
						return H2BlobConstructor.newInstance(getConnectionAssociatedWithCurrentThread().getConnection(), H2ValueMethod.invoke(null, (Object) bytes), -1);
					} catch (Exception e) {
						throw DatabaseException.getDatabaseException(e);
					}
				}

				@Override
				public TransactionIsolation getTransactionIsolation() {
					return TransactionIsolation.TRANSACTION_READ_UNCOMMITTED;
				}

				@Override
				public boolean doesWriteData() {
					return false;
				}

				@Override
				public void initOrReset() {

				}
			}, false);
		}
		catch(DatabaseException e)
		{
			throw new SQLException(e);
		}

	}

	/**
	 * Backup the database into the given directory.
	 *
	 * @param path
	 *            the path where to save the database. If <code>saveAsFiles</code>
	 *            is set to false, it must be a directory, else it must be a file.
	 * @throws DatabaseException
	 *             if a problem occurs
	 */
	public void nativeBackup(File path)
			throws DatabaseException {
		if (path == null)
			throw new NullPointerException("file");

		if (path.exists()) {
			if (!path.isFile())
				throw new IllegalArgumentException("The given path (" + path.getAbsolutePath() + ") must be a file !");
		}
		String f = path.getAbsolutePath();
		if (!f.toLowerCase().endsWith(".zip"))
			f+=".zip";

		final String querry = "BACKUP TO '" + f+"'";
		this.runTransaction(new Transaction() {

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_SERIALIZABLE;
			}

			@Override
			public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
				try {

					lockWrite();
					Connection sql_connection = getConnectionAssociatedWithCurrentThread().getConnection();
					try (PreparedStatement preparedStatement = sql_connection.prepareStatement(querry)) {
						preparedStatement.execute();
					}
					return null;
				} catch (Exception e) {
					throw new DatabaseException("", e);
				} finally {

					unlockWrite();
				}
			}

			@Override
			public boolean doesWriteData() {
				return true;
			}

			@Override
			public void initOrReset() {

			}
		}, true);

	}
	@Override
	protected void checkConstraints(Table<?> table) throws DatabaseException {
		/*try(ResultSet res = getConnectionAssociatedWithCurrentThread().getConnection().getMetaData().getTables(null, null, null, null)) {
			while (res.next()) {
				System.out.println(res.getString("TABLE_NAME"));
			}
			try (Statement s = getConnectionAssociatedWithCurrentThread().getConnection().createStatement()) {
				System.out.println("here");
				ResultSet rs=s.executeQuery("SELECT * FROM "+ROW_PROPERTIES_OF_TABLES);
				if (rs.next())
				{
					for (int i=1;i<=rs.getMetaData().getColumnCount();i++)
						System.out.print(rs.getMetaData().getColumnName(i)+" \t; ");
					System.out.println();
					do {
						for (int i=1;i<=rs.getMetaData().getColumnCount();i++)
							System.out.print(rs.getString(i)+" \t; ");
						System.out.println();
					} while(rs.next());
				}
			}
			//System.exit(0);
		}
		catch(SQLException e)
		{
			throw DatabaseException.getDatabaseException(e);
		}*/
		Connection sql_connection = getConnectionAssociatedWithCurrentThread().getConnection();
		try (Table.ReadQuerry rq = new Table.ReadQuerry(sql_connection, new Table.SqlQuerry(
				"select CONSTRAINT_NAME, CONSTRAINT_TYPE, COLUMN_LIST from "+getConstraintsTableName()+" WHERE TABLE_NAME='"
						+ table.getName() + "';"))) {
			boolean foundPK=false;
			while (rq.result_set.next()) {
				String constraint_name = rq.result_set.getString("CONSTRAINT_NAME");
				String constraint_type = rq.result_set.getString("CONSTRAINT_TYPE");

				switch (constraint_type) {
					case "PRIMARY KEY": {
						if (constraint_name.equals(table.getSqlPrimaryKeyName()))
							foundPK=true;
							/*throw new DatabaseVersionException(table, "There a grouped primary key named " + constraint_name
									+ " which should be named " + table.getSqlPrimaryKeyName());*/
					}
					break;
					case "FOREIGN KEY": {

					}
					break;
					case "UNIQUE": {
						boolean found=false;
						String col=rq.result_set.getString("COLUMN_LIST");
						for (FieldAccessor fa : table.getFieldAccessors()) {
							for (SqlField sf : fa.getDeclaredSqlFields()) {
								if (sf.short_field.toUpperCase().equals(col.toUpperCase()) && fa.isUnique()) {
									found = true;
									break;
								}
							}
							if (found)
								break;
						}
						if (!found)
							throw new DatabaseVersionException(table, "There is a unique sql field " + col
									+ " which does not exists into the OOD database into table "+table.getName());
						/*try (Table.ReadQuerry rq2 = new Table.ReadQuerry(sql_connection,
								new Table.SqlQuerry(
										"select COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"
												+ table.getName() + "' AND CONSTRAINT_NAME='" + constraint_name + "';"))) {
							if (rq2.result_set.next()) {
								String col = (table.getName() + "." + rq2.result_set.getString("COLUMN_NAME"))
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
						}*/

					}
					break;
					case "CHECK":
						break;
					case "REFERENTIAL":
						break;
					default:
						throw new DatabaseVersionException(table, "Unknow constraint " + constraint_type);
				}
			}
			if (!foundPK)
				throw new DatabaseVersionException(table, "Impossible to found PK SQL constraint : " + table.getSqlPrimaryKeyName());

		} catch (SQLException e) {
			throw new DatabaseException("Impossible to check constraints of the table " + table.getName(), e);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
		try (Table.ReadQuerry rq = new Table.ReadQuerry(sql_connection, new Table.SqlQuerry(
				"select PKTABLE_NAME, PKCOLUMN_NAME, FKCOLUMN_NAME from "+getCrossReferencesTableName()+" WHERE FKTABLE_NAME='"
						+ table.getName() + "';"))) {
			while (rq.result_set.next()) {
				String pointed_table = rq.result_set.getString("PKTABLE_NAME");
				String pointed_col = pointed_table + "." + rq.result_set.getString("PKCOLUMN_NAME");
				String fk = table.getName() + "." + rq.result_set.getString("FKCOLUMN_NAME");
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
			throw new DatabaseException("Impossible to check constraints of the table " + table.getName(), e);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
		try {
			Pattern col_size_matcher = Pattern.compile("([0-9]+)");
			for (FieldAccessor fa : table.getFieldAccessors()) {
				for (SqlField sf : fa.getDeclaredSqlFields()) {
					/*
					 * System.out.println("SF : "+sf.short_field);
					 * System.out.println("SF : "+table.getName()); try(ReadQuerry rq=new
					 * ReadQuerry(sql_connection,
					 * "SELECT TYPE_NAME, COLUMN_SIZE, IS_NULLABLE, ORDINAL_POSITION, IS_AUTOINCREMENT FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS"
					 * +getSqlComma())) { while (rq.result_set.next()) {
					 * System.out.println("\t"+rq.result_set.getString("TABLE_NAME")); } }
					 */
					try (Table.ColumnsReadQuerry rq = getColumnMetaData(table.getName(), sf.short_field)) {
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
							if (is_null == sf.not_null)
								throw new DatabaseVersionException(table, "The field " + fa.getFieldName()
										+ " is expected to be " + (fa.isNotNull() ? "not null" : "nullable"));
							boolean is_autoincrement = rq.tableColumnsResultSet.isAutoIncrement();
							if (is_autoincrement != fa.isAutoPrimaryKey())
								throw new DatabaseVersionException(table,
										"The field " + fa.getFieldName() + " is " + (is_autoincrement ? "" : "not ")
												+ "autoincremented into the Sql database where it is "
												+ (is_autoincrement ? "not " : "") + " into the OOD database.");
							sf.sql_position = rq.tableColumnsResultSet.getOrdinalPosition();
						} else
							throw new DatabaseVersionException(table,
									"The field " + fa.getFieldName() + " was not found into the database.");
					}
					if (fa.isForeignKey()) {
						try (Table.ReadQuerry rq = new Table.ReadQuerry(sql_connection, new Table.SqlQuerry(
								"select PKTABLE_NAME, FKTABLE_NAME, PKCOLUMN_NAME, FKCOLUMN_NAME from "+ getCrossReferencesTableName()+" WHERE FKTABLE_NAME='"
										+ table.getName() + "' AND PKTABLE_NAME='" + sf.pointed_table
										+ "' AND PKCOLUMN_NAME='" + sf.short_pointed_field + "' AND FKCOLUMN_NAME='"
										+ sf.short_field + "'"))) {
							if (!rq.result_set.next())
								throw new DatabaseVersionException(table,
										"The field " + fa.getFieldName() + " is a foreign key. One of its Sql fields "
												+ sf.field + " is not a foreign key pointing to the table "
												+ sf.pointed_table);
						}
					}
					if (fa.isUnique()) {
						for(SqlField sf2 : fa.getDeclaredSqlFields())
							try (Table.ReadQuerry rq = new Table.ReadQuerry(sql_connection, new Table.SqlQuerry(
									"select COLUMN_LIST from "+getConstraintsTableName()+" WHERE TABLE_NAME='"
											+ table.getName() + "' AND CONSTRAINT_TYPE='UNIQUE' AND COLUMN_LIST='"+sf2.short_field+"';"))) {
								if (!rq.result_set.next())
									throw new DatabaseVersionException(table, "The OOD field " + fa.getFieldName()
											+ " is a unique key, but it not declared as unique into the Sql database.");
							}


						/*boolean found = false;
						try (ReadQuerry rq = new ReadQuerry(sql_connection, new Table.SqlQuerry(
								"select CONSTRAINT_NAME, CONSTRAINT_TYPE from "+getConstraintsTableName()+" WHERE TABLE_NAME='"
										+ table.getName() + "';"))) {
							while (rq.result_set.next()) {
								if (rq.result_set.getString("CONSTRAINT_TYPE").equals("UNIQUE")) {
									String constraint_name = rq.result_set.getString("CONSTRAINT_NAME");
									try (ReadQuerry rq2 = new ReadQuerry(sql_connection, new Table.SqlQuerry(
											"select COLUMN_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"
													+ table.getName() + "' AND CONSTRAINT_NAME='" + constraint_name
													+ "';"))) {
										if (rq2.result_set.next()) {
											String col = table.getName() + "."
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
									+ " is a unique key, but it not declared as unique into the Sql database.");*/
					}
				}
			}
		} catch (SQLException e) {
			throw new DatabaseException("Impossible to check constraints of the table " + table.getName(), e);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}
	@Override
	protected void startTransaction(Session _openedConnection, TransactionIsolation transactionIsolation, boolean write)
			throws SQLException {
		String isoLevel;
		switch (transactionIsolation) {
			case TRANSACTION_NONE:
			case TRANSACTION_READ_COMMITTED:
				isoLevel = "3";
				break;
			case TRANSACTION_READ_UNCOMMITTED:
				isoLevel = "0";
				break;
			case TRANSACTION_REPEATABLE_READ:
				isoLevel = "1";
				break;
			case TRANSACTION_SERIALIZABLE:
				isoLevel = "1";
				break;
			default:
				throw new IllegalAccessError();

		}

		try (Statement s = _openedConnection.getConnection().createStatement()) {
			s.execute("SET LOCK_MODE " + isoLevel + getSqlComma());
		}

	}

	@Override
	protected boolean doesTableExists(String table_name) throws Exception {

		try(ResultSet res = getConnectionAssociatedWithCurrentThread().getConnection().getMetaData().getTables(null, null, table_name, null)) {
			return res.next();
		}

	}


	@Override
	protected Table.ColumnsReadQuerry getColumnMetaData(String tableName, String columnName) throws Exception {
		Connection sql_connection = getConnectionAssociatedWithCurrentThread().getConnection();
		return new CReadQuerry(sql_connection, new Table.SqlQuerry(
				"SELECT COLUMN_NAME, TYPE_NAME, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"
						+ tableName + (columnName==null?"":"' AND COLUMN_NAME='"+columnName)+ "';"));
	}

	@Override
	public String getConstraintsTableName() {
		return "INFORMATION_SCHEMA.CONSTRAINTS";
	}

	public String getAutoIncrementPart(long startWith)
	{
		return "AUTO_INCREMENT("+startWith+")";
	}


	@Override
	public String getCrossReferencesTableName()
	{
		return "INFORMATION_SCHEMA.CROSS_REFERENCES";
	}

	static class CReadQuerry extends Table.ColumnsReadQuerry {

		public CReadQuerry(Connection _sql_connection, Table.SqlQuerry _querry) throws SQLException, DatabaseException {
			super(_sql_connection, _querry);
			setTableColumnsResultSet(new TCResultSet(this.result_set));
		}


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
			return resultSet.getInt("CHARACTER_MAXIMUM_LENGTH");
		}

		@Override
		public boolean isNullable() throws SQLException {
			return resultSet.getString("IS_NULLABLE").equals("YES");
		}

		@Override
		public boolean isAutoIncrement() throws SQLException {
			String cd=resultSet.getString("COLUMN_DEFAULT");
			if (cd==null)
				return false;
			return cd.contains("SYSTEM_SEQUENCE");
		}

		@Override
		public int getOrdinalPosition() throws SQLException
		{

			return resultSet.getInt("ORDINAL_POSITION");
		}


	}

	@Override
	protected boolean isDisconnetionException(SQLException e) {
		return e.getErrorCode()==90067;
		/*if (e.getErrorCode()==90067)
			return true;
		if (e==null)
			return false;
		Throwable cause=e;
		while ((cause=cause.getCause())!=null)
		{
			if (cause instanceof ClosedByInterruptException)
				return true;
			if (cause instanceof ClosedChannelException)
				return true;

		}
		return false;*/
	}




}
