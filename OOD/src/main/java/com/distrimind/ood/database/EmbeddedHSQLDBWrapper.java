
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

import com.distrimind.ood.database.Table.ColumnsReadQuerry;
import com.distrimind.ood.database.Table.ReadQuerry;
import com.distrimind.ood.database.Table.SqlQuerry;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseVersionException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/*import org.hsqldb.jdbc.JDBCBlob;
import org.hsqldb.lib.tar.DbBackupMain;
import org.hsqldb.lib.tar.TarMalformatException;*/

/**
 * Sql connection wrapper for HSQLDB
 * 
 * @author Jason Mahdjoub
 * @version 1.5
 * @since OOD 1.0
 */
public class EmbeddedHSQLDBWrapper extends CommonHSQLH2DatabaseWrapper {
	/**
	 * Constructor
	 *
	 * @param _file_name
	 *            The file which contains the database. If this file does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
     * @param alwaysDeconectAfterOnTransaction true if the database must always be connected and detected during one transaction
	 * @param concurrencyControl the concurrency mode
	 * @param _cache_rows
	 *            indicates the maximum number of rows of cached tables that are
	 *            held in memory. The value can range between 100- 4 billion.
	 *            Default value is 100. Table loaded into memory are not concerned.
	 * @param _cache_size
	 *            Indicates the total size (in kilobytes) of rows in the memory
	 *            cache used with cached tables. The value can range between 100 KB
	 *            - 4 GB. The default is 10,000, representing 10,000 kilobytes.
	 * @param _result_max_memory_rows
	 *            This property can be set to specify how many rows of each results
	 *            or temporary table are stored in memory before the table is
	 *            written to disk. The default is zero and means data is always
	 *            stored in memory. If this setting is used, it should be set above
	 *            1000.
	 * @param _cache_free_count
	 *            The default indicates 512 unused spaces are kept for later use.
	 *            The value can range between 0 - 8096. When rows are deleted, the
	 *            space is recovered and kept for reuse for new rows. If too many
	 *            rows are deleted, the smaller recovered spaces are lost and the
	 *            largest ones are retained for later use. Normally there is no need
	 *            to set this property.
	 * @param lockFile true if the database's file must be locked to avoid concurrent access
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 * @throws DatabaseException if a problem occurs
	 */
	public EmbeddedHSQLDBWrapper(File _file_name, boolean alwaysDeconectAfterOnTransaction, HSQLDBConcurrencyControl concurrencyControl, int _cache_rows,
			int _cache_size, int _result_max_memory_rows, int _cache_free_count, boolean lockFile)
			throws IllegalArgumentException, DatabaseException {
		super(/*
				 * getConnection(_file_name, concurrencyControl, _cache_rows, _cache_size,
				 * _result_max_memory_rows, _cache_free_count),
				 */"Database from file : " + getHSQLDBDataFileName(_file_name) + ".data", alwaysDeconectAfterOnTransaction);

		this.file_name = _file_name;
		this.concurrencyControl = concurrencyControl;
		this.cache_rows = _cache_rows;
		this.cache_size = _cache_size;
		this.result_max_memory_rows = _result_max_memory_rows;
		this.cache_free_count = _cache_free_count;
		this.lockFile=lockFile;
	}

	private static Connection getConnection(File _file_name, HSQLDBConcurrencyControl concurrencyControl,
			int _cache_rows, int _cache_size, int _result_max_memory_rows, int _cache_free_count, boolean lockFile)
			throws DatabaseLoadingException {
		if (_file_name == null)
			throw new NullPointerException("The parameter _file_name is a null pointer !");
		if (_file_name.isDirectory())
			throw new IllegalArgumentException("The given file name is a directory !");
		ensureHSQLLoading();
		try {
			Connection c = DriverManager
					.getConnection("jdbc:hsqldb:file:" + getHSQLDBDataFileName(_file_name) + ";hsqldb.cache_rows="
							+ _cache_rows + ";hsqldb.cache_size=" + _cache_size + ";hsqldb.result_max_memory_rows="
							+ _result_max_memory_rows + ";hsqldb.cache_free_count=" + _cache_free_count+";hsqldb.lock_file="+lockFile, "SA", "");
			databaseShutdown.set(false);

			try (Statement s = c.createStatement()) {
				s.executeQuery("SET DATABASE TRANSACTION CONTROL " + concurrencyControl.getCode() + ";");
			}

			try (Statement s = c.createStatement()) {
				s.executeQuery("COMMIT;");
			}

			return c;
		} catch (Exception e) {
			throw new DatabaseLoadingException("Impossible to create the database into the file " + _file_name, e);
		}
	}

	/*
	 * private static Connection getConnection(URL _url) throws
	 * DatabaseLoadingException { if (_url==null) throw new
	 * NullPointerException("The parameter _url is a null pointer !");
	 * ensureHSQLLoading(); try { Connection
	 * c=DriverManager.getConnection("jdbc:hsqldb:"+_url.getPath()+
	 * ";hsqldb.cache_rows=0;shutdown=true", "SA", ""); c.setAutoCommit(false);
	 * return c; } catch(Exception e) { throw new
	 * DatabaseLoadingException("Impossible to create the database into the file "
	 * +_url, e); } }
	 */
	private static String getHSQLDBDataFileName(File _file_name) {
		if (_file_name.isDirectory())
			throw new IllegalArgumentException();
		String s = _file_name.getAbsolutePath();
		if (s.toLowerCase().endsWith(".data"))
			return s.substring(0, s.length() - 5);
		else
			return s;
	}

	@Override
	public String getConstraintsTableName() {
		return "INFORMATION_SCHEMA.TABLE_CONSTRAINTS";
	}

	@Override
	public String getCrossReferencesTableName() {
		return "INFORMATION_SCHEMA.SYSTEM_CROSSREFERENCE";
	}

	/**
	 * Delete all the files associated to the .data database file.
	 *
	 * @param _file_name
	 *            the .data database file, or the base file name (the file without
	 *            the .data extension).
	 */
	@SuppressWarnings("ResultOfMethodCallIgnored")
    public static void deleteDatabaseFiles(File _file_name) {
		String file_base = getHSQLDBDataFileName(_file_name);
		File f = new File(file_base + ".data");
		if (f.exists())
			f.delete();
		f = new File(file_base + ".log");
		if (f.exists())
			f.delete();
		f = new File(file_base + ".properties");
		if (f.exists())
			f.delete();
		f = new File(file_base + ".script");
		if (f.exists())
			f.delete();
		f = new File(file_base + ".tmp");
		if (f.exists())
			f.delete();
		f = new File(file_base + ".lobs");
		if (f.exists())
			f.delete();
	}

	/*
	 * Constructor
	 *
	 * @param _url The url pointing to the server which contains the database.
	 *
	 * @throws NullPointerException if parameters are null pointers.
	 *
	 * @throws IllegalArgumentException If the given file is a directory.
	 *
	 * @throws DatabaseLoadingException If a Sql exception exception occurs.
	 */
	/*
	 * public HSQLDBWrapper(URL _url) throws IllegalArgumentException,
	 * DatabaseException { super(getConnection(_url),
	 * "Database from URL : "+_url.getPath()); }
	 */
	private final static AtomicBoolean databaseShutdown = new AtomicBoolean(false);

	@Override
	protected void closeConnection(Connection connection, boolean deepClose) throws SQLException {
		if (!deepClose || databaseShutdown.getAndSet(true)) {
			connection.close();
		} else {
			try (Statement s = connection.createStatement()) {
				s.executeQuery("SHUTDOWN" + getSqlComma());
			} finally {
				connection.close();
			}
		}

	}



	@Override
	protected boolean doesTableExists(String table_name) throws Exception {
		try (ReadQuerry rq = new ReadQuerry(getConnectionAssociatedWithCurrentThread().getConnection(),
				new Table.SqlQuerry("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"
						+ table_name + "'"))) {
			if (rq.result_set.next())
				return true;
		}
		return false;

	}

	@Override
	protected ColumnsReadQuerry getColumnMetaData(String tableName, String columnName) throws Exception {
		Connection sql_connection = getConnectionAssociatedWithCurrentThread().getConnection();
		return new CReadQuerry(sql_connection, new Table.SqlQuerry(
				"SELECT COLUMN_NAME, TYPE_NAME, COLUMN_SIZE, IS_NULLABLE, IS_AUTOINCREMENT, ORDINAL_POSITION FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"
						+ tableName + (columnName==null?"":"' AND COLUMN_NAME='"+columnName)+"';"));
	}

	public String getAutoIncrementPart(long startWith)
	{
		return "GENERATED BY DEFAULT AS IDENTITY(START WITH "+startWith+")";
	}

	@Override
	protected boolean isDuplicateKeyException(SQLException e)
	{
		return e instanceof SQLIntegrityConstraintViolationException;
	}

	@Override
	protected void checkConstraints(Table<?> table) throws DatabaseException {
		Connection sql_connection = getConnectionAssociatedWithCurrentThread().getConnection();
		try (ReadQuerry rq = new ReadQuerry(sql_connection, new Table.SqlQuerry(
				"select CONSTRAINT_NAME, CONSTRAINT_TYPE from INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME='"
						+ table.getName() + "';"))) {
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
					try (ReadQuerry rq2 = new ReadQuerry(sql_connection,
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
			throw new DatabaseException("Impossible to check constraints of the table " + table.getName(), e);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
		try (ReadQuerry rq = new ReadQuerry(sql_connection, new Table.SqlQuerry(
				"select PKTABLE_NAME, PKCOLUMN_NAME, FKCOLUMN_NAME from INFORMATION_SCHEMA.SYSTEM_CROSSREFERENCE WHERE FKTABLE_NAME='"
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

					try (ReadQuerry rq = new ReadQuerry(sql_connection, new Table.SqlQuerry(
							"SELECT TYPE_NAME, COLUMN_SIZE, IS_NULLABLE, ORDINAL_POSITION, IS_AUTOINCREMENT FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"
									+ table.getName() + "' AND COLUMN_NAME='" + sf.short_field + "'"
									+ getSqlComma()))) {
						if (rq.result_set.next()) {
							String type = rq.result_set.getString("TYPE_NAME").toUpperCase();
							if (!sf.type.toUpperCase().startsWith(type))
								throw new DatabaseVersionException(table, "The type of the field " + sf.field
										+ " should  be " + sf.type + " and not " + type);
							if (col_size_matcher.matcher(sf.type).matches()) {
								int col_size = rq.result_set.getInt("COLUMN_SIZE");
								Pattern pattern2 = Pattern.compile("(" + col_size + ")");
								if (!pattern2.matcher(sf.type).matches())
									throw new DatabaseVersionException(table, "The column " + sf.field
											+ " has a size equals to " + col_size + " (expected " + sf.type + ")");
							}
							boolean is_null = rq.result_set.getString("IS_NULLABLE").equals("YES");
							if (is_null == sf.not_null)
								throw new DatabaseVersionException(table, "The field " + fa.getFieldName()
										+ " is expected to be " + (fa.isNotNull() ? "not null" : "nullable"));
							boolean is_autoincrement = rq.result_set.getString("IS_AUTOINCREMENT").equals("YES");
							if (is_autoincrement != fa.isAutoPrimaryKey())
								throw new DatabaseVersionException(table,
										"The field " + fa.getFieldName() + " is " + (is_autoincrement ? "" : "not ")
												+ "autoincremented into the Sql database where it is "
												+ (is_autoincrement ? "not " : "") + " into the OOD database.");
							sf.sql_position = rq.result_set.getInt("ORDINAL_POSITION");
						} else
							throw new DatabaseVersionException(table,
									"The field " + fa.getFieldName() + " was not found into the database.");
					}
					if (fa.isPrimaryKey()) {
						try (ReadQuerry rq = new ReadQuerry(sql_connection,
								new Table.SqlQuerry(
										"select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"
												+ table.getName() + "' AND COLUMN_NAME='" + sf.short_field
												+ "' AND CONSTRAINT_NAME='" + table.getSqlPrimaryKeyName() + "';"))) {
							if (!rq.result_set.next())
								throw new DatabaseVersionException(table, "The field " + fa.getFieldName()
										+ " is not declared as a primary key into the Sql database.");
						}
					}
					if (fa.isForeignKey()) {
						try (ReadQuerry rq = new ReadQuerry(sql_connection, new Table.SqlQuerry(
								"select PKTABLE_NAME, FKTABLE_NAME, PKCOLUMN_NAME, FKCOLUMN_NAME from INFORMATION_SCHEMA.SYSTEM_CROSSREFERENCE WHERE FKTABLE_NAME='"
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
						boolean found = false;
						try (ReadQuerry rq = new ReadQuerry(sql_connection, new Table.SqlQuerry(
								"select CONSTRAINT_NAME, CONSTRAINT_TYPE from INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME='"
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
									+ " is a unique key, but it not declared as unique into the Sql database.");
					}
				}
			}
		} catch (SQLException e) {
			throw new DatabaseException("Impossible to check constraints of the table " + table.getName(), e);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	static class CReadQuerry extends ColumnsReadQuerry {

		public CReadQuerry(Connection _sql_connection, SqlQuerry _querry) throws SQLException, DatabaseException {
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


	/**
	 * Closes the database files, rewrites the script file, deletes the log file and
	 * opens the database.
	 *
	 * @param _defrag
	 *            If true is specified, this command also shrinks the .data file to
	 *            its minimal size.
	 * @throws DatabaseException
	 *             if a SQL exception occurs.
	 */
	public void checkPoint(final boolean _defrag) throws DatabaseException {

		runTransaction(new Transaction() {

			@SuppressWarnings("ThrowFromFinallyBlock")
            @Override
			public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
				try  {
					lockWrite();
					Statement st = null;
					try {
						st = getConnectionAssociatedWithCurrentThread().getConnection().createStatement();
						st.execute("CHECKPOINT" + (_defrag ? " DEFRAG" : ""));
					} catch (SQLException e) {
						throw DatabaseException.getDatabaseException(e);
					} finally {
						try {
                            assert st != null;
                            st.close();
						} catch (SQLException e) {
							throw DatabaseException.getDatabaseException(e);
						}

					}
				}
				finally
				{
					unlockWrite();
				}
				return null;
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_SERIALIZABLE;
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
	protected Blob getBlob(byte[] _bytes) throws SQLException {
		try {
			return JDBCBlobConstructor.newInstance((Object) _bytes);
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

	/**
	 * Backup the database into the given directory. During this backup, the
	 * database will not be available.
	 *
	 * @param file
	 *            the file name where to save the database.
	 * @throws DatabaseException
	 *             if a problem occurs
	 */
	@Override
	public void backup(File file) throws DatabaseException {
		this.backup(file, true, false, false);
	}

	/**
	 * Backup the database into the given directory.
	 *
	 * @param file
	 *            the file name where to save the database.
	 * @param blockDatabase
	 *            if set to true, database can't be used during the backup. Hot
	 *            backup is performed if NOT BLOCKING is specified. In this mode,
	 *            the database can be used during backup. This mode should only be
	 *            used with very large databases. A hot backup set is less compact
	 *            and takes longer to restore and use than a normal backup set
	 *            produced with the BLOCKING option. You can perform a CHECKPOINT
	 *            just before a hot backup in order to reduce the size of the backup
	 *            set.
	 * @throws DatabaseException
	 *             if a problem occurs
	 */
	public void backup(File file, final boolean blockDatabase) throws DatabaseException {
		this.backup(file, blockDatabase, false, !blockDatabase);
	}

	/**
	 * Backup the database into the given directory.
	 *
	 * @param path
	 *            the path where to save the database. If <code>saveAsFiles</code>
	 *            is set to false, it must be a directory, else it must be a file.
	 * @param blockDatabase
	 *            if set to true, database can't be used during the backup. Hot
	 *            backup is performed if NOT BLOCKING is specified. In this mode,
	 *            the database can be used during backup. This mode should only be
	 *            used with very large databases. A hot backup set is less compact
	 *            and takes longer to restore and use than a normal backup set
	 *            produced with the BLOCKING option. You can perform a CHECKPOINT
	 *            just before a hot backup in order to reduce the size of the backup
	 *            set.
	 * @param saveAsFiles
	 *            if set to true, the database files are copied to a directory
	 *            specified by <code>path</code> without any compression. The file
	 *            path must be a directory. If the directory does not exist, it is
	 *            created. The file path may be absolute or relative. If it is
	 *            relative, it is interpreted as relative to the location of
	 *            database files. When set to true, the database will be compressed.
	 * @param performCheckPoint
	 *            if set to true, calls the function {@link #checkPoint(boolean)}
	 * @throws DatabaseException
	 *             if a problem occurs
	 */
	public void backup(File path, final boolean blockDatabase, boolean saveAsFiles, boolean performCheckPoint)
			throws DatabaseException {
		if (path == null)
			throw new NullPointerException("file");
		if (performCheckPoint)
			checkPoint(false);

		if (path.exists()) {
			if (saveAsFiles && !path.isFile())
				throw new IllegalArgumentException("The given path (" + path.getAbsolutePath() + ") must be a file !");
			if (!saveAsFiles && !path.isDirectory())
				throw new IllegalArgumentException(
						"The given path (" + path.getAbsolutePath() + ") must be a directory !");
		}
		String f = path.getAbsolutePath();

		if (!saveAsFiles) {
			f = f + File.separator;
		}
		final String querry = "BACKUP DATABASE TO '" + f + (blockDatabase ? "' BLOCKING" : "' NOT BLOCKING")
				+ (saveAsFiles ? " AS FILES" : "");
		this.runTransaction(new Transaction() {

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_NONE;
			}

			@Override
			public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
				try {
					if (blockDatabase)
						lockWrite();
					else
						lockRead();
					Connection sql_connection = getConnectionAssociatedWithCurrentThread().getConnection();
                    try (PreparedStatement preparedStatement = sql_connection.prepareStatement(querry)) {
                        preparedStatement.execute();
                    }
					return null;
				} catch (Exception e) {
					throw new DatabaseException("", e);
				} finally {
					if (blockDatabase)
						unlockWrite();
					else
						unlockRead();
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

	/**
	 * Restore a database from a source to a directory
	 *
	 * @param sourcePath
	 *            the database source. It can be a directory containing several
	 *            files, or a simple tar file.
	 * @param databaseDirectory
	 *            the restored database directory
	 * @throws IOException
	 *             if a problem occurs
	 *
	 */
	public static void restore(File sourcePath, File databaseDirectory) throws IOException {
		if (sourcePath == null)
			throw new NullPointerException("sourcePath");
		if (databaseDirectory == null)
			throw new NullPointerException("databaseDirectory");
		if (databaseDirectory.exists() && !databaseDirectory.isDirectory())
			throw new IllegalArgumentException("databaseDirectory must be a directory !");
		String[] args = {"--extract", sourcePath.getAbsolutePath(), databaseDirectory.getAbsolutePath()};
		try {
			DbBackupMain.invoke(null, (Object)args);
		} catch (IllegalAccessException | IllegalArgumentException e) {
			throw new IOException(e);
		}
		catch( InvocationTargetException e)
		{
			Throwable t=e.getCause();
			if (t instanceof IOException)
				throw (IOException)t;
			else
				throw new IOException(t);
		}
	}

	@Override
	protected Connection reopenConnectionImpl() throws DatabaseLoadingException {
		return getConnection(file_name, concurrencyControl, cache_rows, cache_size, result_max_memory_rows,
				cache_free_count, lockFile);
	}


	@Override
	protected void startTransaction(Session _openedConnection, TransactionIsolation transactionIsolation, boolean write)
			throws SQLException {
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
		/*
		 * try(Statement s=_openedConnection.createStatement()) {
		 * s.executeQuery("COMMIT"+getSqlComma()); }
		 */

	}

	private static boolean hsql_loaded = false;
	private static Constructor<? extends Blob> JDBCBlobConstructor=null;
	private static Method DbBackupMain;
	private final File file_name;
	private final HSQLDBConcurrencyControl concurrencyControl;
	private final int cache_rows;
	private final int cache_size;
	private final int result_max_memory_rows;
	private final int cache_free_count;
	private final boolean lockFile;

	@SuppressWarnings("unchecked")
	private static void ensureHSQLLoading() throws DatabaseLoadingException {
		synchronized (EmbeddedHSQLDBWrapper.class) {
			if (!hsql_loaded) {
				try {
					Class.forName("org.hsqldb.jdbc.JDBCDriver");
					JDBCBlobConstructor=(Constructor<? extends Blob>)Class.forName("org.hsqldb.jdbc.JDBCBlob").getDeclaredConstructor(byte[].class);
					DbBackupMain=Class.forName("org.hsqldb.lib.tar.DbBackupMain").getDeclaredMethod("main", String[].class);
					hsql_loaded = true;
				} catch (Exception e) {
					throw new DatabaseLoadingException("Impossible to load HSQLDB ", e);
				}
			}
		}
	}

	/**
	 * Constructor
	 *
	 * @param _file_name
	 *            The file which contains the database. If this file does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 */
	public EmbeddedHSQLDBWrapper(File _file_name) throws IllegalArgumentException, DatabaseException {
		this(_file_name, false);
	}

	/**
	 * Constructor
	 *
	 * @param _file_name
	 *            The file which contains the database. If this file does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 * @param alwaysDeconectAfterOnTransaction true if the database must always be connected and detected during one transaction
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 */
	public EmbeddedHSQLDBWrapper(File _file_name, boolean alwaysDeconectAfterOnTransaction) throws IllegalArgumentException, DatabaseException {
		this(_file_name, alwaysDeconectAfterOnTransaction, HSQLDBConcurrencyControl.DEFAULT, 100, 10000, 0, 512, true);
	}

	@Override
	protected boolean isDisconnetionException(SQLException e) {
		return e.getErrorCode()==402 || e.getErrorCode()==1002;
	}
}
