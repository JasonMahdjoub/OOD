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

import java.sql.*;
import java.util.regex.Pattern;

/*import org.hsqldb.jdbc.JDBCBlob;
import org.hsqldb.lib.tar.DbBackupMain;
import org.hsqldb.lib.tar.TarMalformatException;*/

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 2.0
 */

public abstract class CommonHSQLH2DatabaseWrapper extends DatabaseWrapper{



	protected CommonHSQLH2DatabaseWrapper(String databaseName, boolean alwaysDeconectAfterOnTransaction)
			throws DatabaseException {
		super(databaseName, alwaysDeconectAfterOnTransaction);
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

	@Override
	protected String getCachedKeyword() {
		return "CACHED";
	}

	@Override
	protected boolean doesTableExists(String table_name) throws Exception {
		try (Table.ReadQuerry rq = new Table.ReadQuerry(getConnectionAssociatedWithCurrentThread().getConnection(),
				new Table.SqlQuerry("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"
						+ table_name + "'"))) {
			if (rq.result_set.next())
				return true;
		}
		return false;

	}

	@Override
	protected ColumnsReadQuerry getColumnMetaData(String tableName) throws Exception {
		Connection sql_connection = getConnectionAssociatedWithCurrentThread().getConnection();
		return new CReadQuerry(sql_connection, new Table.SqlQuerry(
				"SELECT COLUMN_NAME, TYPE_NAME, COLUMN_SIZE, IS_NULLABLE, IS_AUTOINCREMENT FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"
						+ tableName + "';"));
	}

	@Override
	protected void checkConstraints(Table<?> table) throws DatabaseException {
		Connection sql_connection = getConnectionAssociatedWithCurrentThread().getConnection();
		try (Table.ReadQuerry rq = new Table.ReadQuerry(sql_connection, new Table.SqlQuerry(
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
						try (Table.ReadQuerry rq2 = new Table.ReadQuerry(sql_connection,
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
					/*
					 * System.out.println("SF : "+sf.short_field);
					 * System.out.println("SF : "+table.getName()); try(ReadQuerry rq=new
					 * ReadQuerry(sql_connection,
					 * "SELECT TYPE_NAME, COLUMN_SIZE, IS_NULLABLE, ORDINAL_POSITION, IS_AUTOINCREMENT FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS"
					 * +getSqlComma())) { while (rq.result_set.next()) {
					 * System.out.println("\t"+rq.result_set.getString("TABLE_NAME")); } }
					 */
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


	}
	@Override
	protected String getSqlComma() {
		return ";";
	}

	@Override
	protected int getVarCharLimit() {
		return 16777216;
	}

	@Override
	protected boolean isVarBinarySupported() {
		return true;
	}

	@Override
	protected boolean isLongVarBinarySupported() {
		return false;
	}

	@Override
	protected String getByteType() {
		return "TINYINT";
	}

	@Override
	protected String getIntType() {
		return "INTEGER";
	}

	@Override
	protected String getFloatType() {
		return "DOUBLE";
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
	protected String getBigDecimalType() {
		return "VARCHAR(16374)";
	}

	@Override
	protected String getBigIntegerType() {
		return "VARCHAR(16374)";
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
	protected String getSerializableType() {
		return "BLOB";
	}


	/*@Override
	protected String getSqlQuerryToGetLastGeneratedID() {
		return "CALL IDENTITY()";
	}*/
	@Override
	protected String getOnUpdateCascadeSqlQuerry() {
		return "ON UPDATE CASCADE";
	}

	@Override
	protected String getOnDeleteCascadeSqlQuerry() {
		return "ON DELETE CASCADE";
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
	protected void rollback(Connection openedConnection) throws SQLException {
		try (Statement s = openedConnection.createStatement()) {
			s.executeQuery("ROLLBACK" + getSqlComma());
		}
		/*
		 * try(Statement s=openedConnection.createStatement()) {
		 * s.executeQuery("COMMIT"+getSqlComma()); }
		 */

	}

	@Override
	protected void commit(Connection openedConnection) throws SQLException {
		try (Statement s = openedConnection.createStatement()) {
			s.executeQuery("COMMIT" + getSqlComma());
		}
	}

	@Override
	protected boolean supportSavePoint(Connection openedConnection) {
		return true;
	}

	@Override
	protected void rollback(Connection openedConnection, String _savePointName, Savepoint savepoint)
			throws SQLException {
		try (Statement s = openedConnection.createStatement()) {
			s.executeQuery("ROLLBACK TO SAVEPOINT " + _savePointName + getSqlComma());
		}
		/*
		 * try(Statement s=openedConnection.createStatement()) {
		 * s.executeQuery("COMMIT"+getSqlComma()); }
		 */

	}

	@Override
	protected void disableAutoCommit(Connection openedConnection) throws SQLException {
		try (Statement s = openedConnection.createStatement()) {
			s.executeQuery("SET AUTOCOMMIT FALSE" + getSqlComma());
		}
		try (Statement s = openedConnection.createStatement()) {
			s.executeQuery("COMMIT" + getSqlComma());
		}

	}

	@Override
	protected Savepoint savePoint(Connection _openedConnection, String _savePoint) throws SQLException {
		try (Statement s = _openedConnection.createStatement()) {
			s.executeQuery("SAVEPOINT " + _savePoint + getSqlComma());
		}
		/*
		 * try(Statement s=_openedConnection.createStatement()) {
		 * s.executeQuery("COMMIT"+getSqlComma()); }
		 */

		return null;
	}

	@Override
	protected boolean isThreadSafe() {
		return false;
	}





	@Override
	protected void releasePoint(Connection _openedConnection, String _savePointName, Savepoint savepoint) {
		/*
		 * try(Statement s=_openedConnection.createStatement()) {
		 * s.executeQuery("RELEASE SAVEPOINT "+_savePointName+getSqlComma()); }
		 */
		/*
		 * try(Statement s=_openedConnection.createStatement()) {
		 * s.executeQuery("COMMIT"+getSqlComma()); }
		 */

	}

	@Override
	protected boolean supportFullSqlFieldName() {
		return true;
	}

	@Override
	protected boolean isSerializationException(SQLException e) {
		return e.getSQLState().equals("40001");
	}

}

