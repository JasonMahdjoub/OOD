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

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;


/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 2.0
 */

public abstract class CommonHSQLH2DatabaseWrapper extends DatabaseWrapper{

	protected boolean fileLock;

	protected CommonHSQLH2DatabaseWrapper(String databaseName, File databaseDirectory, boolean alwaysDisconnectAfterOnTransaction, boolean loadToMemory, boolean fileLock)
			throws DatabaseException {
		super(databaseName, databaseDirectory, alwaysDisconnectAfterOnTransaction, loadToMemory);
		this.fileLock=true;

	}
	@Override
	protected boolean supportMultipleAutoPrimaryKeys() {
		return true;
	}

	@Override
	protected boolean supportSingleAutoPrimaryKeys()
	{
		return true;
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
	protected String getNotCachedKeyword() {
		return "";
	}

	@Override
	public boolean supportCache() {
		return true;
	}

	public abstract String getConstraintsTableName();

	public abstract String getCrossReferencesTableName();

	public String getAutoIncrementPart(int startWith)
	{
		return "GENERATED BY DEFAULT AS IDENTITY(START WITH "+startWith+")";
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
	protected String getBigDecimalType(long limit) {
		return "DECIMAL";
		/*if (limit<=0)
			return "NUMERIC";
		else*/

	}


	protected boolean useGetBigDecimalInResultSet()
	{
		return true;
	}

	@Override
	protected String getBigIntegerType(long limit) {
		return "DECIMAL";
		/*if (limit<=0)
			return "VARBINARY(1024)";
		else
			return "VARBINARY("+limit+")";*/
	}

	@Override
	protected String getDateTimeType()
	{
		return "TIMESTAMP";
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
	protected String getBlobType(long limit) {
		return "BLOB("+limit+")";
	}

	@Override
	protected String getTextType(long limit)
	{
		return "CLOB(" + limit + ")";
	}


	/*@Override
	protected String getSqlQuerryToGetLastGeneratedID() {
		return "CALL IDENTITY()";
	}*/
	@Override
	protected String getOnUpdateCascadeSqlQuery() {
		return "ON UPDATE CASCADE";
	}

	@Override
	protected String getOnDeleteCascadeSqlQuery() {
		return "ON DELETE CASCADE";
	}










	@Override
	protected void rollback(Connection openedConnection) throws SQLException {
		try (Statement s = openedConnection.createStatement()) {
			s.execute("ROLLBACK" + getSqlComma());
		}
		/*
		 * try(Statement s=openedConnection.createStatement()) {
		 * s.executeQuery("COMMIT"+getSqlComma()); }
		 */

	}

	@Override
	protected void commit(Connection openedConnection) throws SQLException {
		try (Statement s = openedConnection.createStatement()) {
			s.execute("COMMIT" + getSqlComma());
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
			s.execute("ROLLBACK TO SAVEPOINT " + _savePointName + getSqlComma());
		}
		/*
		 * try(Statement s=openedConnection.createStatement()) {
		 * s.executeQuery("COMMIT"+getSqlComma()); }
		 */

	}

	@Override
	protected void disableAutoCommit(Connection openedConnection) throws SQLException {
		try (Statement s = openedConnection.createStatement()) {
			s.execute("SET AUTOCOMMIT FALSE" + getSqlComma());
		}
		try (Statement s = openedConnection.createStatement()) {
			s.execute("COMMIT" + getSqlComma());
		}

	}

	@Override
	protected Savepoint savePoint(Connection _openedConnection, String _savePoint) throws SQLException {
		try (Statement s = _openedConnection.createStatement()) {
			s.execute("SAVEPOINT " + _savePoint + getSqlComma());
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
}

