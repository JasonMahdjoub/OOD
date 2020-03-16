package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;

import java.io.File;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Savepoint;

public class AndroidSQLiteDatabaseWrapper extends DatabaseWrapper {

    private String url;
    private static volatile boolean driverLoaded=false;

    public AndroidSQLiteDatabaseWrapper(String _package, String _database_name, File databaseDirectory) throws DatabaseException {
        super(_database_name, databaseDirectory, false,false);
        url = "jdbc:sqldroid:" + "/data/data/" + _package + "/"+_database_name+".db";
        checkDriverLoading();
    }

    private static void checkDriverLoading()
    {
        if (!driverLoaded) {
            driverLoaded=true;
            try {
                DriverManager.registerDriver((Driver) Class.forName("org.sqldroid.SQLDroidDriver").newInstance());
            } catch (Exception e) {
                throw new RuntimeException("Failed to register SQLDroidDriver");
            }
        }

    }
    @Override
    protected Connection reopenConnectionImpl() throws DatabaseLoadingException {
        try {
            return DriverManager.getConnection(url);
        } catch (SQLException e) {
            throw new DatabaseLoadingException("Impossible to load database", e);
        }
    }

    @Override
    protected String getCachedKeyword() {
        return null;
    }

    @Override
    protected String getNotCachedKeyword() {
        return null;
    }

    @Override
    public boolean supportCache() {
        return false;
    }

    @Override
    public boolean supportNoCacheParam() {
        return false;
    }

    @Override
    protected void closeConnection(Connection c, boolean deepClosing) throws SQLException {

    }

    @Override
    protected void startTransaction(Session _openedConnection, TransactionIsolation transactionIsolation, boolean write) throws SQLException {

    }

    @Override
    protected void rollback(Connection openedConnection) throws SQLException {

    }

    @Override
    protected void commit(Connection openedConnection) throws SQLException, DatabaseException {

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
    protected boolean isTransactionDeadLockException(SQLException e) throws DatabaseException {
        return false;
    }

    @Override
    protected boolean isDisconnectionException(SQLException e) throws DatabaseException {
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
    protected String getBlobType(long limit) {
        return null;
    }

    @Override
    protected String getTextType(long limit) {
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
    protected String getBigDecimalType(long limit) {
        return null;
    }

    @Override
    protected String getBigIntegerType(long limit) {
        return null;
    }

    @Override
    protected String getDateTimeType() {
        return null;
    }

    @Override
    protected String getDropTableCascadeQuery(Table<?> table) {
        return null;
    }

    @Override
    protected boolean supportMultipleAutoPrimaryKeys() {
        return false;
    }

    @Override
    protected String getOnUpdateCascadeSqlQuery() {
        return null;
    }

    @Override
    protected String getOnDeleteCascadeSqlQuery() {
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

    @Override
    protected String getLimitSqlPart(long startPosition, long rowLimit) {
        return null;
    }
}
