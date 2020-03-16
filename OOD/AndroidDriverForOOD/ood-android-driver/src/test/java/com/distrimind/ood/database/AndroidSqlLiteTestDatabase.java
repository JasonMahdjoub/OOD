package com.distrimind.ood.database;


import com.distrimind.ood.database.exceptions.DatabaseException;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

public class AndroidSqlLiteTestDatabase extends TestDatabase {

    private static final AndroidSQLiteDatabaseFactory factoryA=new AndroidSQLiteDatabaseFactory(AndroidSQLiteDatabaseFactory.class.getPackage(), "androidDBTestA", false);
    private static final AndroidSQLiteDatabaseFactory factoryB=new AndroidSQLiteDatabaseFactory(AndroidSQLiteDatabaseFactory.class.getPackage(), "androidDBTestB", false);

    public AndroidSqlLiteTestDatabase() throws DatabaseException, NoSuchAlgorithmException, NoSuchProviderException {
    }

    @Override
    public int getMultiTestsNumber() {
        return 200;
    }

    @Override
    public int getThreadTestsNumber() {
        return 200;
    }

    @Override
    public boolean isMultiConcurrentDatabase() {
        return true;
    }

    @Override
    public File getDatabaseBackupFileName() {
        return null;
    }
    @Override
    public void testBackup() {

    }

    @Override
    public DatabaseWrapper getDatabaseWrapperInstanceA() throws IllegalArgumentException, DatabaseException {
        return factoryA.newWrapperInstance();
    }

    @Override
    public DatabaseWrapper getDatabaseWrapperInstanceB() throws IllegalArgumentException, DatabaseException {
        return factoryB.newWrapperInstance();
    }

    @Override
    public void deleteDatabaseFilesA() throws IllegalArgumentException, DatabaseException {
        AndroidSQLiteDatabaseWrapper.deleteDatabaseFiles(factoryA);
    }

    @Override
    public void deleteDatabaseFilesB() throws IllegalArgumentException, DatabaseException {
        AndroidSQLiteDatabaseWrapper.deleteDatabaseFiles(factoryB);
    }

    @Override
    public boolean isTestEnabled(int testNumber) {
        return true;
    }
}
