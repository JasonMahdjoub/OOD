package com.distrimind.ood.database;


import android.content.Context;

import com.distrimind.ood.database.database.Table1;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.harddrive.AndroidHardDriveDetect;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.testng.annotations.DataProvider;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.sql.SQLException;
import java.util.Map;

import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.platform.app.InstrumentationRegistry;


@SuppressWarnings("ConstantConditions")
@RunWith(AndroidJUnit4.class)
public class AndroidSqlLiteTestDatabase extends TestDatabase {


    private static final AndroidSQLiteDatabaseFactory factoryA=new AndroidSQLiteDatabaseFactory(AndroidSQLiteDatabaseFactory.class.getPackage(), "androidDBTestA", false);
    private static final AndroidSQLiteDatabaseFactory factoryB=new AndroidSQLiteDatabaseFactory(AndroidSQLiteDatabaseFactory.class.getPackage(), "androidDBTestB", false);
    static {
        AndroidHardDriveDetect.context = InstrumentationRegistry.getInstrumentation().getTargetContext();
    }

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
    public void deleteDatabaseFilesA() throws IllegalArgumentException {
        AndroidSQLiteDatabaseWrapper.deleteDatabaseFiles(factoryA);
    }

    @Override
    public void deleteDatabaseFilesB() throws IllegalArgumentException {
        AndroidSQLiteDatabaseWrapper.deleteDatabaseFiles(factoryB);
    }

    @Override
    public boolean isTestEnabled(int testNumber) {
        return true;
    }


    @SuppressWarnings("unchecked")
    @Test
    public void allTests() throws DatabaseException, NoSuchProviderException, NoSuchAlgorithmException, IOException, SQLException {
        checkUnloadedDatabase();
        firstLoad();
        isLoadedIntoMemory();
        firstAdd();
        firstTestSize();
        firstReload();
        secondTestSize();
        testFirstAdd();
        testCursor();
        addSecondRecord();
        alterRecord();
        getRecord();
        getRecordFilter();
        removeRecord();
        testArrayRecordParameters();
        removeRecords();
        testFilters();
        testRemoveFilters();
        addForeignKeyAndTestUniqueKeys();
        alterRecordWithCascade();
        removePointedRecords();
        removeForeignKeyRecords();
        testIsPointed();
        removeWithCascade();
        setAutoRandomFields();
        for (Object[] o : interpreterCommandsProvider())
        {
            testCommandTranslatorInterpreter((Class<? extends Table<?>>)o[0], (String)o[1], (Map<String, Object>)o[2], (String)o[3], (Map<Integer, Object>)o[4], (Table1.Record)o[5], (boolean)o[6]);
        }
        prepareMultipleTest();
        multipleTests();
        testThreadSafe();
        testCheckPoint();
        testBackup();
        testDatabaseRemove();

    }


    @Override
    public void checkUnloadedDatabase() throws IllegalArgumentException, DatabaseException {
        super.checkUnloadedDatabase();
    }

    @Override
    public void firstLoad() throws IllegalArgumentException, DatabaseException {
        super.firstLoad();
    }

    @Override
    public void isLoadedIntoMemory() throws DatabaseException {
        super.isLoadedIntoMemory();
    }

    @Override
    public void firstAdd() throws DatabaseException {
        super.firstAdd();
    }

    @Override
    public void firstTestSize() throws DatabaseException {
        super.firstTestSize();
    }

    @Override
    public void firstReload() throws DatabaseException {
        super.firstReload();
    }

    @Override
    public void secondTestSize() throws DatabaseException {
        super.secondTestSize();
    }

    @Override
    public void testFirstAdd() throws DatabaseException {
        super.testFirstAdd();
    }

    @Override
    public void testCursor() throws DatabaseException {
        super.testCursor();
    }

    @Override
    public void addSecondRecord() throws DatabaseException {
        super.addSecondRecord();
    }

    @Override
    public void alterRecord() throws DatabaseException {
        super.alterRecord();
    }

    @Override
    public void getRecord() throws DatabaseException {
        super.getRecord();
    }

    @Override
    public void getRecordFilter() throws DatabaseException {
        super.getRecordFilter();
    }

    @Override
    public void removeRecord() throws DatabaseException {
        super.removeRecord();
    }

    @Override
    public void testArrayRecordParameters() throws DatabaseException {
        super.testArrayRecordParameters();
    }

    @Override
    public void removeRecords() throws DatabaseException {
        super.removeRecords();
    }

    @Override
    public void testFilters() throws DatabaseException {
        super.testFilters();
    }

    @Override
    public void testRemoveFilters() throws DatabaseException {
        super.testRemoveFilters();
    }

    @Override
    public void addForeignKeyAndTestUniqueKeys() throws DatabaseException {
        super.addForeignKeyAndTestUniqueKeys();
    }

    @Override
    public void alterRecordWithCascade() throws DatabaseException, NoSuchAlgorithmException, NoSuchProviderException {
        super.alterRecordWithCascade();
    }

    @Override
    public void removePointedRecords() throws DatabaseException {
        super.removePointedRecords();
    }

    @Override
    public void removeForeignKeyRecords() throws DatabaseException {
        super.removeForeignKeyRecords();
    }

    @Override
    public void testIsPointed() throws DatabaseException {
        super.testIsPointed();
    }

    @Override
    public void removeWithCascade() throws DatabaseException {
        super.removeWithCascade();
    }

    @Override
    public void setAutoRandomFields() throws DatabaseException {
        super.setAutoRandomFields();
    }

    @DataProvider(name = "interpreterCommandsProvider")
    @Override
    public Object[][] interpreterCommandsProvider() throws IOException, NoSuchAlgorithmException, NoSuchProviderException, DatabaseException {
        return super.interpreterCommandsProvider();
    }

    @Override
    public <T extends DatabaseRecord> void testIsConcernedInterpreterFunction(Table<T> table, T record, String command, Map<String, Object> parameters, boolean expectedBoolean) throws DatabaseException {
        super.testIsConcernedInterpreterFunction(table, record, command, parameters, expectedBoolean);
    }

    @Override
    public void testCommandTranslatorInterpreter(Class<? extends Table<?>> tableClass, String command, Map<String, Object> parameters, String expectedSqlCommand, Map<Integer, Object> expectedSqlParameters, Table1.Record record, boolean expectedTestResult) throws DatabaseException, IOException, SQLException {
        super.testCommandTranslatorInterpreter(tableClass, command, parameters, expectedSqlCommand, expectedSqlParameters, record, expectedTestResult);
    }

    @Override
    public void prepareMultipleTest() throws DatabaseException {
        super.prepareMultipleTest();
    }

    @Override
    public void multipleTests() throws DatabaseException {
        super.multipleTests();
    }

    @Override
    public void subMultipleTests() throws DatabaseException {
        super.subMultipleTests();
    }

    @Override
    public void testThreadSafe() {
        super.testThreadSafe();
    }

    @Override
    public void testCheckPoint() throws DatabaseException {
        super.testCheckPoint();
    }

    @Override
    public void testDatabaseRemove() throws DatabaseException {
        super.testDatabaseRemove();
    }
}
