
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

import com.distrimind.ood.database.database.*;
import com.distrimind.ood.database.database.Table1.Record;
import com.distrimind.ood.database.exceptions.ConcurentTransactionDatabaseException;
import com.distrimind.ood.database.exceptions.ConstraintsNotRespectedDatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.util.DecentralizedIDGenerator;
import com.distrimind.util.FileTools;
import com.distrimind.util.RenforcedDecentralizedIDGenerator;
import com.distrimind.util.SecuredDecentralizedID;
import com.distrimind.util.crypto.SecureRandomType;
import com.distrimind.util.crypto.SymmetricEncryptionType;
import com.distrimind.util.crypto.SymmetricSecretKey;
import gnu.vm.jgnu.security.NoSuchAlgorithmException;
import gnu.vm.jgnu.security.NoSuchProviderException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static org.testng.Assert.*;
import static org.testng.Assert.assertTrue;

/**
 * 
 * @author Jason Mahdjoub
 * @version 2.0
 * @since OOD 1.0
 */

public class DerbyTestDatabase extends TestDatabase {
	public DerbyTestDatabase() throws DatabaseException, NoSuchAlgorithmException, NoSuchProviderException {
		super();
	}

	private static File data_directory = new File("databasetestDerby");
	private static File data_directoryb = new File("databasetestDerbyb");
	private static File database_backup_directory = new File("databasebackupDerby");

	@Override
	public DatabaseWrapper getDatabaseWrapperInstanceA() throws IllegalArgumentException, DatabaseException {
		return new EmbeddedDerbyWrapper(data_directory);
	}

	@Override
	public DatabaseWrapper getDatabaseWrapperInstanceB() throws IllegalArgumentException, DatabaseException {
		return new EmbeddedDerbyWrapper(data_directoryb);
	}

	@Override
	public void deleteDatabaseFilesA() throws IllegalArgumentException {
		EmbeddedDerbyWrapper.deleteDatabaseFiles(data_directory);

	}

	@Override
	public void deleteDatabaseFilesB() throws IllegalArgumentException {
		EmbeddedDerbyWrapper.deleteDatabaseFiles(data_directoryb);
	}

	@AfterClass
	public static void unloadDatabase()  {
		TestDatabase.unloadDatabase();
		EmbeddedDerbyWrapper.deleteDatabaseFiles(data_directory);
		EmbeddedDerbyWrapper.deleteDatabaseFiles(data_directoryb);
		FileTools.deleteDirectory(database_backup_directory);
	}

	@Override
	public File getDatabaseBackupFileName() {
		return database_backup_directory;
	}

	@Test
	public void checkUnloadedDatabase() throws IllegalArgumentException, DatabaseException {
		super.checkUnloadedDatabase();
	}

	@Test(dependsOnMethods = { "checkUnloadedDatabase" })
	public void firstLoad() throws IllegalArgumentException, DatabaseException {

		super.firstLoad();
	}

	@Test(dependsOnMethods = { "firstLoad" })
	public void isLoadedIntoMemory() {
		super.isLoadedIntoMemory();
	}


	@Test(dependsOnMethods = { "firstLoad" })
	public void firstAdd() throws DatabaseException {
		super.firstAdd();

	}

	@Test(dependsOnMethods = { "firstAdd" })
	public void firstTestSize() throws DatabaseException {
		super.firstTestSize();
	}

	@Test(dependsOnMethods = { "firstTestSize" })
	public void firstReload() throws DatabaseException {
		super.firstReload();

	}

	@Test(dependsOnMethods = { "firstReload" })
	public void secondTestSize() throws DatabaseException {
		super.secondTestSize();

	}

	@Test(dependsOnMethods = { "secondTestSize" })
	public void testFirstAdd() throws DatabaseException {
		super.testFirstAdd();
	}

	@Test(dependsOnMethods = { "alterRecord" })
	public void addSecondRecord() throws DatabaseException {
		super.addSecondRecord();
	}

	@Test(dependsOnMethods = { "testFirstAdd" })
	public void alterRecord() throws DatabaseException {
		super.alterRecord();

	}

	@Test(dependsOnMethods = { "addSecondRecord" })
	public void getRecord() throws DatabaseException {
		super.getRecord();

	}



	@Test(dependsOnMethods = { "addSecondRecord" })
	public void getRecordFilter() throws DatabaseException {
		super.getRecordFilter();

	}

	@Test(dependsOnMethods = { "getRecordFilter", "getRecord", "alterRecord" })
	public void removeRecord() throws DatabaseException {
		super.removeRecord();
	}

	@Test(dependsOnMethods = { "removeRecord" })
	public void testArrayRecordParameters() throws DatabaseException {
		super.testArrayRecordParameters();
	}

	@Test(dependsOnMethods = { "testArrayRecordParameters" })
	public void removeRecords() throws DatabaseException {
		super.removeRecords();

	}

	@Test(dependsOnMethods = { "removeRecords" })
	public void testFilters() throws DatabaseException {
		super.testFilters();

	}

	@Test(dependsOnMethods = { "testFilters" })
	public void testRemoveFilters() throws DatabaseException {
		super.testRemoveFilters();

	}

	@Test(dependsOnMethods = { "testRemoveFilters" })
	public void addForeignKeyAndTestUniqueKeys() throws DatabaseException {
		super.addForeignKeyAndTestUniqueKeys();

	}




	@Override
	@Test(dependsOnMethods = { "addForeignKeyAndTestUniqueKeys" }, enabled = false)
	public void alterRecordWithCascade() {

	}

	@Override
	@Test(dependsOnMethods = { "addForeignKeyAndTestUniqueKeys" })
	public void removePointedRecords() throws DatabaseException {
		Assert.assertEquals(table1.getRecordsNumber(), 2);
		Assert.assertEquals(table3.getRecordsNumber(), 2);
		Assert.assertEquals(table2.getRecordsNumber(), 1);
		Assert.assertEquals(table4.getRecordsNumber(), 1);
		Assert.assertEquals(table5.getRecordsNumber(), 1);
		Assert.assertEquals(table1.getRecords().size(), 2);
		Assert.assertEquals(table2.getRecords().size(), 1);
		Assert.assertEquals(table3.getRecords().size(), 2);
		Assert.assertEquals(table4.getRecords().size(), 1);
		Assert.assertEquals(table5.getRecords().size(), 1);

		HashMap<String, Object> map = new HashMap<>();
		map.put("fr1_pk1", table1.getRecords().get(1));
		map.put("int_value", 1);
		table2.addRecord(map);
		/*
		 * map=new HashMap<>(); map.put("fr1_pk1", table3.getRecords().get(1));
		 * map.put("int_value", new Integer(1)); table4.addRecord(map);
		 */

		table1.removeRecords(new Filter<Table1.Record>() {

			@Override
			public boolean nextRecord(Record _record) {
				return true;
			}
		});

		table3.removeRecords(new Filter<Table3.Record>() {

			@Override
			public boolean nextRecord(Table3.Record _record) {
				return true;
			}
		});

		Assert.assertEquals(table1.getRecordsNumber(), 2);
		Assert.assertEquals(table3.getRecordsNumber(), 1);
		Assert.assertEquals(table2.getRecordsNumber(), 2);
		Assert.assertEquals(table4.getRecordsNumber(), 1);
		Assert.assertEquals(table5.getRecordsNumber(), 1);
		Assert.assertEquals(table1.getRecords().size(), 2);
		Assert.assertEquals(table2.getRecords().size(), 2);
		Assert.assertEquals(table3.getRecords().size(), 1);
		Assert.assertEquals(table4.getRecords().size(), 1);
		Assert.assertEquals(table5.getRecords().size(), 1);

		try {
			table1.removeRecords(table1.getRecords());
			Assert.fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			Assert.assertTrue(true);
		}
		try {
			table3.removeRecords(table3.getRecords());
			Assert.fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			Assert.assertTrue(true);
		}
		Assert.assertEquals(2, table1.getRecordsNumber());
		Assert.assertEquals(1, table3.getRecordsNumber());
		Assert.assertEquals(2, table1.getRecords().size());
		Assert.assertEquals(1, table3.getRecords().size());
		try {
			table1.removeRecord(table1.getRecords().get(0));
			Assert.fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			Assert.assertTrue(true);
		}
		try {
			table3.removeRecord(table3.getRecords().get(0));
			Assert.fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			Assert.assertTrue(true);
		}
		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	@Override
	@Test(dependsOnMethods = { "removePointedRecords" })
	public void removeForeignKeyRecords() throws DatabaseException {
		super.removeForeignKeyRecords();
	}

	@Override
	@Test(dependsOnMethods = { "removeForeignKeyRecords" })
	public void testIsPointed() throws DatabaseException {
		super.testIsPointed();
	}

	@Override
	@Test(dependsOnMethods = { "testIsPointed" })
	public void removeWithCascade() throws DatabaseException {
		super.removeWithCascade();
	}

	@Override
	@Test(dependsOnMethods = { "removeWithCascade" })
	public void setAutoRandomFields() throws DatabaseException {
		super.setAutoRandomFields();
	}

	@Override
	@Test(dependsOnMethods = { "setAutoRandomFields" })
	public void prepareMultipleTest() throws DatabaseException {
		super.prepareMultipleTest();
	}

	@Override
	@Test(dependsOnMethods = { "prepareMultipleTest" })
	public void multipleTests() throws DatabaseException {
		super.multipleTests();
	}

	@Override
	@Test(invocationCount = 0)
	public void subMultipleTests() throws DatabaseException {
		super.subMultipleTests();
	}

	@Override
	@Test(threadPoolSize = 4, invocationCount = 4, timeOut = 2000000, dependsOnMethods = { "multipleTests" })
	public void testThreadSafe() {
		super.testThreadSafe();
	}

	@Override
	@Test(threadPoolSize = 1, dependsOnMethods = { "testThreadSafe" })
	public void testCheckPoint() {

	}

	@Override
	@Test(threadPoolSize = 1, dependsOnMethods = { "testCheckPoint" })
	public void testBackup() throws DatabaseException {
		super.testBackup();
	}

	@Override
	@Test(threadPoolSize = 1, dependsOnMethods = { "testBackup" })
	public void testDatabaseRemove() throws DatabaseException {
		super.testDatabaseRemove();
	}

	@Override
	public boolean isTestEnabled(int _testNumber) {
		return _testNumber != 12 && _testNumber != 13;
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
		return false;
	}

}
