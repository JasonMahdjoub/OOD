package com.distrimind.ood.database;
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

import com.distrimind.ood.database.database.*;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.FileTools;
import com.distrimind.util.crypto.SecureRandomType;
import com.distrimind.util.crypto.SymmetricEncryptionType;
import com.distrimind.util.crypto.SymmetricSecretKey;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0.0
 */
public class TestDatabaseBackupRestore {


	private DatabaseWrapper wrapperForReferenceDatabase;
	private final File referenceDatabaseDirectory=new File("./referenceDatabaseToTest");
	private final File databaseDirectory=new File("./backupDatabaseToTest");
	private final File externalBackupDirectory=new File("./externalBackupDatabaseToTest");
	private Table1.Record recordWithoutForeignKeyA, recordPointedByOtherA, recordWithoutForeignKeyAToRemove, recordPointedByOtherAToRemove;
	private Table2.Record recordPointingToOtherA, recordPointingToOtherAToRemove;
	private Table3.Record recordWithoutForeignKeyB, recordPointedByOtherB, recordWithoutForeignKeyBToRemove, recordPointedByOtherBToRemove;
	private Table4.Record recordPointingToOtherB, recordPointingToOtherBToRemove;
	private final Date date = Calendar.getInstance().getTime();
	private final Calendar calendar = Calendar.getInstance();
	private final SymmetricEncryptionType typeSecretKey;
	private final SymmetricSecretKey secretKey;
	private final SubField subField;
	private final SubSubField subSubField;
	private final File fileTest;
	private DatabaseWrapper wrapper;

	public TestDatabaseBackupRestore() throws DatabaseException, NoSuchProviderException, NoSuchAlgorithmException {
		typeSecretKey = SymmetricEncryptionType.AES_CBC_PKCS5Padding;
		secretKey = typeSecretKey.getKeyGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKey();
		subField = TestDatabase.getSubField();
		subSubField = TestDatabase.getSubSubField();
		fileTest=new File("fileTest");
	}

	@Test
	public void testBackupConfParams()
	{
		new BackupConfiguration(5000L, 10000L, 1000000, 1000L, null);
		try {
			new BackupConfiguration(5000L, 1000L, 1000000, 1000L, null);
			Assert.fail();
		}
		catch(IllegalArgumentException ignored)
		{

		}

	}


	private void loadData(DatabaseWrapper sourceWrapper, DatabaseWrapper destinationWrapper) throws DatabaseException {
		List<Table1.Record> lTable1=sourceWrapper.getTableInstance(Table1.class).getRecords();
		List<Table2.Record> lTable2=sourceWrapper.getTableInstance(Table2.class).getRecords();
		List<Table3.Record> lTable3=sourceWrapper.getTableInstance(Table3.class).getRecords();
		List<Table4.Record> lTable4=sourceWrapper.getTableInstance(Table4.class).getRecords();

		Table1 table1=destinationWrapper.getTableInstance(Table1.class);
		for (Table1.Record r : lTable1)
		{
			Map<String, Object> m=Table.getFields(table1.getFieldAccessors(), r);
			Assert.assertTrue(table1.equalsAllFields(table1.addRecord(m), r));
		}
		Table3 table3=destinationWrapper.getTableInstance(Table3.class);
		for (Table3.Record r : lTable3)
		{
			Map<String, Object> m=Table.getFields(table3.getFieldAccessors(), r);
			Assert.assertTrue(table3.equalsAllFields(table3.addRecord(m), r));
		}
		Table2 table2=destinationWrapper.getTableInstance(Table2.class);
		for (Table2.Record r : lTable2)
		{
			Map<String, Object> m=Table.getFields(table2.getFieldAccessors(), r);
			Assert.assertTrue(table2.equalsAllFields(table2.addRecord(m), r));
		}
		Table4 table4=destinationWrapper.getTableInstance(Table4.class);
		for (Table4.Record r : lTable4)
		{
			Map<String, Object> m=Table.getFields(table4.getFieldAccessors(), r);
			Assert.assertTrue(table4.equalsAllFields(table4.addRecord(m), r));
		}
		assertEquals(sourceWrapper, destinationWrapper, true);

	}

	@SuppressWarnings("SameParameterValue")
	private void assertEquals(DatabaseWrapper sourceWrapper, DatabaseWrapper destinationWrapper, boolean bidirectional) throws DatabaseException {
		List<Table1.Record> lTable1=sourceWrapper.getTableInstance(Table1.class).getRecords();
		List<Table2.Record> lTable2=sourceWrapper.getTableInstance(Table2.class).getRecords();
		List<Table3.Record> lTable3=sourceWrapper.getTableInstance(Table3.class).getRecords();
		List<Table4.Record> lTable4=sourceWrapper.getTableInstance(Table4.class).getRecords();

		List<Table1.Record> lTable1D=destinationWrapper.getTableInstance(Table1.class).getRecords();
		List<Table2.Record> lTable2D=destinationWrapper.getTableInstance(Table2.class).getRecords();
		List<Table3.Record> lTable3D=destinationWrapper.getTableInstance(Table3.class).getRecords();
		List<Table4.Record> lTable4D=destinationWrapper.getTableInstance(Table4.class).getRecords();

		if (bidirectional) {
			Assert.assertEquals(lTable1D.size(), lTable1.size());
			Assert.assertEquals(lTable2D.size(), lTable2.size());
			Assert.assertEquals(lTable3D.size(), lTable3.size());
			Assert.assertEquals(lTable4D.size(), lTable4.size());
		}
		Table1 table1=destinationWrapper.getTableInstance(Table1.class);
		Table2 table2=destinationWrapper.getTableInstance(Table2.class);
		Table3 table3=destinationWrapper.getTableInstance(Table3.class);
		Table4 table4=destinationWrapper.getTableInstance(Table4.class);

		for (Table1.Record r : lTable1)
		{
			boolean found=false;
			for (Table1.Record r2 : lTable1D)
			{
				if (table1.equals(r, r2))
				{
					found=true;
					Assert.assertTrue(table1.equalsAllFields(r, r2), ""+r);
					break;
				}
			}
			Assert.assertTrue(found, ""+r);

		}

		for (Table2.Record r : lTable2)
		{
			boolean found=false;
			for (Table2.Record r2 : lTable2D)
			{
				if (table2.equalsAllFields(r, r2))
				{
					found=true;
					break;
				}
			}
			Assert.assertTrue(found);
		}

		for (Table3.Record r : lTable3)
		{
			boolean found=false;
			for (Table3.Record r2 : lTable3D)
			{
				if (table3.equalsAllFields(r, r2))
				{
					found=true;
					break;
				}
			}
			Assert.assertTrue(found);
		}

		for (Table4.Record r : lTable4)
		{
			boolean found=false;
			for (Table4.Record r2 : lTable4D)
			{
				if (table4.equalsAllFields(r, r2))
				{
					found=true;
					break;
				}
			}
			Assert.assertTrue(found);
		}

	}
	private static int uniqueField=1;
	private void addAndRemoveData(DatabaseWrapper wrapper, int nb) throws DatabaseException {
		Table1 table1=wrapper.getTableInstance(Table1.class);
		Table2 table2=wrapper.getTableInstance(Table2.class);
		Table3 table3=wrapper.getTableInstance(Table3.class);
		Table4 table4=wrapper.getTableInstance(Table4.class);

		for (int i=0;i<nb;i++)
		{
			boolean remove=Math.random()<0.1;
			if (remove)
			{
				table1.removeRecordsWithCascade(new Filter<Table1.Record>() {
					@Override
					public boolean nextRecord(Table1.Record _record)  {
						stopTableParsing();
						return true;
					}
				});
				table2.removeRecords(new Filter<Table2.Record>() {
					@Override
					public boolean nextRecord(Table2.Record _record)  {
						stopTableParsing();
						return true;
					}
				});
				table3.removeRecordsWithCascade(new Filter<Table3.Record>() {
					@Override
					public boolean nextRecord(Table3.Record _record)  {
						stopTableParsing();
						return true;
					}
				});
				table4.removeRecords(new Filter<Table4.Record>() {
					@Override
					public boolean nextRecord(Table4.Record _record)  {
						stopTableParsing();
						return true;
					}
				});
			}
			else
			{
				table1.addRecord(getTable1_3Map());
				Table1.Record addedA2=table1.addRecord(getTable1_3Map());
				table2.addRecord("fr1_pk1", addedA2, "int_value", uniqueField++);
				table3.addRecord(getTable1_3Map());
				Table3.Record addedB2=table3.addRecord(getTable1_3Map());
				table4.addRecord("fr1_pk1", addedB2, "int_value", uniqueField++);

			}
		}
	}

	@BeforeClass
	public void loadReferenceDatabase() throws DatabaseException {
		DatabaseWrapper.deleteDatabaseFiles(referenceDatabaseDirectory);
		DatabaseWrapper.deleteDatabaseFiles(databaseDirectory);
		FileTools.deleteDirectory(this.externalBackupDirectory);
		wrapperForReferenceDatabase=loadWrapper( referenceDatabaseDirectory, false);
		addAndRemoveData(wrapperForReferenceDatabase, 100);
		Table1 table1=wrapperForReferenceDatabase.getTableInstance(Table1.class);
		Table2 table2=wrapperForReferenceDatabase.getTableInstance(Table2.class);
		Table3 table3=wrapperForReferenceDatabase.getTableInstance(Table3.class);
		Table4 table4=wrapperForReferenceDatabase.getTableInstance(Table4.class);

		List<Table1.Record> lTable1=table1.getRecords();
		List<Table2.Record> lTable2=table2.getRecords();
		List<Table3.Record> lTable3=table3.getRecords();
		List<Table4.Record> lTable4=table4.getRecords();

		recordWithoutForeignKeyA=null;
		recordWithoutForeignKeyAToRemove=null;
		recordPointedByOtherA=null;
		recordPointedByOtherAToRemove=null;
		for (Table1.Record r : lTable1)
		{
			if (!table1.isRecordPointedByForeignKeys(r)) {
				if (recordWithoutForeignKeyA ==null)
					recordWithoutForeignKeyA = r;
				else
				{
					recordWithoutForeignKeyAToRemove=r;
				}
			}
			else
			{
				if (recordPointedByOtherA==null)
					recordPointedByOtherA=r;
				else
					recordPointedByOtherAToRemove=r;
			}
			if (recordWithoutForeignKeyA!=null && recordWithoutForeignKeyAToRemove!=null
					&& recordPointedByOtherA!=null && recordPointedByOtherAToRemove!=null)
				break;
		}
		Assert.assertNotNull(recordWithoutForeignKeyA);
		Assert.assertNotNull(recordWithoutForeignKeyAToRemove);
		Assert.assertNotNull(recordPointedByOtherA);
		Assert.assertNotNull(recordPointedByOtherAToRemove);

		recordPointingToOtherA=lTable2.get(50);
		recordPointingToOtherAToRemove=lTable2.get(55);


		recordWithoutForeignKeyB=null;
		recordWithoutForeignKeyBToRemove=null;
		recordPointedByOtherB=null;
		recordPointedByOtherBToRemove=null;
		for (Table3.Record r : lTable3)
		{
			if (!table3.isRecordPointedByForeignKeys(r)) {
				if (recordWithoutForeignKeyB ==null)
					recordWithoutForeignKeyB = r;
				else
				{
					recordWithoutForeignKeyBToRemove=r;
				}
			}
			else
			{
				if (recordPointedByOtherB==null)
					recordPointedByOtherB=r;
				else
					recordPointedByOtherBToRemove=r;
			}
			if (recordWithoutForeignKeyB!=null && recordWithoutForeignKeyBToRemove!=null
					&& recordPointedByOtherB!=null && recordPointedByOtherBToRemove!=null)
				break;

		}

		recordPointingToOtherB=lTable4.get(50);
		recordPointingToOtherBToRemove=lTable4.get(55);

	}

	@AfterClass
	public void unloadReferenceDatabase()
	{
		try {
			recordWithoutForeignKeyA=null;
			recordPointedByOtherA=null;
			recordWithoutForeignKeyAToRemove=null;
			recordPointedByOtherAToRemove=null;

			recordPointingToOtherA=null;
			recordPointingToOtherAToRemove=null;

			recordWithoutForeignKeyB=null;
			recordPointedByOtherB=null;
			recordWithoutForeignKeyBToRemove=null;
			recordPointedByOtherBToRemove=null;

			recordPointingToOtherB=null;
			recordPointingToOtherBToRemove=null;
			wrapperForReferenceDatabase.deleteDatabaseFiles();
		}
		finally {
			wrapperForReferenceDatabase = null;

		}
	}
	@AfterMethod
	public void unloadDatabase()
	{
		System.out.println("Removing database reference");
		try {
			wrapper.deleteDatabaseFiles();
			FileTools.deleteDirectory(this.externalBackupDirectory);
		}
		finally {
			wrapper = null;
		}
	}


	private DatabaseWrapper loadWrapper(File databaseDirectory, boolean useInternalBackup) throws DatabaseException {
		DatabaseWrapper wrapper=new EmbeddedH2DatabaseWrapper(databaseDirectory);
		BackupConfiguration backupConf=null;
		if (useInternalBackup)
			backupConf=new BackupConfiguration(1000L, 10000L, 1000000, 200L, null);
		DatabaseConfiguration conf=new DatabaseConfiguration( Table1.class.getPackage(), null, null, backupConf);

		wrapper.loadDatabase(conf, true);
		return wrapper;
	}


	@SuppressWarnings("SameParameterValue")
	private void loadDatabase(boolean useSeveralRestorationPoint, boolean useInternalBackup, AtomicLong dateRestoration, AtomicLong dataLoadStart, boolean useExternalBackup, boolean addAdditionData, boolean alterRecords) throws DatabaseException, InterruptedException {
		dataLoadStart.set(System.currentTimeMillis());
		wrapper=loadWrapper(databaseDirectory, useInternalBackup);

		loadData(wrapperForReferenceDatabase, wrapper);
		if (useExternalBackup) {
			long t=System.currentTimeMillis();
			BackupRestoreManager externalBRM = wrapper.getExternalBackupRestoreManager(externalBackupDirectory, Table1.class.getPackage());
			Assert.assertNotNull(externalBRM);
			Assert.assertTrue(externalBRM.isEmpty());
			externalBRM.createBackupReference();
			Assert.assertEquals(externalBRM.getBackupDirectory(), new File(externalBackupDirectory, DatabaseWrapper.getLongPackageName(Table1.class.getPackage())));

			Assert.assertTrue(externalBRM.getMinDateUTCInMs() > dataLoadStart.get(), externalBRM.getMinDateUTCInMs()+";"+dataLoadStart.get());
			Assert.assertTrue(t<externalBRM.getMinDateUTCInMs(), t+";"+externalBRM.getMinDateUTCInMs());
			Assert.assertTrue(System.currentTimeMillis()>externalBRM.getMinDateUTCInMs());
			Assert.assertTrue(externalBRM.getMinDateUTCInMs()<externalBRM.getMaxDateUTCInMS());
			Assert.assertTrue(System.currentTimeMillis()>externalBRM.getMaxDateUTCInMS());
		}
		Thread.sleep(100);
		dateRestoration.set(System.currentTimeMillis());
		Thread.sleep(200);

		if (alterRecords)
			alterRecords(wrapper);
		if (addAdditionData) {
			int nbPartFiles=0;
			int nbReferenceFiles=0;
			BackupRestoreManager internalBRM=wrapper.getBackupRestoreManager(Table1.class.getPackage());
			if (internalBRM!=null) {
				nbPartFiles = internalBRM.getPartFilesCount();
				nbReferenceFiles = internalBRM.getReferenceFileCount();
			}
			addAndRemoveData(wrapper, 3);
			Thread.sleep(200);
			addAndRemoveData(wrapper, 3);
			if (internalBRM!=null) {
				Assert.assertTrue(nbPartFiles<internalBRM.getPartFilesCount());
				nbPartFiles = internalBRM.getPartFilesCount();
			}
			Thread.sleep(200);
			if (useSeveralRestorationPoint) {
				Thread.sleep(1000L);
				addAndRemoveData(wrapper, 10);
				Thread.sleep(1000L);
				addAndRemoveData(wrapper, 10);
				Thread.sleep(1000L);
				addAndRemoveData(wrapper, 10);
				if (internalBRM!=null) {
					Assert.assertTrue(nbPartFiles<internalBRM.getPartFilesCount(), nbPartFiles+";"+internalBRM.getPartFilesCount());
					Assert.assertTrue(nbReferenceFiles<internalBRM.getReferenceFileCount(), nbReferenceFiles+";"+internalBRM.getReferenceFileCount());
				}
			}

		}
		if (alterRecords || addAdditionData) {
			BackupRestoreManager internalBRM = wrapper.getBackupRestoreManager(Table1.class.getPackage());
			Assert.assertEquals(internalBRM != null, useInternalBackup);

			if (internalBRM != null) {
				Assert.assertTrue(internalBRM.getMinDateUTCInMs() > dataLoadStart.get(), internalBRM.getMinDateUTCInMs()+";"+dataLoadStart.get());
				Assert.assertTrue(internalBRM.getMinDateUTCInMs() < internalBRM.getMaxDateUTCInMS(), internalBRM.getMinDateUTCInMs()+";"+internalBRM.getMaxDateUTCInMS());
				Thread.sleep(1);
				long utc=System.currentTimeMillis();
				Assert.assertTrue(internalBRM.getMaxDateUTCInMS() < utc, internalBRM.getMaxDateUTCInMS()+";"+utc);
				//dataLoadStart.set(utc);
				Thread.sleep(1);
			}
		}
	}



	private void testExternalBackupAndRestore(boolean useSeveralRestorationPoint, boolean useInternalBackup, boolean restoreToEmptyDatabase, boolean useExternalBRM) throws DatabaseException, InterruptedException {
		assert useExternalBRM || useInternalBackup;
		assert useExternalBRM || !restoreToEmptyDatabase;

		AtomicLong dataLoadStart=new AtomicLong();
		AtomicLong dateRestoration=new AtomicLong();
		boolean alterRecords=true;
		boolean addAdditionData=true;
		//noinspection ConstantConditions
		loadDatabase(useSeveralRestorationPoint, useInternalBackup, dateRestoration, dataLoadStart, useExternalBRM, addAdditionData, alterRecords);

		if (restoreToEmptyDatabase)
		{
			wrapper.deleteDatabaseFiles();
			wrapper=loadWrapper(databaseDirectory, useInternalBackup);
		}
		BackupRestoreManager internalBRM=wrapper.getBackupRestoreManager(Table1.class.getPackage());

		BackupRestoreManager usedBRM;
		if (useExternalBRM) {
			usedBRM = wrapper.getExternalBackupRestoreManager(externalBackupDirectory, Table1.class.getPackage());
			Assert.assertTrue(usedBRM.getMaxDateUTCInMS() < dateRestoration.get());

		}
		else {
			usedBRM = internalBRM;
			//noinspection ConstantConditions
			if (alterRecords || addAdditionData) {

				Assert.assertTrue(usedBRM.getMaxDateUTCInMS() > dateRestoration.get());
			}
			else {
				Assert.assertTrue(usedBRM.getMaxDateUTCInMS() < dateRestoration.get());
			}
		}
		Assert.assertTrue(usedBRM.getMinDateUTCInMs() < usedBRM.getMaxDateUTCInMS());



		Assert.assertNotNull(usedBRM);
		int oldVersion=wrapper.getCurrentDatabaseVersion(Table1.class.getPackage());
		Assert.assertFalse(wrapper.isClosed());
		usedBRM.restoreDatabaseToDateUTC(dateRestoration.get());
		Assert.assertNotEquals(wrapper.getCurrentDatabaseVersion(Table1.class.getPackage()), oldVersion, wrapper.getCurrentDatabaseVersion(Table1.class.getPackage())+";"+oldVersion);
		Assert.assertFalse(wrapper.doesVersionExists(Table1.class.getPackage(), oldVersion));
		wrapper.close();
		wrapper=loadWrapper(databaseDirectory, useInternalBackup);
		Assert.assertNotEquals(wrapper.getCurrentDatabaseVersion(Table1.class.getPackage()), oldVersion);
		Assert.assertFalse(wrapper.doesVersionExists(Table1.class.getPackage(), oldVersion));
		assertEquals(wrapperForReferenceDatabase,wrapper, true);
		Assert.assertTrue(usedBRM.getMinDateUTCInMs()>dataLoadStart.get(), usedBRM.getMinDateUTCInMs()+";"+dataLoadStart.get());
		Assert.assertTrue(usedBRM.getMinDateUTCInMs()<usedBRM.getMaxDateUTCInMS());
		Assert.assertTrue(usedBRM.getMaxDateUTCInMS()<dateRestoration.get());
		if (internalBRM!=null)
		{
			Assert.assertTrue(internalBRM.getMinDateUTCInMs()>dataLoadStart.get());
			Assert.assertTrue(internalBRM.getMinDateUTCInMs()<internalBRM.getMaxDateUTCInMS());
			Assert.assertTrue(internalBRM.getMaxDateUTCInMS()<System.currentTimeMillis());
		}

	}

	@Test(dataProvider = "DataProvExtBackupRestore")
	public void testExternalBackupAndRestore(boolean useSeveralRestorationPoint, boolean useInternalBackup, boolean restoreToEmptyDatabase) throws DatabaseException, InterruptedException {
		testExternalBackupAndRestore(useSeveralRestorationPoint, useInternalBackup, restoreToEmptyDatabase, true);
	}

	/*private <R extends DatabaseRecord> void testRecordRestoration(DatabaseWrapper wrapper, BackupRestoreManager externalBRM, long dateUTC, R referenceRecord, boolean withCascade, boolean expectedToBeRestored, boolean currentDeleted, boolean oldDeleted) throws DatabaseException {

		Table<R> table=wrapper.getTableInstanceFromRecord(referenceRecord);
		Map<String, Object> primaryKeys=Table.getFields(table.getPrimaryKeysFieldAccessors(), referenceRecord);
		if (currentDeleted)
			Assert.assertNull(table.getRecord(primaryKeys));
		else
			Assert.assertNotNull(table.getRecord(primaryKeys));

		Reference<R> restoredRecord=externalBRM.restoreRecordToDateUTC(dateUTC, withCascade, table, primaryKeys);
		if (expectedToBeRestored)
		{
			Assert.assertNotNull(restoredRecord);
			if (oldDeleted)
			{
				Assert.assertNull(restoredRecord.get());
			}
			else
			{
				Assert.assertNotNull(restoredRecord.get());
				Assert.assertTrue(table.equalsAllFields(restoredRecord.get(), referenceRecord));
				R dr=table.getRecord(primaryKeys);
				Assert.assertTrue(table.equalsAllFields(dr, referenceRecord));
			}
		}
		else
			Assert.assertNull(restoredRecord);


	}*/

	private Map<String, Object> getTable1_3Map()
	{
		HashMap<String, Object> map = new HashMap<>();
		map.put("pk1", (int)( Math.random()*1000));
		map.put("int_value", (int)(Math.random()*1000));
		map.put("byte_value", (byte) (Math.random()*100));
		map.put("char_value", 'x');
		map.put("boolean_value", Math.random()<0.5);
		map.put("short_value", (short) (Math.random()*1000));
		map.put("long_value", (long)(Math.random()*1000));
		map.put("float_value", (float)(Math.random()*1000));
		map.put("double_value", Math.random()*1000);
		map.put("string_value", "test string");
		map.put("IntegerNumber_value", (int) (Math.random() * 1000));
		map.put("ByteNumber_value", (byte)(Math.random()*1000));
		map.put("CharacterNumber_value", 'x');
		map.put("BooleanNumber_value", Math.random()<0.5);
		map.put("ShortNumber_value", (short)(Math.random()*1000));
		map.put("LongNumber_value", (long)(Math.random()*1000));
		map.put("FloatNumber_value", (float)(Math.random()*1000));
		map.put("DoubleNumber_value", Math.random()*1000);
		map.put("BigInteger_value", new BigInteger("5"));
		map.put("BigDecimal_value", new BigDecimal("8.8"));
		map.put("DateValue", date);
		map.put("CalendarValue", calendar);
		map.put("secretKey", secretKey);
		map.put("typeSecretKey", typeSecretKey);
		map.put("subField", subField);
		map.put("subSubField", subSubField);
		map.put("file", fileTest);
		return map;
	}

	private void alterRecords(DatabaseWrapper wrapper) throws DatabaseException {
		Table1 table1=wrapper.getTableInstance(Table1.class);
		Map<String, Object> pks;

		pks=Table.getFields(table1.getPrimaryKeysFieldAccessors(), recordWithoutForeignKeyA);
		Table1.Record recordWithoutForeignKeyA=table1.getRecord(pks);
		Assert.assertNotNull(recordWithoutForeignKeyA);
		recordWithoutForeignKeyA.boolean_value=!recordWithoutForeignKeyA.boolean_value;
		table1.updateRecord(recordWithoutForeignKeyA);

		pks=Table.getFields(table1.getPrimaryKeysFieldAccessors(), recordPointedByOtherA);
		Table1.Record recordPointedByOtherA=table1.getRecord(pks);
		recordPointedByOtherA.boolean_value=!recordPointedByOtherA.boolean_value;
		table1.updateRecord(recordPointedByOtherA);

		table1.removeRecord(recordWithoutForeignKeyAToRemove);
		table1.removeRecordWithCascade(recordPointedByOtherAToRemove);

		Table2 table2=wrapper.getTableInstance(Table2.class);

		pks=Table.getFields(table2.getPrimaryKeysFieldAccessors(), recordPointingToOtherA);
		Table2.Record recordPointingToOtherA=table2.getRecord(pks);
		recordPointingToOtherA.int_value=-recordPointingToOtherA.int_value;
		table2.updateRecord(recordPointingToOtherA);

		pks=Table.getFields(table2.getPrimaryKeysFieldAccessors(), recordPointingToOtherAToRemove);
		Table2.Record recordPointingToOtherAToRemove=table2.getRecord(pks);
		Map<String, Object> map = getTable1_3Map();
		table2.removeRecord(recordPointingToOtherAToRemove);
		recordPointingToOtherAToRemove.fr1_pk1=table1.addRecord(map);
		table2.addRecord(recordPointingToOtherAToRemove);

		Table3 table3=wrapper.getTableInstance(Table3.class);

		pks=Table.getFields(table3.getPrimaryKeysFieldAccessors(), recordWithoutForeignKeyB);
		Table3.Record recordWithoutForeignKeyB=table3.getRecord(pks);
		recordWithoutForeignKeyB.boolean_value=!recordWithoutForeignKeyB.boolean_value;
		table3.updateRecord(recordWithoutForeignKeyB);

		pks=Table.getFields(table3.getPrimaryKeysFieldAccessors(), recordPointedByOtherB);
		Table3.Record recordPointedByOtherB=table3.getRecord(pks);
		recordPointedByOtherB.boolean_value=!recordPointedByOtherB.boolean_value;
		table3.updateRecord(recordPointedByOtherB);

		table3.removeRecord(recordWithoutForeignKeyBToRemove);
		table3.removeRecordWithCascade(recordPointedByOtherBToRemove);

		Table4 table4=wrapper.getTableInstance(Table4.class);

		pks=Table.getFields(table4.getPrimaryKeysFieldAccessors(), recordPointingToOtherB);
		Table4.Record recordPointingToOtherB=table4.getRecord(pks);
		recordPointingToOtherB.int_value=-recordPointingToOtherB.int_value;
		table4.updateRecord(recordPointingToOtherB);

		pks=Table.getFields(table4.getPrimaryKeysFieldAccessors(), recordPointingToOtherBToRemove);
		Table4.Record recordPointingToOtherBToRemove=table4.getRecord(pks);
		table4.removeRecord(recordPointingToOtherBToRemove);
		recordPointingToOtherBToRemove.fr1_pk1=table3.addRecord(map);
		table4.addRecord(recordPointingToOtherBToRemove);

	}

	/*@Test(dataProvider = "DataProvExtIndBackupRestore", dependsOnMethods = "testExternalBackupAndRestore")
	public void testIndividualExternalBackupAndRestore(boolean useInternalBackup, boolean useSeveralRestorationPoint, boolean addAdditionalData) throws DatabaseException, InterruptedException {
		testIndividualBackupAndRestore(useInternalBackup, useSeveralRestorationPoint, addAdditionalData, true);
	}*/


	/*public void testIndividualBackupAndRestore(boolean useInternalBackup, boolean useSeveralRestorationPoint, boolean addAdditionalData, boolean useExternalManager) throws DatabaseException, InterruptedException {
		assert useExternalManager || useInternalBackup;
		AtomicLong dataLoadStart=new AtomicLong();
		AtomicLong dateRestoration=new AtomicLong();
		loadDatabase(useSeveralRestorationPoint, useInternalBackup, dateRestoration, dataLoadStart, true, addAdditionalData, true);

		BackupRestoreManager internalBRM=wrapper.getBackupRestoreManager(Table1.class.getPackage());

		BackupRestoreManager usedBRM;
		if (useExternalManager)
			usedBRM=wrapper.getExternalBackupRestoreManager(externalBackupDirectory, Table1.class.getPackage());
		else
			usedBRM=internalBRM;
		Assert.assertTrue(usedBRM.getMinDateUTCInMs()>dataLoadStart.get());
		Assert.assertTrue(usedBRM.getMinDateUTCInMs()<usedBRM.getMaxDateUTCInMS());
		Assert.assertTrue(usedBRM.getMaxDateUTCInMS()<dateRestoration.get());

		Assert.assertNotNull(usedBRM);

		alterRecords(wrapper);
		Table1.Record addedA=wrapper.getTableInstance(Table1.class).addRecord(getTable1_3Map());
		Table1.Record addedA2=wrapper.getTableInstance(Table1.class).addRecord(getTable1_3Map());
		Table2.Record addedA3=wrapper.getTableInstance(Table2.class).addRecord("fr1_pk1", addedA2, "int_value", 5);
		Table3.Record addedB=wrapper.getTableInstance(Table3.class).addRecord(getTable1_3Map());
		Table3.Record addedB2=wrapper.getTableInstance(Table3.class).addRecord(getTable1_3Map());
		Table4.Record addedB3=wrapper.getTableInstance(Table4.class).addRecord("fr1_pk1", addedB2, "int_value", 7);

		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordWithoutForeignKeyA, true, true, false, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointedByOtherA, true, true, false, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordWithoutForeignKeyAToRemove, true, true, true, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointedByOtherAToRemove, true, true, true, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointingToOtherA, true, true, false, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointingToOtherAToRemove, true, true, true, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), addedA, true, true, false, true);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), addedA2, true, true, false, true);
		Assert.assertNull(wrapper.getTableInstance(Table2.class).getRecord(Table.getFields(wrapper.getTableInstance(Table2.class).getPrimaryKeysFieldAccessors(), addedA3)));

		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordWithoutForeignKeyB, true, true, false, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointedByOtherB, true, true, false, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordWithoutForeignKeyBToRemove, true, true, true, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointedByOtherBToRemove, true, true, true, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointingToOtherB, true, true, false, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointingToOtherBToRemove, true, true, true, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), addedB, true, true, false, true);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), addedB2, true, true, false, true);
		Assert.assertNull(wrapper.getTableInstance(Table4.class).getRecord(Table.getFields(wrapper.getTableInstance(Table4.class).getPrimaryKeysFieldAccessors(), addedB3)));

		Assert.assertTrue(usedBRM.getMinDateUTCInMs()>dataLoadStart.get());
		Assert.assertTrue(usedBRM.getMinDateUTCInMs()<usedBRM.getMaxDateUTCInMS());
		Assert.assertTrue(usedBRM.getMaxDateUTCInMS()<dateRestoration.get());
		if (internalBRM!=null)
		{
			Assert.assertTrue(internalBRM.getMinDateUTCInMs()>dataLoadStart.get());
			Assert.assertTrue(internalBRM.getMinDateUTCInMs()<internalBRM.getMaxDateUTCInMS());
			Assert.assertTrue(internalBRM.getMaxDateUTCInMS()<System.currentTimeMillis());
		}

		assertEquals(wrapperForReferenceDatabase,wrapper, !addAdditionalData);

		alterRecords(wrapper);
		addedA=wrapper.getTableInstance(Table1.class).addRecord(getTable1_3Map());
		addedA2=wrapper.getTableInstance(Table1.class).addRecord(getTable1_3Map());
		addedA3=wrapper.getTableInstance(Table2.class).addRecord("fr1_pk1", addedA2, "int_value", 5);
		addedB=wrapper.getTableInstance(Table3.class).addRecord(getTable1_3Map());
		addedB2=wrapper.getTableInstance(Table3.class).addRecord(getTable1_3Map());
		addedB3=wrapper.getTableInstance(Table4.class).addRecord("fr1_pk1", addedB2, "int_value", 7);

		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordWithoutForeignKeyA, false, true, false, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointedByOtherA, false, false, false, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordWithoutForeignKeyAToRemove, false, true, true, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointedByOtherAToRemove, false, false, true, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointingToOtherA, false, true, false, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointingToOtherAToRemove, false, false, true, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), addedA, false, true, false, true);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), addedA2, false, false, false, true);
		Assert.assertNotNull(wrapper.getTableInstance(Table2.class).getRecord(Table.getFields(wrapper.getTableInstance(Table2.class).getPrimaryKeysFieldAccessors(), addedA3)));

		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordWithoutForeignKeyB, false, true, false, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointedByOtherB, false, false, false, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordWithoutForeignKeyBToRemove, false, true, true, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointedByOtherBToRemove, false, false, true, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointingToOtherB, false, true, false, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), recordPointingToOtherBToRemove, false, false, true, false);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), addedB, false, true, false, true);
		testRecordRestoration(wrapper, usedBRM, dateRestoration.get(), addedB2, false, false, false, true);
		Assert.assertNotNull(wrapper.getTableInstance(Table4.class).getRecord(Table.getFields(wrapper.getTableInstance(Table4.class).getPrimaryKeysFieldAccessors(), addedB3)));

		Assert.assertTrue(usedBRM.getMinDateUTCInMs()>dataLoadStart.get());
		Assert.assertTrue(usedBRM.getMinDateUTCInMs()<usedBRM.getMaxDateUTCInMS());
		Assert.assertTrue(usedBRM.getMaxDateUTCInMS()<dateRestoration.get());
		if (internalBRM!=null)
		{
			Assert.assertTrue(internalBRM.getMinDateUTCInMs()>dataLoadStart.get());
			Assert.assertTrue(internalBRM.getMinDateUTCInMs()<internalBRM.getMaxDateUTCInMS());
			Assert.assertTrue(internalBRM.getMaxDateUTCInMS()<System.currentTimeMillis());
		}

	}*/

	@Test(dataProvider = "DataProvIntBackupRestore", dependsOnMethods = "testExternalBackupAndRestore")
	public void testInternalBackupAndRestore(boolean useSeveralRestorationPoint) throws DatabaseException, InterruptedException {
		testExternalBackupAndRestore(useSeveralRestorationPoint, true, false, false);
	}

	/*@Test(dataProvider = "DataProvIntIndBackupRestore", dependsOnMethods = "testInternalBackupAndRestore")
	public void testIndividualInternalBackupAndRestore(boolean useSeveralRestorationPoint, boolean addAdditionalData) throws DatabaseException, InterruptedException {
		testIndividualBackupAndRestore(true, useSeveralRestorationPoint, addAdditionalData, false);
	}*/


	@Test(dependsOnMethods = "testInternalBackupAndRestore")
	public void testBackupCleaning() throws DatabaseException, InterruptedException {
		DatabaseWrapper wrapper=new EmbeddedH2DatabaseWrapper(databaseDirectory);
		BackupConfiguration backupConf=new BackupConfiguration(200L, 1000L, 1000000, 100L, null);
		DatabaseConfiguration conf=new DatabaseConfiguration( Table1.class.getPackage(), null, null, backupConf);

		wrapper.loadDatabase(conf, true);
		BackupRestoreManager manager=wrapper.getBackupRestoreManager(Table1.class.getPackage());
		long start=System.currentTimeMillis();
		addAndRemoveData(wrapper, 100);
		Assert.assertTrue(start<=manager.getMinDateUTCInMs());
		Assert.assertTrue(manager.getMinDateUTCInMs()<=manager.getMaxDateUTCInMS());
		start=manager.getMinDateUTCInMs();
		Thread.sleep(200);
		addAndRemoveData(wrapper, 100);
		Thread.sleep(1000);
		manager.cleanOldBackups();
		Assert.assertTrue(start<manager.getMinDateUTCInMs());
		Assert.assertTrue(manager.getMinDateUTCInMs()<=manager.getMaxDateUTCInMS());
	}


	@DataProvider(name="DataProvExtBackupRestore")
	public Object[][] provideDataForExternalBackupRestore()
	{
		ArrayList<Object[]> l=new ArrayList<>();

		for (boolean useSeveralRestorationPoint : new boolean[]{true, false})
		{
			for (boolean useInternalBackup : new boolean[]{true, false})
			{
				for (boolean restoreToEmptyDatabase : new boolean[]{true, false})
				{
					l.add(new Object[]{useSeveralRestorationPoint, useInternalBackup, restoreToEmptyDatabase});
				}

			}

		}
		Object[][] res=new Object[l.size()][];
		int i=0;
		for (Object[] o : l)
			res[i++]=o;
		return res;
	}
	@DataProvider(name="DataProvExtIndBackupRestore")
	public Object[][] provideDataForExternalIndividualBackupRestore()
	{
		ArrayList<Object[]> l=new ArrayList<>();

		for (boolean useInternalBackup : new boolean[]{true, false})
		{
			for (boolean useSeveralRestorationPoint : new boolean[]{true, false})
			{
				for (boolean addAdditionalData : new boolean[]{true, false})
				{
					l.add(new Object[]{useInternalBackup, useSeveralRestorationPoint, addAdditionalData});
				}

			}

		}
		Object[][] res=new Object[l.size()][];
		int i=0;
		for (Object[] o : l)
			res[i++]=o;
		return res;

	}
	@DataProvider(name="DataProvIntBackupRestore")
	public Object[][] provideDataForInternalBackupRestore()
	{
		return new Object[][]{{true}, {false}};
	}

	@DataProvider(name="DataProvIntIndBackupRestore")
	public Object[][] provideDataForInternalIndividualBackupRestore()
	{
		ArrayList<Object[]> l=new ArrayList<>();

		for (boolean useSeveralRestorationPoint : new boolean[]{true, false})
		{
			for (boolean addAdditionalData : new boolean[]{true, false})
			{
				l.add(new Object[]{useSeveralRestorationPoint, addAdditionalData});
			}

		}
		Object[][] res=new Object[l.size()][];
		int i=0;
		for (Object[] o : l)
			res[i++]=o;
		return res;
	}
	@DataProvider(name="DataProvForTestTransactionCanceling")
	public Object[][] provideDataForTestTransactionCanceling()
	{
		ArrayList<Object[]> l=new ArrayList<>();

		for (boolean useSeveralRestorationPoint : new boolean[]{true, false})
		{
			for (boolean addAdditionalData : new boolean[]{true, false})
			{
				for (boolean alterRecords : new boolean[]{true, false}) {
					l.add(new Object[]{useSeveralRestorationPoint, addAdditionalData, alterRecords});
				}
			}

		}
		Object[][] res=new Object[l.size()][];
		int i=0;
		for (Object[] o : l)
			res[i++]=o;
		return res;
	}
	@Test(dependsOnMethods = "testBackupCleaning", dataProvider = "DataProvForTestTransactionCanceling")
	public void testTransactionCanceling(boolean useSeveralRestorationPoint, boolean addAdditionalData, boolean alterRecords) throws DatabaseException, InterruptedException {
		AtomicLong dataLoadStart=new AtomicLong();
		AtomicLong dateRestoration=new AtomicLong();

		loadDatabase(useSeveralRestorationPoint, true, dateRestoration, dataLoadStart, false, addAdditionalData, alterRecords);
		final Table1 table1=wrapper.getTableInstance(Table1.class);
		final Table2 table2=wrapper.getTableInstance(Table2.class);
		final Table3 table3=wrapper.getTableInstance(Table3.class);
		final Table4 table4=wrapper.getTableInstance(Table4.class);
		BackupRestoreManager manager=wrapper.getBackupRestoreManager(Table1.class.getPackage());
		long minDate=manager.getMinDateUTCInMs();
		long maxDate=manager.getMaxDateUTCInMS();
		File lastFile=manager.getLastFile();
		long lastFileSize=-1;
		if (lastFile!=null)
			lastFileSize=lastFile.getTotalSpace();
		wrapper.runSynchronizedTransaction(new SynchronizedTransaction<Void>() {
			@Override
			public Void run() throws Exception {
				for (int i=0;i<Math.random()*5;i++)
					table1.addRecord(getTable1_3Map());
				for (int i=0;i<Math.random()*5;i++)
					table3.addRecord(getTable1_3Map());
				for (int i=0;i<Math.random()*5;i++)
				{
					Table1.Record addedA2=table1.addRecord(getTable1_3Map());
					table2.addRecord("fr1_pk1", addedA2, "int_value", uniqueField++);
				}
				for (int i=0;i<Math.random()*5;i++)
				{
					Table3.Record addedB2=table3.addRecord(getTable1_3Map());
					table4.addRecord("fr1_pk1", addedB2, "int_value", uniqueField++);
				}
				cancelTransaction();



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
		});
		Assert.assertEquals(manager.getMinDateUTCInMs(), minDate);
		Assert.assertEquals(manager.getMaxDateUTCInMS(), maxDate);
		File lastFile2=manager.getLastFile();
		if (lastFile2==null)
			Assert.assertNull(lastFile);
		else
		{
			Assert.assertNotNull(lastFile2);
			Assert.assertEquals(lastFile2.getTotalSpace(), lastFileSize);
		}

	}
}
