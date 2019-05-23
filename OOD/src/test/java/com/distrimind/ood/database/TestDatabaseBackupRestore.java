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

import com.distrimind.ood.database.database.Table1;
import com.distrimind.ood.database.exceptions.DatabaseException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Date;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0.0
 */
public class TestDatabaseBackupRestore {


	final DatabaseWrapper wrapperForReferenceDatabase;
	final File databaseDirectory=new File("backupDatabaseToTest");
	final File externalBackupDirectory=new File("externalBackupDatabaseToTest");

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
		try {
			new BackupConfiguration(5000L, 10000L, 1000000, 1000L, null);
			Assert.fail();
		}
		catch(IllegalArgumentException ignored)
		{

		}

	}

	@Test(dependsOnMethods = "testBackupConfParams")
	public void testNewFilePartIncrement()
	{
		//TODO test also backup limits
	}

	@Test(dependsOnMethods = "testNewFilePartIncrement")
	public void testNewFileReferenceAdd()
	{
		//TODO test also backup limits
	}

	private void loadData(DatabaseWrapper sourceWrapper, DatabaseWrapper destinationWrapper)
	{
		//TODO complete
	}

	private void assertEquals(DatabaseWrapper sourceWrapper, DatabaseWrapper destinationWrapper)
	{
		//TODO complete
	}
	private void addData(DatabaseWrapper wrapper, int nb)
	{
		//TODO complete
		//test also database version
	}

	private void copyDataFromOldVersionToOldVersion(DatabaseWrapper wrapper, int oldVersion, int newVersion)
	{
		//TODO complete
	}

	@Test(dataProvider = "DataProvExtBackupRestore", dependsOnMethods = "testNewFileReferenceAdd")
	public void testExternalBackupAndRestore(boolean useInternalBackup, boolean restoreToEmptyDatabase, boolean changeVersionDuringRestoration) throws DatabaseException, InterruptedException {
		DatabaseWrapper wrapper=new EmbeddedH2DatabaseWrapper(databaseDirectory);
		BackupConfiguration backupConf=null;
		if (useInternalBackup)
			backupConf=new BackupConfiguration(5000L, 10000L, 1000000, 1000L, null);
		DatabaseConfiguration conf=new DatabaseConfiguration(1, Table1.class.getPackage(), null, null, backupConf);

		wrapper.loadDatabase(conf, true);
		long dataLoadStart=System.currentTimeMillis();
		loadData(wrapperForReferenceDatabase, wrapper);
		BackupRestoreManager externalBRM=wrapper.getExternalBackupRestoreManager(externalBackupDirectory, Table1.class.getPackage());
		Assert.assertNotNull(externalBRM);
		externalBRM.createBackupReference();
		Thread.sleep(100);
		long dateRestoration=System.currentTimeMillis();
		Thread.sleep(200);


		addData(wrapper, 3);
		if (changeVersionDuringRestoration)
		{
			wrapper.close();
			DatabaseConfiguration conf2=new DatabaseConfiguration(2, Table1.class.getPackage(), new DatabaseLifeCycles() {
				@Override
				public void transferDatabaseFromOldVersion(DatabaseWrapper wrapper, DatabaseConfiguration oldDatabaseConfiguration, DatabaseConfiguration newDatabaseConfiguration) throws Exception {
					copyDataFromOldVersionToOldVersion(wrapper, oldDatabaseConfiguration.getVersion(), newDatabaseConfiguration.getVersion());
				}

				@Override
				public void afterDatabaseCreation(DatabaseWrapper wrapper, DatabaseConfiguration newDatabaseConfiguration) throws Exception {

				}

				@Override
				public boolean hasToRemoveOldDatabase() throws Exception {
					return true;
				}
			}, conf, backupConf);
			wrapper=new EmbeddedH2DatabaseWrapper(databaseDirectory);
			wrapper.loadDatabase(conf2, true);
			addData(wrapper, 4);
		}
		BackupRestoreManager internalBRM=wrapper.getBackupRestoreManager(Table1.class.getPackage());
		Assert.assertEquals(internalBRM!=null, useInternalBackup);

		if (internalBRM!=null)
		{
			Assert.assertTrue(internalBRM.getMinDateUTC()>dataLoadStart);
			Assert.assertTrue(internalBRM.getMinDateUTC()<internalBRM.getMaxDateUTC());
			Assert.assertTrue(internalBRM.getMaxDateUTC()<System.currentTimeMillis());
			dataLoadStart=System.currentTimeMillis();
		}
		externalBRM=wrapper.getExternalBackupRestoreManager(externalBackupDirectory, Table1.class.getPackage());
		Assert.assertTrue(externalBRM.getMinDateUTC()>dataLoadStart);
		Assert.assertTrue(externalBRM.getMinDateUTC()<externalBRM.getMaxDateUTC());
		Assert.assertTrue(externalBRM.getMaxDateUTC()<dateRestoration);

		Assert.assertNotNull(externalBRM);
		externalBRM.restoreDatabaseToDate(new Date(dateRestoration));
		assertEquals(wrapperForReferenceDatabase,wrapper );
		Assert.assertTrue(externalBRM.getMinDateUTC()>dataLoadStart);
		Assert.assertTrue(externalBRM.getMinDateUTC()<externalBRM.getMaxDateUTC());
		Assert.assertTrue(externalBRM.getMaxDateUTC()<dateRestoration);
		if (internalBRM!=null)
		{
			Assert.assertTrue(internalBRM.getMinDateUTC()>dataLoadStart);
			Assert.assertTrue(internalBRM.getMinDateUTC()<internalBRM.getMaxDateUTC());
			Assert.assertTrue(internalBRM.getMaxDateUTC()<System.currentTimeMillis());
		}

	}

	@Test(dataProvider = "DataProvExtIndBackupRestore", dependsOnMethods = "testExternalBackupAndRestore")
	public void testIndividualExternalBackupAndRestoreWithForeignKey(boolean useInternalBackup, boolean changeVersionDuringRestoration, boolean useSeveralRestorationPoint)
	{

	}

	@Test(dataProvider = "DataProvExtIndBackupRestore", dependsOnMethods = "testIndividualExternalBackupAndRestoreWithForeignKey")
	public void testIndividualExternalBackupAndRestoreWithoutForeignKey(boolean useInternalBackup, boolean changeVersionDuringRestoration, boolean useSeveralRestorationPoint)
	{

	}


	@Test(dataProvider = "DataProvIntBackupRestore")
	public void testInternalBackupAndRestore(boolean changeVersionDuringRestoration, boolean useSeveralRestorationPoint)
	{

	}

	@Test(dataProvider = "DataProvIntBackupRestore")
	public void testIndividualInternalBackupAndRestoreWithForeignKey(boolean changeVersionDuringRestoration, boolean useSeveralRestorationPoint)
	{

	}

	@Test(dataProvider = "DataProvIntBackupRestore")
	public void testIndividualInternalBackupAndRestoreWithoutForeignKey(boolean changeVersionDuringRestoration, boolean useSeveralRestorationPoint)
	{

	}



	@DataProvider(name="DataProvExtBackupRestore", parallel = true)
	public Object[][] provideDataForExternalBackupRestore()
	{

	}
	@DataProvider(name="DataProvExtIndBackupRestore", parallel = true)
	public Object[][] provideDataForExternalIndividualBackupRestore()
	{

	}
	@DataProvider(name="DataProvIntBackupRestore", parallel = true)
	public Object[][] provideDataForInternalBackupRestore()
	{

	}
}
