package com.distrimind.ood.database.tests;
/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

jason.mahdjoub@distri-mind.fr

This software (Object Oriented Database (OOD)) is a computer program 
whose purpose is to manage a local database with the object paradigm 
and the java language 

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

import com.distrimind.ood.database.decentralizeddatabase.TableAlone;
import com.distrimind.ood.database.decentralizeddatabase.TablePointed;
import com.distrimind.ood.database.decentralizeddatabase.TablePointing;
import com.distrimind.ood.database.exceptions.DatabaseException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;

import java.util.ArrayList;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public abstract class TestRevertToOldVersionIntoDecentralizedNetwork extends TestDatabaseToOperateActionIntoDecentralizedNetwork{
	private final boolean preferOtherChannelThanLocalChannelIfAvailable;
	private ArrayList<TableAlone.Record> aloneRecords=null;
	private ArrayList<TablePointed.Record> pointedRecords=null;
	private ArrayList<TablePointing.Record> pointingRecords=null;

	protected TestRevertToOldVersionIntoDecentralizedNetwork(boolean useCentralDatabaseBackup, boolean canSendIndirectTransactions, boolean upgradeDatabaseVersionWhenConnectedWithPeers, boolean upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion, boolean hasToRemoveOldDatabase, boolean preferOtherChannelThanLocalChannelIfAvailable) {
		super(useCentralDatabaseBackup, canSendIndirectTransactions, upgradeDatabaseVersionWhenConnectedWithPeers, upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion, hasToRemoveOldDatabase);
		this.preferOtherChannelThanLocalChannelIfAvailable=preferOtherChannelThanLocalChannelIfAvailable;
	}

	@DataProvider(name = "constructorRevertParameters")
	public static Object[][] constructorRevertParameters() {

		Object[][] res=constructorParameters();
		Object[][] res2=new Object[res.length*2][res[0].length+1];
		int index=0;
		for (boolean preferOtherChannelThanLocalChannelIfAvailable : new boolean[]{false, true}) {
			for (Object[] re : res) {
				System.arraycopy(re, 0, res2[index], 0, re.length);
				res2[index++][re.length] = preferOtherChannelThanLocalChannelIfAvailable;
			}
		}
		assert index==res2.length;
		return res2;
	}

	@Override
	public void doAction() throws Exception {
		connectAllDatabase();
		exchangeMessages();
		disconnectAllDatabase();
		exchangeMessages();
		if (upgradeDatabaseVersionWhenConnectedWithPeers)
		{
			if(preferOtherChannelThanLocalChannelIfAvailable)
				connectSelectedDatabase(upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion, db1, db2, db3);
			else
				connectSelectedDatabase(upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion, db1, db2);
			//connectAllDatabase(Collections.singletonList(db3.getHostID()), upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion);
			exchangeMessages();
		} else if (upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion)
		{
			connectCentralDatabaseBackupWithConnectedDatabase();
			exchangeMessages();
		}
		backupActualDatabase();

		final long timeUTC=System.currentTimeMillis();
		Thread.sleep(getBackupConfiguration().getMaxBackupFileAgeInMs());
		testSynchronizationWithSavedRecords(db1);

		for (int i=0;i<20;i++) {
			addElements();
		}
		exchangeMessages();


		//db1.getDbwrapper().getBackupRestoreManager(TableAlone.class.getPackage()).restoreDatabaseToDateUTC(timeUTC, false);
		db1.getDbwrapper().getDatabaseConfigurationsBuilder()
				.restoreGivenDatabaseToOldVersion(TableAlone.class.getPackage(), timeUTC, preferOtherChannelThanLocalChannelIfAvailable, false)
				.commit();
		//testSynchronizationWithSavedRecords(db1);

		if (!upgradeDatabaseVersionWhenConnectedWithPeers)
		{
			if(preferOtherChannelThanLocalChannelIfAvailable)
				connectSelectedDatabase(upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion, db1, db2, db3);
			else
				connectSelectedDatabase(upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion, db1, db2);
			exchangeMessages();
			testSynchronizationWithSavedRecords(db1);
			testSynchronizationWithSavedRecords(db2);
			if (upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion || preferOtherChannelThanLocalChannelIfAvailable)
				testSynchronizationWithSavedRecords(db3);

		}
		else if (!upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion && useCentralDatabaseBackup) {
			connectCentralDatabaseBackupWithConnectedDatabase();
			exchangeMessages();
			testSynchronizationWithSavedRecords();
		}
		else
		{
			exchangeMessages();
			testSynchronizationWithSavedRecords(db1);
			testSynchronizationWithSavedRecords(db2);
			if (upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion)
				testSynchronizationWithSavedRecords(db3);
		}
		disconnectAllDatabase();
		exchangeMessages();
		connectAllDatabase();
		exchangeMessages();
		testSynchronizationWithSavedRecords();
	}

	private void backupActualDatabase() throws DatabaseException {
		testSynchronisation();
		aloneRecords=db1.getTableAlone().getRecords();
		pointedRecords=db1.getTablePointed().getRecords();
		pointingRecords=db1.getTablePointing().getRecords();
	}
	private void testSynchronizationWithSavedRecords() throws DatabaseException {
		for (Database db : listDatabase)
			testSynchronizationWithSavedRecords(db);
	}
	private void testSynchronizationWithSavedRecords(Database db) throws DatabaseException {
		TableAlone tableAlone=db.getDbwrapper().getTableInstance(TableAlone.class);
		TablePointed tablePointed=db.getDbwrapper().getTableInstance(TablePointed.class);
		TablePointing tablePointing=db.getDbwrapper().getTableInstance(TablePointing.class);
		Assert.assertEquals(tableAlone.getRecordsNumber(), this.aloneRecords.size());
		Assert.assertEquals(tablePointed.getRecordsNumber(), this.pointedRecords.size());
		Assert.assertEquals(tablePointing.getRecordsNumber(), this.pointingRecords.size());
		for (TableAlone.Record r : this.aloneRecords)
		{
			TableAlone.Record otherR = tableAlone.getRecord("id", r.id, "id2", r.id2);
			Assert.assertNotNull(otherR);
			Assert.assertEquals(otherR.value, r.value);
		}
		for (TablePointed.Record r : this.pointedRecords) {
			TablePointed.Record otherR = tablePointed.getRecord("id", r.id);
			Assert.assertNotNull(otherR);
			Assert.assertEquals(otherR.value, r.value);
		}
		for (TablePointing.Record r : this.pointingRecords) {
			TablePointing.Record otherR = tablePointing.getRecord("id", r.id);
			Assert.assertNotNull(otherR);
			if (r.table2 == null)
				Assert.assertNull(otherR.table2);
			else
				Assert.assertEquals(otherR.table2.value, r.table2.value);
		}
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName()+"{" +
				"useCentralDatabaseBackup=" + useCentralDatabaseBackup +
				", canSendIndirectTransactions=" + canSendIndirectTransactions +
				", upgradeDatabaseVersionWhenConnectedWithPeers=" + upgradeDatabaseVersionWhenConnectedWithPeers +
				", upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion=" + upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion +
				", hasToRemoveOldDatabase=" + hasToRemoveOldDatabase +
				", preferOtherChannelThanLocalChannelIfAvailable=" + preferOtherChannelThanLocalChannelIfAvailable +
				'}';
	}
}

