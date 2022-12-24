package com.distrimind.ood.database.tests;
/*
Copyright or © or Corp. Jason Mahdjoub (01/04/2013)

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

import com.distrimind.ood.database.*;
import com.distrimind.ood.database.centraldatabaseapi.ClientTable;
import com.distrimind.ood.database.decentralizeddatabase.*;
import com.distrimind.ood.database.decentralizeddatabasev2.*;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedIDGenerator;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.ASymmetricAuthenticatedSignatureType;
import com.distrimind.util.crypto.SecureRandomType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 3.0.0
 */
public abstract class TestAddDatabaseWithCentralDatabaseBackupConnected extends TestDatabaseToOperateActionIntoDecentralizedNetwork {
	final BackupConfiguration backupConfiguration=new BackupConfiguration(10000, 20000, 1000000, 1000, null);

	protected TestAddDatabaseWithCentralDatabaseBackupConnected(boolean useCentralDatabaseBackup, boolean canSendIndirectTransactions, boolean upgradeDatabaseVersionWhenConnectedWithPeers, boolean upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion, boolean hasToRemoveOldDatabase) {
		super(useCentralDatabaseBackup, canSendIndirectTransactions, upgradeDatabaseVersionWhenConnectedWithPeers, upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion, hasToRemoveOldDatabase);
	}

	@Override
	public BackupConfiguration getBackupConfiguration()
	{
		return backupConfiguration;
	}
	@Override
	public boolean canInitCentralBackup()
	{
		return useCentralDatabaseBackup;
	}
	@Override
	protected boolean sendIndirectTransactions()
	{
		return canSendIndirectTransactions;
	}


	@Override
	public void doAction() throws Exception {

		if (upgradeDatabaseVersionWhenConnectedWithPeers)
		{
			connectAllDatabase();
		}
		else
			disconnectAllDatabase();
		if (upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion)
			connectCentralDatabaseBackupWithConnectedDatabase();
		else if (useCentralDatabaseBackup)
			disconnectCentralDatabaseBakcup();

		db1.getDbwrapper().getDatabaseConfigurationsBuilder()
				.addConfiguration(new DatabaseConfiguration(
								new DatabaseSchema(TablePointedV2.class.getPackage()),
								canInitCentralBackup() ? DatabaseConfiguration.SynchronizationType.DECENTRALIZED_SYNCHRONIZATION_AND_SYNCHRONIZATION_WITH_CENTRAL_BACKUP_DATABASE : DatabaseConfiguration.SynchronizationType.DECENTRALIZED_SYNCHRONIZATION,
								getPeersToSynchronize(db1), getBackupConfiguration(),
								true),
						false)
				.commit();

		for (int i=0;i<20;i++)
			addElements();

		exchangeMessages();
		List<Database> databasesToTest= Arrays.asList(db1, db2);
		if (upgradeDatabaseVersionWhenConnectedWithPeers || upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion)
		{
			testSynchronisationV2(db1, databasesToTest);
		}
		if (!upgradeDatabaseVersionWhenConnectedWithPeers)
		{
			connectAllDatabase(Collections.singletonList(db3.getHostID()), false);
		}
		exchangeMessages();
		if (!upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion && useCentralDatabaseBackup)
		{
			Thread.sleep(getBackupConfiguration().getMaxBackupFileAgeInMs());
			connectCentralDatabaseBackupWithConnectedDatabase();
		}
		testSynchronisationV2(db1, databasesToTest);
		connectAllDatabase();
		exchangeMessages();

		testSynchronisationV2();
	}

	protected void addElementsToDB2() throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase) {
			addElementsToDB2(db);
		}
	}

	protected void addElementsToDB2(CommonDecentralizedTests.Database db) throws DatabaseException {
		addTableAloneRecordToDB2(db);
		addTablePointedAndPointingRecordsV2(db);
	}

	protected void addTableAloneRecordToDB2(CommonDecentralizedTests.Database db) throws DatabaseException {
		TableAloneV2.Record ralone = generatesTableAloneRecordV2();
		TableAloneV2 tableAloneV2=db1.getDbwrapper().getTableInstance(TableAloneV2.class);
		tableAloneV2.addRecord(ralone);
	}
	protected void addTablePointedAndPointingRecordsV2(CommonDecentralizedTests.Database db) throws DatabaseException {
		TablePointedV2 tablePointedV2=db1.getDbwrapper().getTableInstance(TablePointedV2.class);
		TablePointingV2 tablePointingV2=db1.getDbwrapper().getTableInstance(TablePointingV2.class);

		TablePointedV2.Record rpointed = new TablePointedV2.Record();
		rpointed.id = new DecentralizedIDGenerator();
		rpointed.value = generateString();

		rpointed = tablePointedV2.addRecord(rpointed);

		TablePointingV2.Record rpointing1 = new TablePointingV2.Record();
		try {
			rpointing1.id = ASymmetricAuthenticatedSignatureType.BC_FIPS_Ed25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair().getASymmetricPublicKey();
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
		}

		rpointing1.table2 = null;
		tablePointingV2.addRecord(rpointing1);
		TablePointingV2.Record rpointing2 = new TablePointingV2.Record();
		try {
			rpointing2.id = ASymmetricAuthenticatedSignatureType.BC_FIPS_Ed25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair().getASymmetricPublicKey();
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
		rpointing2.table2 = rpointed;
		tablePointingV2.addRecord(rpointing2);
	}

	protected TableAloneV2.Record generatesTableAloneRecordV2() throws DatabaseException {

		return new TableAloneV2.Record(generatesTableAloneRecord());
	}

	protected void testSynchronisationV2(CommonDecentralizedTests.Database db, List<Database> listDatabase) throws DatabaseException {
		TableAloneV2 tableAloneV2=db.getDbwrapper().getTableInstance(TableAloneV2.class);
		TablePointedV2 tablePointedV2=db.getDbwrapper().getTableInstance(TablePointedV2.class);
		TablePointingV2 tablePointingV2=db.getDbwrapper().getTableInstance(TablePointingV2.class);
		for (TableAloneV2.Record r : tableAloneV2.getRecords()) {

			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					TableAloneV2.Record otherR = other.getDbwrapper().getTableInstance(TableAloneV2.class).getRecord("id", r.id, "id2", r.id2);
					Assert.assertNotNull(otherR);
					Assert.assertEquals(otherR.value, r.value);
				}
			}
		}
		for (TablePointedV2.Record r : tablePointedV2.getRecords()) {
			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					TablePointedV2.Record otherR = other.getDbwrapper().getTableInstance(TablePointedV2.class).getRecord("id", r.id);
					Assert.assertNotNull(otherR);
					Assert.assertEquals(otherR.value, r.value);
				}
			}
		}
		for (TablePointingV2.Record r : tablePointingV2.getRecords()) {
			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					TablePointingV2.Record otherR = other.getDbwrapper().getTableInstance(TablePointingV2.class).getRecord("id", r.id);
					Assert.assertNotNull(otherR);
					if (r.table2 == null)
						Assert.assertNull(otherR.table2);
					else
						Assert.assertEquals(otherR.table2.value, r.table2.value);
				}
			}
		}
	}

	protected void testSynchronisationV2() throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase)
		{
			testSynchronisationV2(db, listDatabase);
		}
	}

	@Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods = { "testAction" })
	public void testSynchroBetweenThreePeers2(boolean exceptionDuringTransaction, boolean generateDirectConflict,
											  boolean peersInitiallyConnected)
			throws Exception {
		for (TableEvent<DatabaseRecord> event : provideTableEventsForSynchro())
			testSynchroBetweenPeersImpl(3, exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dependsOnMethods = { "testSynchroBetweenThreePeers2" })
	public void testSynchroAfterTestsBetweenThreePeers2() throws DatabaseException {
		testSynchronisation();
	}
}
