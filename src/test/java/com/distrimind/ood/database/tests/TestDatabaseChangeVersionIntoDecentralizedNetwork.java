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
import com.distrimind.ood.database.decentralizeddatabase.*;
import com.distrimind.ood.database.decentralizeddatabasev2.*;
import com.distrimind.ood.database.exceptions.DatabaseException;
import org.testng.Assert;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;


/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 3.0.0
 */
public abstract class TestDatabaseChangeVersionIntoDecentralizedNetwork extends TestDatabaseToOperateActionIntoDecentralizedNetwork {

	public TestDatabaseChangeVersionIntoDecentralizedNetwork(boolean useCentralDatabaseBackup, boolean canSendIndirectTransactions, boolean upgradeDatabaseVersionWhenConnectedWithPeers, boolean upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion, boolean hasToRemoveOldDatabase) {
		super(useCentralDatabaseBackup, canSendIndirectTransactions, upgradeDatabaseVersionWhenConnectedWithPeers, upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion, hasToRemoveOldDatabase);
	}


	@Override
	public void doAction() throws Exception {

		for (Database db : listDatabase)
			Assert.assertNotNull(db.getDbwrapper().getDatabaseConfigurationsBuilder().getLifeCycles());
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
		changeConfiguration(db1);
		changeConfiguration(db2);
		//Thread.sleep(getBackupConfiguration().getMaxBackupFileAgeInMs());
		exchangeMessages();
		Collection<Database> databasesToTest= Arrays.asList(db1, db2);
		if (upgradeDatabaseVersionWhenConnectedWithPeers || upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion)
		{
			testSynchronisationWithNewDatabaseVersion(db1, databasesToTest);
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
		testSynchronisationWithNewDatabaseVersion(db1, databasesToTest);
		changeConfiguration(db3);
		exchangeMessages();

		testSynchronisationWithNewDatabaseVersion();
	}
	protected void testSynchronisationWithNewDatabaseVersion() throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase)
			testSynchronisationWithNewDatabaseVersion(db, listDatabase);
	}
	protected void testSynchronisationWithNewDatabaseVersion(CommonDecentralizedTests.Database db, Collection<Database> listDatabase) throws DatabaseException {
		for (CommonDecentralizedTests.Database other : listDatabase) {
			if (other!=db) {
				Assert.assertEquals(db.getDbwrapper().getTableInstance(TableAloneV2.class).getRecordsNumber(), other.getDbwrapper().getTableInstance(TableAloneV2.class).getRecordsNumber());
				Assert.assertEquals(db.getDbwrapper().getTableInstance(TablePointedV2.class).getRecordsNumber(), other.getDbwrapper().getTableInstance(TablePointedV2.class).getRecordsNumber());
				Assert.assertEquals(db.getDbwrapper().getTableInstance(TablePointingV2.class).getRecordsNumber(), other.getDbwrapper().getTableInstance(TablePointingV2.class).getRecordsNumber());
			}
		}

		for (TableAloneV2.Record r : db.getDbwrapper().getTableInstance(TableAloneV2.class).getRecords()) {

			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					TableAloneV2.Record otherR = other.getDbwrapper().getTableInstance(TableAloneV2.class).getRecord("id", r.id, "id2", r.id2);
					Assert.assertNotNull(otherR);
					Assert.assertEquals(otherR.value, r.value);
				}
			}
		}
		for (TablePointedV2.Record r : db.getDbwrapper().getTableInstance(TablePointedV2.class).getRecords()) {
			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					TablePointedV2.Record otherR = other.getDbwrapper().getTableInstance(TablePointedV2.class).getRecord("id", r.id);
					Assert.assertNotNull(otherR);
					Assert.assertEquals(otherR.value, r.value);
				}
			}
		}
		for (TablePointingV2.Record r : db.getDbwrapper().getTableInstance(TablePointingV2.class).getRecords()) {
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
		for (UndecentralizableTableA1V2.Record r : db.getDbwrapper().getTableInstance(UndecentralizableTableA1V2.class).getRecords()) {
			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					UndecentralizableTableA1V2.Record otherR = other.getDbwrapper().getTableInstance(UndecentralizableTableA1V2.class).getRecord("id",
							r.id);
					if (otherR != null) {
						Assert.assertNotEquals(otherR.value, r.value);
					}
				}
			}
		}
		for (UndecentralizableTableB1V2.Record r : db.getDbwrapper().getTableInstance(UndecentralizableTableB1V2.class).getRecords()) {
			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					UndecentralizableTableB1V2.Record otherR = other.getDbwrapper().getTableInstance(UndecentralizableTableB1V2.class).getRecord("id", r.id);
					if (otherR != null) {
						if (r.pointing == null)
							Assert.assertNull(otherR.pointing);
						else
							Assert.assertNotEquals(otherR.pointing.value, r.pointing.value);
					}
				}
			}
		}
	}

	private void changeConfiguration(CommonDecentralizedTests.Database db) throws DatabaseException {
		long tableAloneNBR;
		long tablePointedNBR;
		long tablePointingNBR;
		long undecentralizableTableA1NBR;
		long undecentralizableTableB1NBR;
		{
			TableAlone tableAlone=db.getDbwrapper().getTableInstance(TableAlone.class);
			TablePointed tablePointed=db.getDbwrapper().getTableInstance(TablePointed.class);
			TablePointing tablePointing=db.getDbwrapper().getTableInstance(TablePointing.class);
			UndecentralizableTableA1 undecentralizableTableA1=db.getDbwrapper().getTableInstance(UndecentralizableTableA1.class);
			UndecentralizableTableB1 undecentralizableTableB1=db.getDbwrapper().getTableInstance(UndecentralizableTableB1.class);
			tableAloneNBR=tableAlone.getRecordsNumber();
			tablePointedNBR=tablePointed.getRecordsNumber();
			tablePointingNBR=tablePointing.getRecordsNumber();
			undecentralizableTableA1NBR=undecentralizableTableA1.getRecordsNumber();
			undecentralizableTableB1NBR=undecentralizableTableB1.getRecordsNumber();
			db.getDbwrapper().getDatabaseConfigurationsBuilder()
					.addConfiguration(new DatabaseConfiguration(
									new DatabaseSchema(TablePointedV2.class.getPackage(), new DatabaseSchema(TablePointing.class.getPackage())),
									canInitCentralBackup() ? DatabaseConfiguration.SynchronizationType.DECENTRALIZED_SYNCHRONIZATION_AND_SYNCHRONIZATION_WITH_CENTRAL_BACKUP_DATABASE : DatabaseConfiguration.SynchronizationType.DECENTRALIZED_SYNCHRONIZATION,
									getPeersToSynchronize(db), getBackupConfiguration(),
									true),
							false)
					.commit();
		}
		TableAloneV2 tableAloneV2=db.getDbwrapper().getTableInstance(TableAloneV2.class);
		TablePointedV2 tablePointedV2=db.getDbwrapper().getTableInstance(TablePointedV2.class);
		TablePointingV2 tablePointingV2=db.getDbwrapper().getTableInstance(TablePointingV2.class);
		UndecentralizableTableA1V2 undecentralizableTableA1V2=db.getDbwrapper().getTableInstance(UndecentralizableTableA1V2.class);
		UndecentralizableTableB1V2 undecentralizableTableB1V2=db.getDbwrapper().getTableInstance(UndecentralizableTableB1V2.class);
		Assert.assertEquals(tableAloneV2.getRecordsNumber(), tableAloneNBR);
		Assert.assertEquals(tablePointedV2.getRecordsNumber(), tablePointedNBR);
		Assert.assertEquals(tablePointingV2.getRecordsNumber(), tablePointingNBR);
		Assert.assertEquals(undecentralizableTableA1V2.getRecordsNumber(), undecentralizableTableA1NBR);
		Assert.assertEquals(undecentralizableTableB1V2.getRecordsNumber(), undecentralizableTableB1NBR);
		try {
			db.getDbwrapper().getTableInstance(TableAlone.class);
			Assert.fail();
		}
		catch (DatabaseException ignored)
		{

		}
	}



	public DatabaseLifeCycles getDatabaseLifeCyclesInstance(final boolean replaceDistantConflictualRecordsWhenDistributedDatabaseIsResynchronized)
	{
		return new DatabaseLifeCycles() {
			@Override
			public void transferDatabaseFromOldVersion(DatabaseWrapper wrapper, DatabaseConfiguration newDatabaseConfiguration) throws Exception {
				Assert.assertEquals(newDatabaseConfiguration.getDatabaseSchema().getPackage(), TableAloneV2.class.getPackage());
				Assert.assertEquals(newDatabaseConfiguration.getDatabaseSchema().getOldSchema().getPackage(), TableAlone.class.getPackage());
				TableAlone tableAlone=wrapper.getTableInstance(TableAlone.class);
				TablePointed tablePointed=wrapper.getTableInstance(TablePointed.class);
				TablePointing tablePointing=wrapper.getTableInstance(TablePointing.class);
				UndecentralizableTableA1 undecentralizableTableA1=wrapper.getTableInstance(UndecentralizableTableA1.class);
				UndecentralizableTableB1 undecentralizableTableB1=wrapper.getTableInstance(UndecentralizableTableB1.class);
				TableAloneV2 tableAloneV2=wrapper.getTableInstance(TableAloneV2.class);
				TablePointedV2 tablePointedV2=wrapper.getTableInstance(TablePointedV2.class);
				TablePointingV2 tablePointingV2=wrapper.getTableInstance(TablePointingV2.class);
				UndecentralizableTableA1V2 undecentralizableTableA1V2=wrapper.getTableInstance(UndecentralizableTableA1V2.class);
				UndecentralizableTableB1V2 undecentralizableTableB1V2=wrapper.getTableInstance(UndecentralizableTableB1V2.class);
				wrapper.runSynchronizedTransaction(new SynchronizedTransaction<Void>() {
					@Override
					public Void run() throws Exception {
						tableAlone.getRecords(new Filter<TableAlone.Record>(){
							@Override
							public boolean nextRecord(TableAlone.Record _record) throws DatabaseException {
								tableAloneV2.addRecord(new TableAloneV2.Record(_record));
								return false;
							}
						});
						tablePointed.getRecords(new Filter<TablePointed.Record>(){
							@Override
							public boolean nextRecord(TablePointed.Record _record) throws DatabaseException {
								tablePointedV2.addRecord(new TablePointedV2.Record(_record));
								return false;
							}
						});
						tablePointing.getRecords(new Filter<TablePointing.Record>(){
							@Override
							public boolean nextRecord(TablePointing.Record _record) throws DatabaseException {
								tablePointingV2.addRecord(new TablePointingV2.Record(_record));
								return false;
							}
						});
						undecentralizableTableA1.getRecords(new Filter<UndecentralizableTableA1.Record>(){
							@Override
							public boolean nextRecord(UndecentralizableTableA1.Record _record) throws DatabaseException {
								undecentralizableTableA1V2.addRecord(new UndecentralizableTableA1V2.Record(_record));
								return false;
							}
						});
						undecentralizableTableB1.getRecords(new Filter<UndecentralizableTableB1.Record>(){
							@Override
							public boolean nextRecord(UndecentralizableTableB1.Record _record) throws DatabaseException {
								undecentralizableTableB1V2.addRecord(new UndecentralizableTableB1V2.Record(_record));
								return false;
							}
						});
						return null;
					}

					@Override
					public TransactionIsolation getTransactionIsolation() {
						return TransactionIsolation.TRANSACTION_SERIALIZABLE;
					}

					@Override
					public boolean doesWriteData() {
						return replaceDistantConflictualRecordsWhenDistributedDatabaseIsResynchronized;
					}

					@Override
					public void initOrReset() {

					}
				});

			}

			@Override
			public void afterDatabaseCreation(DatabaseWrapper wrapper, DatabaseConfiguration newDatabaseConfiguration) {

			}

			@Override
			public boolean hasToRemoveOldDatabase(DatabaseConfiguration databaseConfiguration)  {
				return hasToRemoveOldDatabase;
			}

			@Override
			public boolean replaceDistantConflictualRecordsWhenDistributedDatabaseIsResynchronized(DatabaseConfiguration databaseConfiguration) {
				return false;
			}

			@Override
			public void saveDatabaseConfigurations(DatabaseConfigurations databaseConfigurations) {

			}

		};
	}
}
