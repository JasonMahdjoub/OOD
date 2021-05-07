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
package com.distrimind.ood.database.tests;

import com.distrimind.ood.database.CommonDecentralizedTests;
import com.distrimind.ood.database.DatabaseEventType;
import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.TableEvent;
import com.distrimind.ood.database.decentralizeddatabase.TableAlone;
import com.distrimind.ood.database.exceptions.DatabaseException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.5
 */
public abstract class TestCentralBackupWithDecentralizedDatabase extends CommonDecentralizedTests {


	public TestCentralBackupWithDecentralizedDatabase() {
		super();
	}

	@Override
	public boolean canInitCentralBackup()
	{
		return true;
	}
	@Override
	protected boolean sendIndirectTransactions()
	{
		return false;
	}

	protected void testSynchroBetweenPeersWithCentralBackupImpl(int peersNumber, boolean exceptionDuringTransaction,
																boolean generateDirectConflict, TableEvent<DatabaseRecord> event)
			throws Exception {
		this.actualGenerateDirectConflict=generateDirectConflict;
		if (peersNumber < 2 || peersNumber > listDatabase.size())
			throw new IllegalArgumentException();
		List<TableEvent<DatabaseRecord>> levents = Collections.singletonList(event);
		ArrayList<CommonDecentralizedTests.Database> l = new ArrayList<>(peersNumber);
		for (int i = 0; i < peersNumber; i++)
			l.add(listDatabase.get(i));
		CommonDecentralizedTests.Database[] concernedDatabase = l.toArray(new Database[0]);



		if (exceptionDuringTransaction) {

			CommonDecentralizedTests.Database db = concernedDatabase[0];
			try {
				proceedEvent(db, true, levents);
				Assert.fail();
			} catch (Exception ignored) {

			}

			Thread.sleep(1200);
			exchangeMessages();

			for (int i = 1; i < peersNumber; i++) {
				db = concernedDatabase[i];
				testEventSynchronized(db, event, false);
			}


		} else {

			if (generateDirectConflict) {
				int i = 0;
				for (CommonDecentralizedTests.Database db : listDatabase) {
					db.setReplaceWhenCollisionDetected(i++ != 0);
					db.resetCollisions();
					db.getAnomalies().clear();
				}
				for (CommonDecentralizedTests.Database db : concernedDatabase) {

					proceedEvent(db, false, clone(levents), true);

				}
				Thread.sleep(1200);
				exchangeMessages();
				i = 0;
				for (CommonDecentralizedTests.Database db : concernedDatabase) {

					//Assert.assertFalse(db.isNewDatabaseEventDetected());

					CommonDecentralizedTests.DetectedCollision dcollision = db.getDetectedCollision();
					if (dcollision!=null) {
						//Assert.assertNotNull(dcollision, "i=" + (i));
						testCollision(db, event, dcollision);

					}
					if (i==0 || (event.getType()!= DatabaseEventType.REMOVE && event.getType()!= DatabaseEventType.REMOVE_WITH_CASCADE))
						Assert.assertTrue(db.getAnomalies().isEmpty(), ""+i+", eventT="+event.getType()+" : "+db.getAnomalies().toString());

					++i;
				}

			} else {


				CommonDecentralizedTests.Database db = concernedDatabase[0];
				proceedEvent(db, false, levents);
				System.out.println("sleep");
				Thread.sleep(1200);

				exchangeMessages();
				Assert.assertTrue(db.getAnomalies().isEmpty());

				for (int i = 1; i < concernedDatabase.length; i++) {
					db = concernedDatabase[i];
					Assert.assertNull(db.getDetectedCollision());
					Assert.assertTrue(db.getAnomalies().isEmpty());
					Assert.assertTrue(db.isNewDatabaseEventDetected());
					testEventSynchronized(db, event, true);

				}

				for (int i = peersNumber; i < listDatabase.size(); i++) {
					db = listDatabase.get(i);
//					testEventSynchronized(db, event, true);
					db.clearPendingEvents();
				}

			}


			for (int i = peersNumber; i < listDatabase.size(); i++) {
				CommonDecentralizedTests.Database db = listDatabase.get(i);
				DetectedCollision collision=db.getDetectedCollision();
				if (!generateDirectConflict)
					Assert.assertNull(collision, "Database N°"+i);
				// DetectedCollision collision=db.getDetectedCollision();
				// Assert.assertNotNull(collision, "Database N°"+i);
				//db.getAnomalies().clear();
				if (event.getType()!= DatabaseEventType.REMOVE && event.getType()!= DatabaseEventType.REMOVE_WITH_CASCADE)
					Assert.assertTrue(db.getAnomalies().isEmpty(), ""+i+", eventT="+event.getType()+" : "+db.getAnomalies().toString());

				//Assert.assertFalse(db.isNewDatabaseEventDetected());
				testEventSynchronized(db, event, true);

			}


		}
		testSynchronisation();
		connectCentralDatabaseBackup();
		checkAllDatabaseInternalDataUsedForSynchro();
	}

	@Test(dependsOnMethods = {"testSynchroAfterTestsBetweenThreePeers" })
	public void connectCentralDatabaseBackup()
			throws Exception {
		connectCentralDatabaseBackupWithConnectedDatabase();
		addDatabasePackageToSynchronizeWithCentralDatabaseBackup(TableAlone.class.getPackage());
		disconnectAllDatabase();
		exchangeMessages();
		disconnectCentralDatabaseBackup();
		connectCentralDatabaseBackupWithConnectedDatabase();
		exchangeMessages();
		for (Database d : listDatabase)
		{
			Assert.assertTrue(d.getDbwrapper().getSynchronizer().isInitializedWithCentralBackup());
			for (Database d2 : listDatabase)
			{
				if (d2!=d)
				{
					Assert.assertTrue(d.getDbwrapper().getSynchronizer().isInitializedWithCentralBackup(d2.getHostID()));
				}
			}

		}

	}



	@Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods = {
			"connectCentralDatabaseBackup" })
	// @Test(dataProvider = "provideDataForSynchroBetweenTwoPeers",
	// dependsOnMethods={"testSynchroBetweenThreePeers2"})
	public void testSynchroBetweenTwoPeersWithCentralBackup(boolean exceptionDuringTransaction, boolean generateDirectConflict,
										   boolean peersInitiallyConnected)
			throws Exception {
		for (TableEvent<DatabaseRecord> event : provideTableEventsForSynchro())
			testSynchroBetweenPeersWithCentralBackupImpl(2, exceptionDuringTransaction, generateDirectConflict, event);
	}

	@Test(dependsOnMethods = { "testSynchroBetweenTwoPeersWithCentralBackup" })
	public void testSynchroAfterTestsBetweenTwoPeersWithCentralBackup() throws DatabaseException {
		testSynchronisation();
	}

	@Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods = { "testSynchroAfterTestsBetweenTwoPeersWithCentralBackup" })
	public void testSynchroBetweenThreePeersWithCentralBackup(boolean exceptionDuringTransaction, boolean generateDirectConflict,
											 boolean peersInitiallyConnected)
			throws Exception {
		for (TableEvent<DatabaseRecord> event : provideTableEventsForSynchro())
			testSynchroBetweenPeersWithCentralBackupImpl(3, exceptionDuringTransaction, generateDirectConflict, event);
	}

	@Test(dependsOnMethods = { "testSynchroBetweenThreePeersWithCentralBackup" })
	public void testSynchroAfterTestsBetweenThreePeersWithCentralBackup() throws DatabaseException {
		testSynchronisation();
	}

	@Test(dependsOnMethods = {"testSynchroAfterTestsBetweenThreePeersWithCentralBackup" })
	public void disconnectCentralDatabaseBackup()
			throws Exception {
		disconnectCentralDatabaseBakcup();
		for (Database d : listDatabase)
		{
			Assert.assertFalse(d.getDbwrapper().getSynchronizer().isInitializedWithCentralBackup());
			for (Database d2 : listDatabase)
			{
				if (d2!=d)
				{
					Assert.assertFalse(d.getDbwrapper().getSynchronizer().isInitialized(d2.getHostID()));
				}
			}

		}

	}
}