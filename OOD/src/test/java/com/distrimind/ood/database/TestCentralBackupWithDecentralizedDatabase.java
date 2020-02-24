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

import com.distrimind.ood.database.DatabaseWrapper.SynchronizationAnomalyType;
import com.distrimind.ood.database.decentralizeddatabase.*;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.DecentralizedIDGenerator;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.ASymmetricAuthenticatedSignatureType;
import com.distrimind.util.crypto.SecureRandomType;
import com.distrimind.util.io.RandomByteArrayInputStream;
import com.distrimind.util.io.RandomByteArrayOutputStream;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.5
 */
public class TestCentralBackupWithDecentralizedDatabase extends CommonDecentralizedTests{

	final String database_file_name1 = "decentralizedDatabaseWithBackup1";
	final String database_file_name2 = "decentralizedDatabaseWithBackup2";
	final String database_file_name3 = "decentralizedDatabaseWithBackup3";
	final String database_file_name4 = "decentralizedDatabaseWithBackup4";

	@Override
	public DatabaseWrapper getDatabaseWrapperInstance1() throws IllegalArgumentException, DatabaseException {
		return new InFileEmbeddedH2DatabaseFactory(new File(database_file_name1)).newWrapperInstance();
	}

	@Override
	public DatabaseWrapper getDatabaseWrapperInstance2() throws IllegalArgumentException, DatabaseException {
		return new InFileEmbeddedH2DatabaseFactory(new File(database_file_name2)).newWrapperInstance();
	}

	@Override
	public DatabaseWrapper getDatabaseWrapperInstance3() throws IllegalArgumentException, DatabaseException {
		return new InFileEmbeddedH2DatabaseFactory(new File(database_file_name3)).newWrapperInstance();
	}

	@Override
	public DatabaseWrapper getDatabaseWrapperInstance4() throws IllegalArgumentException, DatabaseException {
		return new InFileEmbeddedH2DatabaseFactory(new File(database_file_name4)).newWrapperInstance();
	}

	@Override
	public void removeDatabaseFiles1() {
		EmbeddedH2DatabaseWrapper.deleteDatabaseFiles(new File(database_file_name1 ));

	}

	@Override
	public void removeDatabaseFiles2() {
		EmbeddedH2DatabaseWrapper.deleteDatabaseFiles(new File(database_file_name2 ));
	}

	@Override
	public void removeDatabaseFiles3() {
		EmbeddedH2DatabaseWrapper.deleteDatabaseFiles(new File(database_file_name3 ));
	}

	@Override
	public void removeDatabaseFiles4() {
		EmbeddedH2DatabaseWrapper.deleteDatabaseFiles(new File(database_file_name4 ));
	}


	@Test(dependsOnMethods = { "testAllConnect" })
	public void testOldElementsAddedBeforeAddingSynchroSynchronized()
			throws Exception {
		exchangeMessages();
		testSynchronisation();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();

	}


	@Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods = {
			"testOldElementsAddedBeforeAddingSynchroSynchronized" })
	// @Test(dataProvider = "provideDataForSynchroBetweenTwoPeers",
	// dependsOnMethods={"testSynchroBetweenThreePeers2"})
	public void testSynchroBetweenTwoPeers(boolean exceptionDuringTransaction, boolean generateDirectConflict,
										   boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenPeers(2, exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dependsOnMethods = { "testSynchroBetweenTwoPeers" })
	public void testSynchroAfterTestsBetweenTwoPeers() throws DatabaseException {
		testSynchronisation();
	}



	@Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods = { "testSynchroBetweenTwoPeers" })
	public void testSynchroBetweenThreePeers(boolean exceptionDuringTransaction, boolean generateDirectConflict,
											 boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenPeers(3, exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dependsOnMethods = { "testSynchroBetweenThreePeers" })
	public void testSynchroAfterTestsBetweenThreePeers() throws DatabaseException {
		testSynchronisation();
	}

	@DataProvider(name = "provideDataForIndirectSynchro")
	public Object[][] provideDataForIndirectSynchro() throws DatabaseException {
		int numberEvents = 40;
		Object[][] res = new Object[numberEvents * 2 * 2 * 2][];
		int index = 0;
		for (boolean generateDirectConflict : new boolean[] { true, false }) {
			boolean[] ict = generateDirectConflict ? new boolean[]{true} : new boolean[]{false};
			for (boolean peersInitiallyConnected : ict) {
				for (TableEvent<DatabaseRecord> te : provideTableEvents(numberEvents)) {
					res[index++] = new Object[] {generateDirectConflict,
							peersInitiallyConnected, te };
				}
			}
		}
		return res;

	}

	@Test(dataProvider = "provideDataForIndirectSynchro", dependsOnMethods = {
			"testSynchroAfterTestsBetweenThreePeers" })
	public void testIndirectSynchro(boolean generateDirectConflict, boolean peersInitiallyConnected,
									TableEvent<DatabaseRecord> event) throws Exception {
		List<TableEvent<DatabaseRecord>> levents = Collections.singletonList(event);
		final CommonDecentralizedTests.Database[] indirectDatabase = new CommonDecentralizedTests.Database[] { listDatabase.get(0), listDatabase.get(2) };
		final CommonDecentralizedTests.Database[] segmentA = new CommonDecentralizedTests.Database[] { listDatabase.get(0), listDatabase.get(1) };
		final CommonDecentralizedTests.Database[] segmentB = new CommonDecentralizedTests.Database[] { listDatabase.get(1), listDatabase.get(2) };
		if (generateDirectConflict) {
			int i = 0;
			for (CommonDecentralizedTests.Database db : indirectDatabase)// TODO test with opposite direction
			{
				if (i++ == 0) {
					db.setReplaceWhenCollisionDetected(false);
				} else {
					db.setReplaceWhenCollisionDetected(true);
				}

				proceedEvent(db, false, levents);
			}
			listDatabase.get(1).setReplaceWhenCollisionDetected(false);
			connectSelectedDatabase(segmentA);
			exchangeMessages();

			CommonDecentralizedTests.Database db = listDatabase.get(1);

			Assert.assertNull(db.getDetectedCollision());

			Assert.assertTrue(db.isNewDatabaseEventDetected());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());
			db.setNewDatabaseEventDetected(false);

			disconnectSelectedDatabase(segmentA);
			cleanPendedEvents();

			connectSelectedDatabase(segmentB);
			exchangeMessages();

			CommonDecentralizedTests.DetectedCollision dcollision = db.getDetectedCollision();

			testCollision(db, event, dcollision);
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());
			db = listDatabase.get(2);

			CommonDecentralizedTests.DetectedCollision collision = db.getDetectedCollision();
			testCollision(db, event, collision);
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.isNewDatabaseEventDetected());
			Assert.assertTrue(db.getAnomalies().isEmpty());

			disconnectSelectedDatabase(segmentB);
			cleanPendedEvents();

			connectSelectedDatabase(segmentB);
			exchangeMessages();

			db = listDatabase.get(1);

			Assert.assertNull(db.getDetectedCollision());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());
			if (listDatabase.size() == 3) {
				Assert.assertEquals(db.getDbwrapper().getDatabaseDistantTransactionEvent().getRecords().size(), 0);
				Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionsPerHostTable().getRecords().size(), 0);
				Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionEventsTable().getRecords().size(), 0);
			}

			db = listDatabase.get(2);

			Assert.assertNull(db.getDetectedCollision());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());
			if (listDatabase.size() == 3) {
				Assert.assertEquals(db.getDbwrapper().getDatabaseDistantTransactionEvent().getRecords().size(), 0);
				Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionsPerHostTable().getRecords().size(), 0);
				Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionEventsTable().getRecords().size(), 0);
			}

			disconnectSelectedDatabase(segmentB);
			cleanPendedEvents();

			connectSelectedDatabase(segmentA);
			exchangeMessages();

			db = listDatabase.get(0);

			Assert.assertNull(db.getDetectedCollision());
			// Assert.assertFalse(db.isNewDatabaseEventDetected());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

			disconnectSelectedDatabase(segmentA);
			cleanPendedEvents();
			// checkAllDatabaseInternalDataUsedForSynchro();
			connectSelectedDatabase(segmentB);
			exchangeMessages();

			db = listDatabase.get(2);

			Assert.assertNull(db.getDetectedCollision());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.isNewDatabaseEventDetected());
			Assert.assertTrue(db.getAnomalies().isEmpty());

			db = listDatabase.get(1);

			Assert.assertNull(db.getDetectedCollision());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

			disconnectSelectedDatabase(segmentB);
			cleanPendedEvents();
			// TODO make possible this test here :
			// checkAllDatabaseInternalDataUsedForSynchro();
		} else {
			if (peersInitiallyConnected)
				connectSelectedDatabase(segmentA);

			CommonDecentralizedTests.Database db = listDatabase.get(0);
			proceedEvent(db, false, levents);

			if (!peersInitiallyConnected)
				connectSelectedDatabase(segmentA);

			exchangeMessages();
			Assert.assertTrue(db.getAnomalies().isEmpty());

			db = listDatabase.get(1);
			Assert.assertNull(db.getDetectedCollision());
			Assert.assertTrue(db.isNewDatabaseEventDetected());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

			disconnectSelectedDatabase(segmentA);
			connectSelectedDatabase(segmentB);
			exchangeMessages();

			db = listDatabase.get(2);
			Assert.assertNull(db.getDetectedCollision());
			Assert.assertTrue(db.isNewDatabaseEventDetected());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());
			disconnectSelectedDatabase(segmentB);
		}

		connectAllDatabase();
		exchangeMessages();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();

	}

	@Test(dependsOnMethods = { "testIndirectSynchro" })
	public void testSynchroAfterIndirectTestsBetweenPeers() throws DatabaseException {
		testSynchronisation();
	}

	@DataProvider(name = "provideDataForIndirectSynchroWithIndirectConnection")
	public Object[][] provideDataForIndirectSynchroWithIndirectConnection() throws DatabaseException {
		return provideDataForIndirectSynchro();
	}

	@Test(dataProvider = "provideDataForIndirectSynchroWithIndirectConnection", dependsOnMethods = {
			"testIndirectSynchro" })
	// @Test(dataProvider = "provideDataForIndirectSynchroWithIndirectConnection",
	// dependsOnMethods={"testOldElementsAddedBeforeAddingSynchroSynchronized"})
	public void testIndirectSynchroWithIndirectConnection(boolean generateDirectConflict,
														  boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		List<TableEvent<DatabaseRecord>> levents = Collections.singletonList(event);
		final CommonDecentralizedTests.Database[] indirectDatabase = new CommonDecentralizedTests.Database[] { listDatabase.get(0), listDatabase.get(2) };
		final CommonDecentralizedTests.Database[] segmentA = new CommonDecentralizedTests.Database[] { listDatabase.get(0), listDatabase.get(1) };
		final CommonDecentralizedTests.Database[] segmentB = new CommonDecentralizedTests.Database[] { listDatabase.get(1), listDatabase.get(2) };
		if (generateDirectConflict) {
			int i = 0;
			for (CommonDecentralizedTests.Database db : indirectDatabase) {
				if (i++ == 0)
					db.setReplaceWhenCollisionDetected(false);
				else
					db.setReplaceWhenCollisionDetected(true);
				proceedEvent(db, false, levents);
			}
			segmentA[1].setReplaceWhenCollisionDetected(false);
			connectSelectedDatabase(segmentA);
			connectSelectedDatabase(segmentB);
			exchangeMessages();

			CommonDecentralizedTests.Database db = listDatabase.get(0);
			// Assert.assertFalse(db.isNewDatabaseEventDetected());
			Assert.assertNull(db.getDetectedCollision());

			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

			db = listDatabase.get(1);
			testCollision(db, event, db.getDetectedCollision());
			// Assert.assertTrue(db.isNewDatabaseEventDetected());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

			db = listDatabase.get(2);

			// Assert.assertFalse(db.isNewDatabaseEventDetected());
			testCollision(db, event, db.getDetectedCollision());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());
			// TODO make possible this test here :
			// checkAllDatabaseInternalDataUsedForSynchro();
		} else {
			if (peersInitiallyConnected) {
				connectSelectedDatabase(segmentA);
				connectSelectedDatabase(segmentB);
			}

			CommonDecentralizedTests.Database db = listDatabase.get(0);
			proceedEvent(db, false, levents);

			if (!peersInitiallyConnected) {
				connectSelectedDatabase(segmentA);
				connectSelectedDatabase(segmentB);
			}

			exchangeMessages();
			Assert.assertTrue(db.getAnomalies().isEmpty());

			db = listDatabase.get(1);
			Assert.assertNull(db.getDetectedCollision());
			Assert.assertTrue(db.isNewDatabaseEventDetected());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

			db = listDatabase.get(2);
			Assert.assertNull(db.getDetectedCollision());
			Assert.assertTrue(db.isNewDatabaseEventDetected());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

		}
		disconnectSelectedDatabase(segmentA);
		disconnectSelectedDatabase(segmentB);

		connectAllDatabase();
		exchangeMessages();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();

	}

	@Test(dependsOnMethods = { "testIndirectSynchroWithIndirectConnection" })
	public void testSynchroAfterPostIndirectTestsBetweenPeers() throws DatabaseException {
		testSynchronisation();
	}





	@Test(dataProvider = "provideDataForTransactionBetweenTwoPeers", dependsOnMethods = {
			"testSynchroAfterPostIndirectTestsBetweenPeers" })
	public void testTransactionBetweenTwoPeers(boolean peersInitiallyConnected,
											   List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionBetweenPeers(2, peersInitiallyConnected, levents, false);
	}



	@Test(dataProvider = "provideDataForTransactionBetweenThreePeers", dependsOnMethods = {
			"testTransactionBetweenTwoPeers" })
	public void testTransactionBetweenThreePeers(boolean peersInitiallyConnected,
												 List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionBetweenPeers(3, peersInitiallyConnected, levents, false);
	}


	@Test(dependsOnMethods = { "testTransactionBetweenThreePeers" })
	public void preTestTransactionSynchros() throws Exception {
		CommonDecentralizedTests.Database[] concernedDatabase = new CommonDecentralizedTests.Database[3];
		concernedDatabase[0] = listDatabase.get(0);
		concernedDatabase[1] = listDatabase.get(1);
		concernedDatabase[2] = listDatabase.get(2);

		connectSelectedDatabase(concernedDatabase);
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods = {
			"preTestTransactionSynchros" }, invocationCount = 4)
	public void testTransactionSynchros(List<TableEvent<DatabaseRecord>> levents)
			throws Exception {
		testTransactionBetweenPeers(3, true, levents, true);
	}

	@Test(dependsOnMethods = { "testTransactionSynchros" })
	public void postTestTransactionSynchros() throws Exception {
		CommonDecentralizedTests.Database[] concernedDatabase = new CommonDecentralizedTests.Database[3];
		concernedDatabase[0] = listDatabase.get(0);
		concernedDatabase[1] = listDatabase.get(1);
		concernedDatabase[2] = listDatabase.get(2);
		disconnectSelectedDatabase(concernedDatabase);

		connectAllDatabase();
		exchangeMessages();
		disconnectAllDatabase();
		testSynchronisation();

		checkAllDatabaseInternalDataUsedForSynchro();
	}

	@DataProvider(name = "provideDataForTransactionSynchrosWithIndirectConnection")
	public Object[][] provideDataForTransactionSynchrosWithIndirectConnection() throws DatabaseException {
		return provideDataForTransactionBetweenTwoPeers();
	}

	@DataProvider(name = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", parallel = true)
	public Object[][] provideDataForTransactionSynchrosWithIndirectConnectionThreaded() throws DatabaseException {
		int numberTransactions = 40;
		Object[][] res = new Object[numberTransactions][];
		int index = 0;
		for (int i = 0; i < numberTransactions; i++) {
			res[index++] = new Object[] { provideTableEvents((int) (5.0 + Math.random() * 10.0)) };

		}

		return res;

	}



	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnection", dependsOnMethods = {
			"postTestTransactionSynchros" })
	public void testTransactionSynchrosWithIndirectConnection(boolean peersInitiallyConnected,
															  List<TableEvent<DatabaseRecord>> levents) throws Exception {
		synchronized (TestDecentralizedDatabase.class) {
			testTransactionSynchrosWithIndirectConnection(peersInitiallyConnected, levents, false);
		}
	}

	@Test(dependsOnMethods = { "testTransactionSynchrosWithIndirectConnection" })
	public void preTestTransactionSynchrosWithIndirectConnectionThreaded()
			throws Exception {
		testSynchronisation();
		final CommonDecentralizedTests.Database[] segmentA = new CommonDecentralizedTests.Database[] { listDatabase.get(0), listDatabase.get(1) };
		final CommonDecentralizedTests.Database[] segmentB = new CommonDecentralizedTests.Database[] { listDatabase.get(1), listDatabase.get(2) };
		connectSelectedDatabase(segmentA);
		connectSelectedDatabase(segmentB);

	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods = {
			"preTestTransactionSynchrosWithIndirectConnectionThreaded" })
	public void testTransactionSynchrosWithIndirectConnectionThreaded(List<TableEvent<DatabaseRecord>> levents)
			throws Exception {
		testTransactionSynchrosWithIndirectConnection(true, levents, true);
	}

	@Test(dependsOnMethods = { "testTransactionSynchrosWithIndirectConnectionThreaded" })
	public void postTestTransactionSynchrosWithIndirectConnectionThreaded()
			throws Exception {
		final CommonDecentralizedTests.Database[] segmentA = new CommonDecentralizedTests.Database[] { listDatabase.get(0), listDatabase.get(1) };
		final CommonDecentralizedTests.Database[] segmentB = new CommonDecentralizedTests.Database[] { listDatabase.get(1), listDatabase.get(2) };
		exchangeMessages();
		disconnectSelectedDatabase(segmentA);
		disconnectSelectedDatabase(segmentB);
		disconnectAllDatabase();
		connectAllDatabase();
		exchangeMessages();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();
		testSynchronisation();

	}

	@Test(dependsOnMethods = { "postTestTransactionSynchrosWithIndirectConnectionThreaded" })
	public void testSynchroTransactionTests() throws DatabaseException {
		testSynchronisation();
	}

	@Test(dependsOnMethods = { "testSynchroTransactionTests" })
	public void addNewPeer() throws Exception {
		// TODO add new peer a second time at the end of these tests
		connectAllDatabase();
		testSynchronisation();
		disconnectAllDatabase();
		db4 = new CommonDecentralizedTests.Database(getDatabaseWrapperInstance4());
		listDatabase.add(db4);

		db4.getDbwrapper().getSynchronizer().setNotifier(db4);
		db4.getDbwrapper().setMaxTransactionsToSynchronizeAtTheSameTime(5);
		db4.getDbwrapper().setMaxTransactionEventsKeepedIntoMemory(3);
		db4.getDbwrapper().getSynchronizer().addHookForLocalDatabaseHost(db4.getHostID(),
				TablePointed.class.getPackage());
		Assert.assertTrue(db4.getDbwrapper().getSynchronizer().isInitialized());

		for (CommonDecentralizedTests.Database other : listDatabase) {
			if (other != db4) {
				HookAddRequest har = db4.getDbwrapper().getSynchronizer().askForHookAddingAndSynchronizeDatabase(
						other.getHostID(), false, TablePointed.class.getPackage());
				har = other.getDbwrapper().getSynchronizer().receivedHookAddRequest(har);
				db4.getDbwrapper().getSynchronizer().receivedHookAddRequest(har);
			}
		}

		testAllConnect();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();
		testSynchronisation();

	}

	@Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods = { "addNewPeer" })
	public void testSynchroBetweenTwoPeers2(boolean exceptionDuringTransaction, boolean generateDirectConflict,
											boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenTwoPeers(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods = { "testSynchroBetweenTwoPeers2" })
	public void testSynchroBetweenThreePeers2(boolean exceptionDuringTransaction, boolean generateDirectConflict,
											  boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenThreePeers(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected,
				event);
	}

	@Test(dataProvider = "provideDataForIndirectSynchro", dependsOnMethods = { "testSynchroBetweenThreePeers2" })
	public void testIndirectSynchro2(boolean generateDirectConflict, boolean peersInitiallyConnected,
									 TableEvent<DatabaseRecord> event) throws Exception {
		testIndirectSynchro(generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataForIndirectSynchroWithIndirectConnection", dependsOnMethods = {
			"testIndirectSynchro2" })
	public void testIndirectSynchroWithIndirectConnection2(boolean generateDirectConflict,
														   boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testIndirectSynchroWithIndirectConnection(generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataForTransactionBetweenTwoPeers", dependsOnMethods = {
			"testIndirectSynchroWithIndirectConnection2" })
	public void testTransactionBetweenTwoPeers2(boolean peersInitiallyConnected,
												List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionBetweenTwoPeers(peersInitiallyConnected, levents);
	}

	@Test(dataProvider = "provideDataForTransactionBetweenThreePeers", dependsOnMethods = {
			"testTransactionBetweenTwoPeers2" })
	public void testTransactionBetweenThreePeers2(boolean peersInitiallyConnected,
												  List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionBetweenThreePeers(peersInitiallyConnected, levents);
	}

	@Test(dependsOnMethods = { "testTransactionBetweenThreePeers2" })
	public void preTestTransactionSynchros2() throws Exception {
		preTestTransactionSynchros();
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods = {
			"preTestTransactionSynchros2" }, invocationCount = 4)
	public void testTransactionSynchros2(List<TableEvent<DatabaseRecord>> levents)
			throws Exception {
		testTransactionSynchros(levents);
	}

	@Test(dependsOnMethods = { "testTransactionSynchros2" })
	public void postTestTransactionSynchros2() throws Exception {
		postTestTransactionSynchros();
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnection", dependsOnMethods = {
			"postTestTransactionSynchros2" })
	public void testTransactionSynchrosWithIndirectConnection2(boolean peersInitiallyConnected,
															   List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionSynchrosWithIndirectConnection(peersInitiallyConnected, levents);
	}

	@Test(dependsOnMethods = { "testTransactionSynchrosWithIndirectConnection2" })
	public void preTestTransactionSynchrosWithIndirectConnectionThreaded2()
			throws Exception {
		preTestTransactionSynchrosWithIndirectConnectionThreaded();
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods = {
			"preTestTransactionSynchrosWithIndirectConnectionThreaded2" })
	public void testTransactionSynchrosWithIndirectConnectionThreaded2(List<TableEvent<DatabaseRecord>> levents)
			throws Exception {
		testTransactionSynchrosWithIndirectConnectionThreaded(levents);
	}

	@Test(dependsOnMethods = { "testTransactionSynchrosWithIndirectConnectionThreaded2" })
	public void postTestTransactionSynchrosWithIndirectConnectionThreaded2()
			throws Exception {
		postTestTransactionSynchrosWithIndirectConnectionThreaded();
	}

	@Test(dependsOnMethods = { "postTestTransactionSynchrosWithIndirectConnectionThreaded2" })
	public void testSynchroTransactionTests2() throws DatabaseException {
		testSynchronisation();
	}

	@Test(dependsOnMethods = { "testSynchroTransactionTests2" })
	public void removeNewPeer() throws Exception {

		for (CommonDecentralizedTests.Database other : listDatabase) {
			if (db4 != other) {
				other.getDbwrapper().getSynchronizer().removeHook(db4.getHostID(), TableAlone.class.getPackage());
				db4.getDbwrapper().getSynchronizer().removeHook(other.getHostID(), TableAlone.class.getPackage());
			}
		}
		db4.getDbwrapper().getSynchronizer().removeHook(db4.getHostID(), TableAlone.class.getPackage());

		unloadDatabase4();

		testAllConnect();
		testSynchronisation();
		disconnectAllDatabase();

	}

	@Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods = { "removeNewPeer" })
	public void testSynchroBetweenTwoPeers3(boolean exceptionDuringTransaction, boolean generateDirectConflict,
											boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenTwoPeers(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods = { "testSynchroBetweenTwoPeers3" })
	public void testSynchroBetweenThreePeers3(boolean exceptionDuringTransaction, boolean generateDirectConflict,
											  boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenThreePeers(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected,
				event);
	}

	@Test(dataProvider = "provideDataForIndirectSynchro", dependsOnMethods = { "testSynchroBetweenThreePeers3" })
	public void testIndirectSynchro3(boolean generateDirectConflict, boolean peersInitiallyConnected,
									 TableEvent<DatabaseRecord> event) throws Exception {
		testIndirectSynchro(generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataForIndirectSynchroWithIndirectConnection", dependsOnMethods = {
			"testIndirectSynchro3" })
	public void testIndirectSynchroWithIndirectConnection3(boolean generateDirectConflict,
														   boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testIndirectSynchroWithIndirectConnection(generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataForTransactionBetweenTwoPeers", dependsOnMethods = {
			"testIndirectSynchroWithIndirectConnection3" })
	public void testTransactionBetweenTwoPeers3(boolean peersInitiallyConnected,
												List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionBetweenTwoPeers(peersInitiallyConnected, levents);
	}

	@Test(dataProvider = "provideDataForTransactionBetweenThreePeers", dependsOnMethods = {
			"testTransactionBetweenTwoPeers3" })
	public void testTransactionBetweenThreePeers3(boolean peersInitiallyConnected,
												  List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionBetweenThreePeers(peersInitiallyConnected, levents);
	}

	@Test(dependsOnMethods = { "testTransactionBetweenThreePeers3" })
	public void preTestTransactionSynchros3() throws Exception {
		preTestTransactionSynchros();
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods = {
			"preTestTransactionSynchros3" }, invocationCount = 4)
	public void testTransactionSynchros3(List<TableEvent<DatabaseRecord>> levents)
			throws Exception {
		testTransactionSynchros(levents);
	}

	@Test(dependsOnMethods = { "testTransactionSynchros3" })
	public void postTestTransactionSynchros3() throws Exception {
		postTestTransactionSynchros();
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnection", dependsOnMethods = {
			"postTestTransactionSynchros3" })
	public void testTransactionSynchrosWithIndirectConnection3(boolean peersInitiallyConnected,
															   List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionSynchrosWithIndirectConnection(peersInitiallyConnected, levents);
	}

	@Test(dependsOnMethods = { "testTransactionSynchrosWithIndirectConnection3" })
	public void preTestTransactionSynchrosWithIndirectConnectionThreaded3()
			throws Exception {
		preTestTransactionSynchrosWithIndirectConnectionThreaded();
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods = {
			"preTestTransactionSynchrosWithIndirectConnectionThreaded3" })
	public void testTransactionSynchrosWithIndirectConnectionThreaded3(List<TableEvent<DatabaseRecord>> levents)
			throws Exception {
		testTransactionSynchrosWithIndirectConnectionThreaded(levents);
	}

	@Test(dependsOnMethods = { "testTransactionSynchrosWithIndirectConnectionThreaded3" })
	public void postTestTransactionSynchrosWithIndirectConnectionThreaded3()
			throws Exception {
		postTestTransactionSynchrosWithIndirectConnectionThreaded();
	}

	@Test(dependsOnMethods = { "postTestTransactionSynchrosWithIndirectConnectionThreaded3" })
	public void testSynchroTransactionTests3() throws DatabaseException {
		testSynchronisation();
	}



	public void testSynchroBetweenPeersAfterRestoration(int nbPeers, boolean peersInitiallyConnected,
														List<TableEvent<DatabaseRecord>> levents) throws Exception {
		ArrayList<TableEvent<DatabaseRecord>> events1=new ArrayList<>();
		for (int i=0;i<levents.size()/2;i++)
			events1.add(levents.get(i));

		ArrayList<TableEvent<DatabaseRecord>> events2=new ArrayList<>();
		for (int i=levents.size()/2;i<levents.size();i++)
			events2.add(levents.get(i));

		CommonDecentralizedTests.Database db=listDatabase.get(0);
		BackupRestoreManager manager=db.getDbwrapper().getExternalBackupRestoreManager(TestDatabaseBackupRestore.externalBackupDirectory, TablePointed.class.getPackage(), TestDatabaseBackupRestore.getBackupConfiguration());
		if (nbPeers==2)
			testTransactionBetweenTwoPeers(peersInitiallyConnected, events1);
		else
			testTransactionBetweenThreePeers(peersInitiallyConnected, events1);
		disconnectAllDatabase();
		manager.createBackupReference();
		Thread.sleep(50);
		long backupUTC=System.currentTimeMillis();
		Thread.sleep(50);
		if (nbPeers==2)
			testTransactionBetweenTwoPeers(peersInitiallyConnected, events2);
		else
			testTransactionBetweenThreePeers(peersInitiallyConnected, events2);

		manager.restoreDatabaseToDateUTC(backupUTC);


		connectAllDatabase();
		exchangeMessages();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();
	}

	@Test(dataProvider = "provideDataForTransactionBetweenTwoPeersForRestorationTests", dependsOnMethods = {
			"testSynchroTransactionTests3" })
	public void testSynchroBetweenTwoPeersAfterRestoration(boolean peersInitiallyConnected,
														   List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testSynchroBetweenPeersAfterRestoration(2, peersInitiallyConnected, levents);
	}

	@Test(dataProvider = "provideDataForTransactionBetweenThreePeersForRestorationTests", dependsOnMethods = {
			"testSynchroBetweenTwoPeersAfterRestoration" })
	public void testSynchroBetweenThreePeersAfterRestoration(boolean peersInitiallyConnected,
															 List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testSynchroBetweenPeersAfterRestoration(3, peersInitiallyConnected, levents);
	}

}
