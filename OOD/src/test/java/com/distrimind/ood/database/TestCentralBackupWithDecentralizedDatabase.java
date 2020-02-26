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
public class TestCentralBackupWithDecentralizedDatabase extends CommonDecentralizedTests {

	final String database_file_name1 = "decentralizedDatabaseWithBackup1";
	final String database_file_name2 = "decentralizedDatabaseWithBackup2";
	final String database_file_name3 = "decentralizedDatabaseWithBackup3";
	final String database_file_name4 = "decentralizedDatabaseWithBackup4";
	final BackupConfiguration backupConfiguration=new BackupConfiguration(10000, 20000, 1000000, 1000, null);

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
		EmbeddedH2DatabaseWrapper.deleteDatabaseFiles(new File(database_file_name1));

	}

	@Override
	public void removeDatabaseFiles2() {
		EmbeddedH2DatabaseWrapper.deleteDatabaseFiles(new File(database_file_name2));
	}

	@Override
	public void removeDatabaseFiles3() {
		EmbeddedH2DatabaseWrapper.deleteDatabaseFiles(new File(database_file_name3));
	}

	@Override
	public void removeDatabaseFiles4() {
		EmbeddedH2DatabaseWrapper.deleteDatabaseFiles(new File(database_file_name4));
	}

	@Override
	public BackupConfiguration getBackupConfiguration() {
		return backupConfiguration;
	}

	public boolean canInitCentralBackup()
	{
		return true;
	}

	protected void testSynchroBetweenPeersWithCentralBackup(int peersNumber, boolean exceptionDuringTransaction,
										   boolean generateDirectConflict, TableEvent<DatabaseRecord> event)
			throws Exception {
		if (peersNumber < 2 || peersNumber > listDatabase.size())
			throw new IllegalArgumentException();
		List<TableEvent<DatabaseRecord>> levents = Collections.singletonList(event);
		ArrayList<CommonDecentralizedTests.Database> l = new ArrayList<>(peersNumber);
		for (int i = 0; i < peersNumber; i++)
			l.add(listDatabase.get(i));
		CommonDecentralizedTests.Database[] concernedDatabase = new CommonDecentralizedTests.Database[l.size()];
		for (int i = 0; i < l.size(); i++)
			concernedDatabase[i] = l.get(i);

		if (exceptionDuringTransaction) {

			CommonDecentralizedTests.Database db = concernedDatabase[0];
			try {
				proceedEvent(db, true, levents);
				Assert.fail();
			} catch (Exception ignored) {

			}

			Thread.sleep(1200);
			checkForNewDatabaseBackupBlocks();
			exchangeMessages();

			for (int i = 1; i < peersNumber; i++) {
				db = concernedDatabase[i];
				testEventSynchronized(db, event, false);
			}


		} else {

			if (generateDirectConflict) {
				int i = 0;
				for (CommonDecentralizedTests.Database db : concernedDatabase) {
					if (i++ == 0) {
						db.setReplaceWhenCollisionDetected(false);
					} else {
						db.setReplaceWhenCollisionDetected(true);
					}
					proceedEvent(db, false, clone(levents), true);

				}
				Thread.sleep(1200);
				checkForNewDatabaseBackupBlocks();
				exchangeMessages();
				i = 0;
				for (CommonDecentralizedTests.Database db : concernedDatabase) {
					Assert.assertTrue(db.isNewDatabaseEventDetected());

					CommonDecentralizedTests.DetectedCollision dcollision = db.getDetectedCollision();
					Assert.assertNotNull(dcollision, "i=" + (i));
					testCollision(db, event, dcollision);
					Assert.assertTrue(db.getAnomalies().isEmpty(), db.getAnomalies().toString());

					++i;
				}

			} else {


				CommonDecentralizedTests.Database db = concernedDatabase[0];
				proceedEvent(db, false, levents);

				Thread.sleep(1200);
				checkForNewDatabaseBackupBlocks();

				exchangeMessages();
				Assert.assertTrue(db.getAnomalies().isEmpty());

				for (int i = 1; i < concernedDatabase.length; i++) {
					db = concernedDatabase[i];
					Assert.assertNull(db.getDetectedCollision());
					Assert.assertTrue(db.getAnomalies().isEmpty());
					Assert.assertTrue(db.isNewDatabaseEventDetected());
					testEventSynchronized(db, event, true);

				}

			}

			for (int i = peersNumber; i < listDatabase.size(); i++) {
				CommonDecentralizedTests.Database db = listDatabase.get(i);
				testEventSynchronized(db, event, false);
				db.clearPendingEvents();
			}

			Thread.sleep(1200);
			checkForNewDatabaseBackupBlocks();
			exchangeMessages();

			for (int i = peersNumber; i < listDatabase.size(); i++) {
				CommonDecentralizedTests.Database db = listDatabase.get(i);
				// DetectedCollision collision=db.getDetectedCollision();
				// Assert.assertNotNull(collision, "Database N°"+i);
				Assert.assertTrue(db.getAnomalies().isEmpty());
				Assert.assertTrue(db.isNewDatabaseEventDetected());
				testEventSynchronized(db, event, true);

			}


		}
		testSynchronisation();
		checkAllDatabaseInternalDataUsedForSynchro();

	}

	@Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods = {
			"testSynchroBetweenTwoPeers" })
	// @Test(dataProvider = "provideDataForSynchroBetweenTwoPeers",
	// dependsOnMethods={"testSynchroBetweenThreePeers2"})
	public void testSynchroBetweenTwoPeersWithCentralBackup(boolean exceptionDuringTransaction, boolean generateDirectConflict,
										   boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenPeersWithCentralBackup(2, exceptionDuringTransaction, peersInitiallyConnected, event);
	}

	@Test(dependsOnMethods = { "testSynchroBetweenTwoPeersWithCentralBackup" })
	public void testSynchroAfterTestsBetweenTwoPeersWithCentralBackup() throws DatabaseException {
		testSynchronisation();
	}

	@Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods = { "testSynchroAfterTestsBetweenTwoPeersWithCentralBackup" })
	public void testSynchroBetweenThreePeersWithCentralBackup(boolean exceptionDuringTransaction, boolean generateDirectConflict,
											 boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenPeersWithCentralBackup(3, exceptionDuringTransaction, generateDirectConflict, event);
	}

	@Test(dependsOnMethods = { "testSynchroBetweenThreePeersWithCentralBackup" })
	public void testSynchroAfterTestsBetweenThreePeersWithCentralBackup() throws DatabaseException {
		testSynchronisation();
	}
}