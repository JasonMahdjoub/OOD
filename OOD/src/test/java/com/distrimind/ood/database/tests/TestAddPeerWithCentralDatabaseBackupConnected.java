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

import com.distrimind.ood.database.BackupConfiguration;
import com.distrimind.ood.database.CommonDecentralizedTests;
import com.distrimind.ood.database.DatabaseFactory;
import com.distrimind.ood.database.centraldatabaseapi.ClientTable;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.SecureRandomType;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 3.0.0
 */
public abstract class TestAddPeerWithCentralDatabaseBackupConnected extends CommonDecentralizedTests {
	final BackupConfiguration backupConfiguration=new BackupConfiguration(10000, 20000, 1000000, 1000, null);

	@Override
	public BackupConfiguration getBackupConfiguration()
	{
		return backupConfiguration;
	}
	@Override
	public boolean canInitCentralBackup()
	{
		return true;
	}
	@Override
	protected boolean sendIndirectTransactions()
	{
		return true;
	}


	@Test(dependsOnMethods = { "testSynchroAfterTestsBetweenThreePeers" })
	public void addPeer()
			throws Exception {
		connectAllDatabase();
		exchangeMessages();
		checkAllDatabaseInternalDataUsedForSynchro();
		testSynchronisation();
		for (int i=0;i<20;i++)
			addElements();

		DatabaseFactory<?> df= getDatabaseFactoryInstance4();
		df.setEncryptionProfileProviders(signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup, encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup, protectedSignatureProfileProviderForAuthenticatedP2PMessages, SecureRandomType.DEFAULT);
		db4 = new Database(df.getDatabaseWrapperSingleton());
		listDatabase.add(db4);
		addConfiguration(db4);
		Assert.assertTrue(db4.getDbwrapper().getSynchronizer().isInitialized());
		testAllConnect();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();
		testSynchronisation();
	}

	@Test(dependsOnMethods = { "addPeer" })
	public void removePeer() throws Exception {
		disconnectAllDatabase();
		for (int i=0;i<20;i++)
			addElements();
		testAllDisconnected();
		DecentralizedValue hostID4=db4.getHostID();
		ClientTable t=centralDatabaseBackupDatabase.getTableInstance(ClientTable.class);
		Assert.assertTrue(t.hasRecordsWithAllFields("clientID", hostID4));
		listDatabase.get(0).getDbwrapper().getDatabaseConfigurationsBuilder()
				.removeDistantPeer(db4.getHostID())
				.commit();

		unloadDatabase4();
		testAllDisconnected();
		testAllConnect();
		testSynchronisation();
		disconnectAllDatabase();

		testAllConnect();
		testSynchronisation();
		for (CommonDecentralizedTests.Database db : listDatabase) {
			for (CommonDecentralizedTests.Database otherdb : listDatabase) {
				Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized(otherdb.getHostID()));
			}
		}
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro(false);
		Thread.sleep(1200);
		centralDatabaseBackupReceiver.cleanObsoleteData();

		Assert.assertFalse(t.hasRecordsWithAllFields("clientID", hostID4), ""+t.getRecord("clientID", hostID4));
	}
}
