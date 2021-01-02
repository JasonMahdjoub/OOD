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
package com.distrimind.ood.database.centraldatabaseapi;

import com.distrimind.ood.database.*;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.messages.DistantBackupCenterConnexionInitialisation;
import com.distrimind.ood.database.messages.MessageDestinedToCentralDatabaseBackup;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.Integrity;

import java.io.IOException;
import java.util.*;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public abstract class CentralDatabaseBackupReceiver {
	private final Map<DecentralizedValue, CentralDatabaseBackupReceiverPerPeer> receiversPerPeer=new HashMap<>();
	private final DatabaseWrapper wrapper;
	private final DecentralizedValue centralID;
	protected final ClientTable clientTable;
	protected final LastValidatedDistantIDPerClientTable lastValidatedDistantIDPerClientTable;
	protected final DatabaseBackupPerClientTable databaseBackupPerClientTable;
	protected final EncryptedBackupPartReferenceTable encryptedBackupPartReferenceTable;
	protected final ClientCloudAccountTable clientCloudAccountTable;
	protected final ConnectedClientsTable connectedClientsTable;
	private static final Set<Class<?>> listTableClasses=new HashSet<>(Arrays.asList(
			ClientCloudAccountTable.class,
			ClientTable.class,
			ConnectedClientsTable.class,
			DatabaseBackupPerClientTable.class,
			EncryptedBackupPartReferenceTable.class,
			LastValidatedDistantIDPerClientTable.class));



	public CentralDatabaseBackupReceiver(DatabaseWrapper wrapper, DecentralizedValue centralID) throws DatabaseException {
		if (wrapper==null)
			throw new NullPointerException();
		if (centralID==null)
			throw new NullPointerException();
		this.wrapper = wrapper;
		this.centralID=centralID;
		wrapper.getDatabaseConfigurationsBuilder()
				.addConfiguration(new DatabaseConfiguration(new DatabaseSchema(CentralDatabaseBackupReceiver.class.getPackage(), listTableClasses)), false, true )
				.commit();
		this.clientTable=wrapper.getTableInstance(ClientTable.class);
		this.clientCloudAccountTable=wrapper.getTableInstance(ClientCloudAccountTable.class);
		this.lastValidatedDistantIDPerClientTable=wrapper.getTableInstance(LastValidatedDistantIDPerClientTable.class);
		this.encryptedBackupPartReferenceTable=wrapper.getTableInstance(EncryptedBackupPartReferenceTable.class);
		this.databaseBackupPerClientTable=wrapper.getTableInstance(DatabaseBackupPerClientTable.class);
		this.connectedClientsTable=wrapper.getTableInstance(ConnectedClientsTable.class);
		disconnectAllPeers();
	}

	protected abstract CentralDatabaseBackupReceiverPerPeer newCentralDatabaseBackupReceiverPerPeerInstance(DatabaseWrapper wrapper);

	public Integrity received(MessageDestinedToCentralDatabaseBackup message) throws DatabaseException, IOException {
		CentralDatabaseBackupReceiverPerPeer r=receiversPerPeer.get(message.getHostSource());
		if (r==null)
		{
			if (message instanceof DistantBackupCenterConnexionInitialisation) {
				r = newCentralDatabaseBackupReceiverPerPeerInstance(wrapper);
				Integrity res=r.init((DistantBackupCenterConnexionInitialisation)message);
				if (r.isConnected())
					receiversPerPeer.put(message.getHostSource(), r);
				return res;
			}
			else
				return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
		}
		Integrity res=r.received(message);
		if (!r.isConnected())
			receiversPerPeer.remove(message.getHostSource());
		return res;
	}
	public boolean isConnectedIntoThisServer(DecentralizedValue peerID)
	{
		CentralDatabaseBackupReceiverPerPeer r=receiversPerPeer.get(peerID);
		return r!=null && r.isConnected();
	}

	public boolean isConnectedIntoOneOfCentralDatabaseBackupServers(DecentralizedValue peerID) throws DatabaseException {
		return connectedClientsTable.hasRecordsWithAllFields("clientID", peerID);
	}

	public DecentralizedValue getCentralDatabaseBackupServerIDConnectedWithGivenPeerID(DecentralizedValue peerID) throws DatabaseException {
		ConnectedClientsTable.Record r=connectedClientsTable.getRecord("clientID", peerID);
		if (r==null)
			return null;
		else
			return r.getCentralID();
	}



	public DecentralizedValue getCentralID() {
		return centralID;
	}

	public void disconnect() throws DatabaseException {
		disconnectAllPeers();
	}
	private void disconnectAllPeers() throws DatabaseException {
		connectedClientsTable.removeRecordsWithAllFields("centralID", centralID);
		receiversPerPeer.clear();
	}

	public void cleanObsoleteData() throws DatabaseException {
		long curTime=System.currentTimeMillis();
		wrapper.runSynchronizedTransaction(new SynchronizedTransaction<Void>() {
			@Override
			public Void run() throws Exception {
				encryptedBackupPartReferenceTable.removeRecords(new Filter<EncryptedBackupPartReferenceTable.Record>() {
					@Override
					public boolean nextRecord(EncryptedBackupPartReferenceTable.Record _record) {
						_record.getFileReference().delete();
						return true;
					}
				}, "database.removeTimeUTC IS NOT NULL AND database.removeTimeUTC<=%ct", "ct", curTime);
				databaseBackupPerClientTable.removeRecords("removeTimeUTC IS NOT NULL AND removeTimeUTC<=%ct", "ct", curTime);

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
	}
}
