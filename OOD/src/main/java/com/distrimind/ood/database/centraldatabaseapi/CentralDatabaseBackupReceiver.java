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
import com.distrimind.ood.database.messages.CentralDatabaseBackupCertificateChangedMessage;
import com.distrimind.ood.database.messages.DistantBackupCenterConnexionInitialisation;
import com.distrimind.ood.database.messages.MessageComingFromCentralDatabaseBackup;
import com.distrimind.ood.database.messages.MessageDestinedToCentralDatabaseBackup;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.IASymmetricPublicKey;
import com.distrimind.util.io.Integrity;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

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
	protected final RevokedCertificateTable revokedCertificateTable;




	public CentralDatabaseBackupReceiver(DatabaseWrapper wrapper, DecentralizedValue centralID) throws DatabaseException {
		if (wrapper==null)
			throw new NullPointerException();
		if (centralID==null)
			throw new NullPointerException();
		this.wrapper = wrapper;
		this.centralID=centralID;

		wrapper.getSynchronizer().loadCentralDatabaseClassesIfNecessary();
		this.clientTable=wrapper.getTableInstance(ClientTable.class);
		this.clientCloudAccountTable=wrapper.getTableInstance(ClientCloudAccountTable.class);
		this.lastValidatedDistantIDPerClientTable=wrapper.getTableInstance(LastValidatedDistantIDPerClientTable.class);
		this.encryptedBackupPartReferenceTable=wrapper.getTableInstance(EncryptedBackupPartReferenceTable.class);
		this.databaseBackupPerClientTable=wrapper.getTableInstance(DatabaseBackupPerClientTable.class);
		this.connectedClientsTable=wrapper.getTableInstance(ConnectedClientsTable.class);
		this.revokedCertificateTable=wrapper.getTableInstance(RevokedCertificateTable.class);
		disconnectAllPeers();
	}

	protected abstract CentralDatabaseBackupReceiverPerPeer newCentralDatabaseBackupReceiverPerPeerInstance(DatabaseWrapper wrapper);

	public Integrity received(MessageDestinedToCentralDatabaseBackup message) throws DatabaseException, IOException {
		Logger l=clientTable.getDatabaseWrapper().getCentralDatabaseLogger();
		if (l!=null && l.isLoggable(Level.FINER))
			l.finer("receive message " + message);
		CentralDatabaseBackupReceiverPerPeer r=receiversPerPeer.get(message.getHostSource());
		if (r==null)
		{


			if (message instanceof DistantBackupCenterConnexionInitialisation) {
				r = newCentralDatabaseBackupReceiverPerPeerInstance(wrapper);
				receiversPerPeer.put(message.getHostSource(), r);
				Integrity res=r.init((DistantBackupCenterConnexionInitialisation)message);
				if (!r.isConnected())
					receiversPerPeer.remove(message.getHostSource());
				return res;
			}
			else {

				return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
			}
		}
		Integrity res=r.received(message);
		if (!r.isConnected())
			receiversPerPeer.remove(message.getHostSource());
		return res;
	}

	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	public boolean sendMessageFromThisCentralDatabaseBackup(MessageComingFromCentralDatabaseBackup message) throws DatabaseException {
		CentralDatabaseBackupReceiverPerPeer r=receiversPerPeer.get(message.getHostDestination());
		if (r==null)
			return false;
		else {
			r.sendMessageFromThisCentralDatabaseBackup(message);
			return true;
		}
	}

	public boolean isConnectedIntoThisServer(DecentralizedValue peerID)
	{
		CentralDatabaseBackupReceiverPerPeer r=receiversPerPeer.get(peerID);
		return r!=null && r.isConnected();
	}

	CentralDatabaseBackupReceiverPerPeer getConnectedIntoThisServer(DecentralizedValue peerID)
	{
		CentralDatabaseBackupReceiverPerPeer r=receiversPerPeer.get(peerID);
		if (r!=null && !r.isConnected())
			return null;
		return r;
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

	public Integrity certificateChanged(CentralDatabaseBackupCertificate certificate, CentralDatabaseBackupCertificate certificateToRevoke) throws DatabaseException {
		if (certificate==null)
			throw new NullPointerException();
		if (certificateToRevoke==null)
			throw new NullPointerException();
		byte[] id=certificate.getCertificateIdentifier();
		byte[] revokedID=certificateToRevoke.getCertificateIdentifier();
		if (id==null)
			throw new NullPointerException();
		if (revokedID==null)
			throw new NullPointerException();
		if (revokedCertificateTable.hasRecordsWithAllFields("certificateID", id))
			return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
		if (!revokedCertificateTable.hasRecordsWithAllFields("certificateID", revokedID))
			revokedCertificateTable.addRecord("certificateID", revokedID);
		for (Iterator<Map.Entry<DecentralizedValue, CentralDatabaseBackupReceiverPerPeer>> it =receiversPerPeer.entrySet().iterator();it.hasNext();)
		{
			CentralDatabaseBackupReceiverPerPeer c=it.next().getValue();
			if (c.connectedClientRecord.getAccount().getExternalAccountID().equals(certificate.getCertifiedAccountPublicKey()) && Arrays.equals(c.certificate.getCertificateIdentifier(), certificateToRevoke.getCertificateIdentifier()))
			{
				if (c.checkCertificate(certificate)) {
					c.sendMessageFromThisCentralDatabaseBackup(new CentralDatabaseBackupCertificateChangedMessage(c.connectedClientID));
					c.disconnect();
					it.remove();
				}
				else
					return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
			}
		}
		return Integrity.OK;
	}

	public DecentralizedValue getCentralID() {
		return centralID;
	}

	public void disconnect() throws DatabaseException {
		disconnectAllPeers();
	}
	public void disconnectAllPeers() throws DatabaseException {
		for (CentralDatabaseBackupReceiverPerPeer cpp : receiversPerPeer.values())
			cpp.disconnect();
		receiversPerPeer.clear();
		connectedClientsTable.removeRecordsWithAllFields("centralID", centralID);
	}
	public void peerDisconnected(DecentralizedValue clientID) throws DatabaseException {
		CentralDatabaseBackupReceiverPerPeer cpp=receiversPerPeer.remove(clientID);
		if (cpp!=null)
			cpp.disconnect();
	}

	public void cleanObsoleteData() throws DatabaseException {

		wrapper.runSynchronizedTransaction(new SynchronizedTransaction<Void>() {
			@Override
			public Void run() throws Exception {
				long timeReferenceToRemoveObsoleteBackup=System.currentTimeMillis()- getDurationInMsBeforeRemovingDatabaseBackupAfterAnDeletionOrder();
				long timeReferenceToRemoveObsoleteHosts=System.currentTimeMillis()-Math.max(getDurationInMsThatPermitToCancelPeerRemovingWhenThePeerIsTryingToReconnect(), getDurationInMsBeforeRemovingDatabaseBackupAfterAnDeletionOrder());
				long timeReferenceToRemoveObsoleteBackups=System.currentTimeMillis()- getDurationInMsBeforeOrderingDatabaseBackupDeletion();
				long timeReferenceToRemoveObsoleteAccounts=Math.min(Math.min(System.currentTimeMillis()- getDurationInMsToWaitBeforeRemovingAccountDefinitively(),timeReferenceToRemoveObsoleteHosts), timeReferenceToRemoveObsoleteHosts);

				clientCloudAccountTable.removeRecordsWithCascade("removeAccountQueryUTCInMs is not null and removeAccountQueryUTCInMs<=%t", "t", timeReferenceToRemoveObsoleteAccounts);
				encryptedBackupPartReferenceTable.removeRecordsWithCascade(new Filter<EncryptedBackupPartReferenceTable.Record>() {
					@Override
					public boolean nextRecord(EncryptedBackupPartReferenceTable.Record _record) throws DatabaseException {
						_record.delete(wrapper);
						return true;
					}
				}, "(database.removeTimeUTC IS NOT NULL AND database.removeTimeUTC<=%ct) OR (database.client.toRemoveOrderTimeUTCInMs IS NOT NULL AND database.client.toRemoveOrderTimeUTCInMs<=%ctc)", "ct", timeReferenceToRemoveObsoleteBackup, "ctc", timeReferenceToRemoveObsoleteHosts);

				clientTable.removeRecordsWithCascade("toRemoveOrderTimeUTCInMs IS NOT NULL AND toRemoveOrderTimeUTCInMs<=%ct", "ct", timeReferenceToRemoveObsoleteHosts);
				databaseBackupPerClientTable.removeRecords("removeTimeUTC IS NOT NULL AND removeTimeUTC<=%ct", "ct", timeReferenceToRemoveObsoleteBackup);
				databaseBackupPerClientTable.updateRecords(new AlterRecordFilter<DatabaseBackupPerClientTable.Record>() {
					@Override
					public void nextRecord(DatabaseBackupPerClientTable.Record _record) throws DatabaseException {
						if (encryptedBackupPartReferenceTable.getRecordsNumber("isReferenceFile=%rf and database=%d", "rf", true, "d", _record)>=2)
						{
							update("removeTimeUTC", System.currentTimeMillis());
						}

					}
				},"lastFileBackupPartUTC<=%ct and removeTimeUTC IS NULL", "ct", timeReferenceToRemoveObsoleteBackups);

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

	public abstract long getDurationInMsBeforeRemovingDatabaseBackupAfterAnDeletionOrder();
	public abstract long getDurationInMsBeforeOrderingDatabaseBackupDeletion();

	public abstract long getDurationInMsThatPermitToCancelPeerRemovingWhenThePeerIsTryingToReconnect();
	public abstract long getDurationInMsToWaitBeforeRemovingAccountDefinitively();
	public boolean cancelRemoveAccount(IASymmetricPublicKey externalAccountID) throws DatabaseException {
		return cancelRemoveAccount("externalAccountID", externalAccountID);
	}
	public boolean cancelRemoveAccount(long accountID) throws DatabaseException {
		return cancelRemoveAccount("accountID", accountID);
	}
	public boolean removeAccount(IASymmetricPublicKey externalAccountID) throws DatabaseException {
		return removeAccount("externalAccountID", externalAccountID);
	}
	public boolean removeAccount(long accountID) throws DatabaseException {
		return removeAccount("accountID", accountID);
	}
	public ClientCloudAccountTable.Record addClient(short maxClients, IASymmetricPublicKey externalAccountID) throws DatabaseException {
		ClientCloudAccountTable.Record r=new ClientCloudAccountTable.Record(maxClients, externalAccountID);
		clientCloudAccountTable.addRecord(r);
		return r;
	}
	private boolean removeAccount(Object ... keys) throws DatabaseException {
		return wrapper.runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {
			@Override
			public Boolean run() throws Exception {
				List<ClientCloudAccountTable.Record> l=clientCloudAccountTable.getRecordsWithAllFields(keys);
				if (l.size()==0)
					return false;
				if (l.size()>1)
					throw new IllegalAccessError();
				ClientCloudAccountTable.Record r=l.iterator().next();
				if (r.getRemoveAccountQueryUTCInMs()==null) {
					clientCloudAccountTable.updateRecord(r, "removeAccountQueryUTCInMs", System.currentTimeMillis());
					clientTable.getRecords(new Filter<ClientTable.Record>() {
						@Override
						public boolean nextRecord(ClientTable.Record _record) throws DatabaseException {
							CentralDatabaseBackupReceiverPerPeer c=receiversPerPeer.get(_record.getClientID());
							if (c!=null)
								c.disconnect();
							return false;
						}
					}, "account=%a", "a", r);

					return true;
				}
				else
					return false;
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_REPEATABLE_READ;
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
	private boolean cancelRemoveAccount(Object ... keys) throws DatabaseException {
		return wrapper.runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {
			@Override
			public Boolean run() throws Exception {
				List<ClientCloudAccountTable.Record> l=clientCloudAccountTable.getRecordsWithAllFields(keys);
				if (l.size()==0)
					return false;
				if (l.size()>1)
					throw new IllegalAccessError();
				ClientCloudAccountTable.Record r=l.iterator().next();
				if (r.getRemoveAccountQueryUTCInMs()==null) {
					return false;
				}
				else {
					clientCloudAccountTable.updateRecord(r, "removeAccountQueryUTCInMs", null);
					return true;
				}
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_REPEATABLE_READ;
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
