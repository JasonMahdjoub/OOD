package com.distrimind.ood.database.centraldatabaseapi;
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

import com.distrimind.ood.database.*;
import com.distrimind.ood.database.exceptions.*;
import com.distrimind.ood.database.filemanager.FileRecord;
import com.distrimind.ood.database.filemanager.FileReference;
import com.distrimind.ood.database.filemanager.FileReferenceManager;
import com.distrimind.ood.database.messages.*;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.Reference;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.crypto.IASymmetricPublicKey;
import com.distrimind.util.io.Integrity;
import com.distrimind.util.io.MessageExternalizationException;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public abstract class CentralDatabaseBackupReceiverPerPeer {
	protected ClientTable.Record connectedClientRecord;
	protected DecentralizedValue connectedClientID;
	protected final CentralDatabaseBackupReceiver centralDatabaseBackupReceiver;
	//final Map<DecentralizedValue, DatabaseBackupPerHost> databaseBackup=new HashMap<>();

	protected ClientCloudAccountTable.Record clientCloud;
	protected volatile boolean connected;
	CentralDatabaseBackupCertificate certificate;

	public CentralDatabaseBackupReceiverPerPeer(CentralDatabaseBackupReceiver centralDatabaseBackupReceiver, DatabaseWrapper wrapper) {
		if (centralDatabaseBackupReceiver==null)
			throw new NullPointerException();
		if (wrapper==null)
			throw new NullPointerException();
		this.centralDatabaseBackupReceiver=centralDatabaseBackupReceiver;
		this.connected=false;
	}
	protected void sendMessage(MessageComingFromCentralDatabaseBackup message) throws DatabaseException {
		Logger l=centralDatabaseBackupReceiver.clientTable.getDatabaseWrapper().getCentralDatabaseLogger();
		if (centralDatabaseBackupReceiver.isConnectedIntoThisServer(message.getHostDestination())) {
			if (l != null && l.isLoggable(Level.FINER))
				l.finer("Send message : " + message);
			sendMessageFromThisCentralDatabaseBackup(message);
		}
		else {
			if (l!=null && l.isLoggable(Level.FINER))
				l.finer("Send message from other central database backup : "+message);
			DecentralizedValue sid=centralDatabaseBackupReceiver.getCentralDatabaseBackupServerIDConnectedWithGivenPeerID(message.getHostDestination());
			if (sid!=null)
				sendMessageFromOtherCentralDatabaseBackup(sid, message);
			else if (l!=null)
				l.warning("Impossible to find central database backup which connect "+DatabaseWrapper.toString(message.getHostDestination()));
		}
	}
	protected abstract void sendMessageFromThisCentralDatabaseBackup(MessageComingFromCentralDatabaseBackup message) throws DatabaseException;
	protected abstract void sendMessageFromOtherCentralDatabaseBackup(DecentralizedValue centralDatabaseBackupID, MessageComingFromCentralDatabaseBackup message) throws DatabaseException;

	public Integrity disconnect() throws DatabaseException {
		if (connected)
			centralDatabaseBackupReceiver.connectedClientsTable.removeRecord("clientID", connectedClientID);
		this.connectedClientID=null;
		this.clientCloud=null;
		this.connectedClientRecord=null;

		connected=false;
		return Integrity.OK;
	}

	public boolean isConnected()
	{
		return connected;
	}
	protected boolean isRevokedCertificate(CentralDatabaseBackupCertificate certificate) throws DatabaseException {
		byte[] id=certificate.getCertificateIdentifier();
		if (id==null)
			return true;
		return centralDatabaseBackupReceiver.revokedCertificateTable.hasRecordsWithAllFields("certificateID", id);
	}
	protected abstract EncryptionProfileProvider getEncryptionProfileProviderToValidateCertificateOrGetNullIfNoValidProviderIsAvailable(CentralDatabaseBackupCertificate certificate);
	public abstract Integrity isAcceptableCertificate(CentralDatabaseBackupCertificate certificate) ;
	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	private boolean checkMessageSignature(AuthenticatedMessageDestinedToCentralDatabaseBackup message) throws DatabaseException {
		if (message==null)
			throw new NullPointerException();
		return checkCertificateImpl(message.getCertificate(), message);
	}
	private boolean checkCertificateImpl(CentralDatabaseBackupCertificate certificate, AuthenticatedMessageDestinedToCentralDatabaseBackup message) throws DatabaseException {
		if (certificate==null)
			return false;
		if (certificate.getCertificateExpirationTimeUTCInMs()<System.currentTimeMillis())
			return false;
		if (isAcceptableCertificate(certificate)!=Integrity.OK)
			return false;
		EncryptionProfileProvider encryptionProfileProvider=getEncryptionProfileProviderToValidateCertificateOrGetNullIfNoValidProviderIsAvailable(certificate);
		if (encryptionProfileProvider==null)
			return false;
		if (certificate.isValidCertificate(getAccountID(), getExternalAccountID(), connectedClientID==null?(message==null?null:message.getHostSource()):connectedClientID, getCentralID())==Integrity.OK && (message==null || message.checkHashAndPublicSignature(encryptionProfileProvider)==Integrity.OK)) {
			if (isRevokedCertificate(certificate))
			{
				sendMessageFromThisCentralDatabaseBackup(new CentralDatabaseBackupCertificateChangedMessage(connectedClientID));
				return false;
			}
			else
				return true;
		}
		else
			return false;
	}

	boolean checkCertificate(CentralDatabaseBackupCertificate certificate) throws DatabaseException {
		return checkCertificateImpl(certificate, null);
	}
	public Integrity init(DistantBackupCenterConnexionInitialisation initialMessage) throws DatabaseException {
		final CentralDatabaseBackupCertificate certificate=initialMessage.getCertificate();
		if (certificate==null) {
			disconnect();
			return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
		}

		return centralDatabaseBackupReceiver.clientCloudAccountTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				List<ClientCloudAccountTable.Record> l = centralDatabaseBackupReceiver.clientCloudAccountTable.getRecordsWithAllFields("externalAccountID", certificate.getCertifiedAccountPublicKey());
				CentralDatabaseBackupReceiverPerPeer.this.clientCloud = null;
				if (l.size() > 0)
					CentralDatabaseBackupReceiverPerPeer.this.clientCloud = l.iterator().next();

				if (CentralDatabaseBackupReceiverPerPeer.this.clientCloud == null || !checkMessageSignature(initialMessage)) {
					disconnect();
					return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
				} else {
					if (CentralDatabaseBackupReceiverPerPeer.this.clientCloud.getRemoveAccountQueryUTCInMs() != null) {
						disconnect();
						return Integrity.FAIL;
					}
					CentralDatabaseBackupReceiverPerPeer.this.certificate = certificate;
					CentralDatabaseBackupReceiverPerPeer.this.connectedClientID = initialMessage.getHostSource();
					try {
						CentralDatabaseBackupReceiverPerPeer.this.connectedClientRecord = getClientRecord(initialMessage.getHostSource());
					} catch (MessageExternalizationException e) {
						return e.getIntegrity();
					}
					if (CentralDatabaseBackupReceiverPerPeer.this.connectedClientRecord == null) {
						if (centralDatabaseBackupReceiver.clientTable.getRecordsNumber("account=%a and toRemoveOrderTimeUTCInMs is null", "a", clientCloud) >= clientCloud.getMaxClients())
							return Integrity.FAIL;
						try {
							connectedClientRecord = centralDatabaseBackupReceiver.clientTable.addRecord(new ClientTable.Record(connectedClientID, CentralDatabaseBackupReceiverPerPeer.this.clientCloud));
						} catch (ConstraintsNotRespectedDatabaseException e) {
							disconnect();
							return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
						}
					} else {
						if (CentralDatabaseBackupReceiverPerPeer.this.connectedClientRecord.getAccount().getAccountID() != clientCloud.getAccountID()) {
							disconnect();
							return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
						}
					}
					try {
						centralDatabaseBackupReceiver.connectedClientsTable.addRecord(new ConnectedClientsTable.Record(connectedClientID, centralDatabaseBackupReceiver.getCentralID()));
					} catch (ConstraintsNotRespectedDatabaseException ignored) {
						ConnectedClientsTable.Record r = centralDatabaseBackupReceiver.connectedClientsTable.getRecord("clientID", connectedClientID);
						if (r == null)
							return Integrity.FAIL;
						else if (!r.getCentralID().equals(centralDatabaseBackupReceiver.getCentralID())) {
							disconnect();
							return Integrity.OK;

						}

					}
					CentralDatabaseBackupReceiverPerPeer.this.connected = true;

					return received(initialMessage);
				}
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


	public Integrity received(MessageDestinedToCentralDatabaseBackup message) throws DatabaseException, IOException {

		if (!connected)
			return Integrity.FAIL;
		if (message==null)
			return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
		if (!message.getHostSource().equals(connectedClientID))
			return Integrity.FAIL_AND_CANDIDATE_TO_BAN;

		if (message instanceof AuthenticatedCentralDatabaseBackupMessage) {
			if (!checkMessageSignature((AuthenticatedMessageDestinedToCentralDatabaseBackup) message)) {
				disconnect();
				return Integrity.FAIL_AND_CANDIDATE_TO_BAN;

			}
		}

		if (message instanceof EncryptedBackupPartDestinedToCentralDatabaseBackup)
			return received((EncryptedBackupPartDestinedToCentralDatabaseBackup)message);
		else if (message instanceof AskForDatabaseBackupPartDestinedToCentralDatabaseBackup)
			return received((AskForDatabaseBackupPartDestinedToCentralDatabaseBackup)message);
		else if (message instanceof AskForMetaDataPerFileToCentralDatabaseBackup)
			return received((AskForMetaDataPerFileToCentralDatabaseBackup)message);
		else if (message instanceof DatabaseBackupToRemoveDestinedToCentralDatabaseBackup)
			return received((DatabaseBackupToRemoveDestinedToCentralDatabaseBackup)message);
		else if (message instanceof PeerToRemoveMessageDestinedToCentralDatabaseBackup)
		{
			return received((PeerToRemoveMessageDestinedToCentralDatabaseBackup)message);
		}
		else if (message instanceof PeerToAddMessageDestinedToCentralDatabaseBackup)
		{
			return received((PeerToAddMessageDestinedToCentralDatabaseBackup)message);
		}
		else if (message instanceof DistantBackupCenterConnexionUpdate)
			return received((DistantBackupCenterConnexionUpdate)message);
		else if (message instanceof DistantBackupCenterConnexionInitialisation)
			return received((DistantBackupCenterConnexionInitialisation)message);
		else if (message instanceof LastValidatedDistantTransactionDestinedToCentralDatabaseBackup)
			return received((LastValidatedDistantTransactionDestinedToCentralDatabaseBackup)message);
		else if (message instanceof IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup)
			return received((IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup)message);
		else if (message instanceof DisconnectCentralDatabaseBackup) {
			return disconnect();
		}
		else if (message instanceof CompatibleDatabasesMessageDestinedToCentralDatabaseBackup)
		{
			return received((CompatibleDatabasesMessageDestinedToCentralDatabaseBackup)message);
		}
		else if (message instanceof AskForInitialSynchronizationPlanMessageDestinedToCentralDatabaseBackup)
		{
			return received((AskForInitialSynchronizationPlanMessageDestinedToCentralDatabaseBackup)message);
		}
		/*else if (message instanceof InitialSynchronizationAppliedMessageDestinedToCentralDatabaseBackup)
		{
			return received((InitialSynchronizationAppliedMessageDestinedToCentralDatabaseBackup)message);
		}*/
		else
			return Integrity.FAIL_AND_CANDIDATE_TO_BAN;

	}
	/*private Integrity received(InitialSynchronizationAppliedMessageDestinedToCentralDatabaseBackup message) throws DatabaseException {
		return centralDatabaseBackupReceiver.clientTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				try {
					ClientTable.Record client = getClientRecord(message.getClonedHost());
					if (client == null)
						return Integrity.FAIL_AND_CANDIDATE_TO_BAN;

					DatabaseBackupPerClientTable.Record r=centralDatabaseBackupReceiver.databaseBackupPerClientTable.getRecord("client", client, "packageString", message.getPackageString());
					if (r==null)
					{
						centralDatabaseBackupReceiver.databaseBackupPerClientTable.addRecord(new DatabaseBackupPerClientTable.Record(client, message.getPackageString(), message.getLastLocalTransactionUTC(), message.getLastEncryptedLocalTransactionID()));
					}
					else
					{
						centralDatabaseBackupReceiver.databaseBackupPerClientTable.updateRecord(r, "lastFileBackupPartUTC", message.getLastLocalTransactionUTC(), "lastValidatedAndEncryptedID", message.getLastEncryptedLocalTransactionID());
					}
					/*parseClients(new Filter<ClientTable.Record>() {
						@Override
						public boolean nextRecord(ClientTable.Record _record) throws DatabaseException {
							CentralDatabaseBackupReceiverPerPeer r=centralDatabaseBackupReceiver.getConnectedIntoThisServer(_record.getClientID());
							if (r!=null)
								r.sendInitialMessageComingFromCentralBackup();
							return false;
						}
					}, null);*/


					/*return Integrity.OK;
				}
				catch (MessageExternalizationException e)
				{
					e.printStackTrace();
					return e.getIntegrity();
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
	}*/
	private Integrity received(AskForInitialSynchronizationPlanMessageDestinedToCentralDatabaseBackup message) throws DatabaseException {
		try {

			Integrity i = centralDatabaseBackupReceiver.clientTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
				@Override
				public Integrity run() throws Exception {

					ClientTable.Record clientSource;
					try {
						clientSource = getClientRecord(message.getHostSource());
					} catch (MessageExternalizationException e) {
						return e.getIntegrity();
					}
					if (clientSource == null)
						return Integrity.FAIL;

					Reference<DatabaseBackupPerClientTable.Record> chosenBackup = new Reference<>();
					centralDatabaseBackupReceiver.databaseBackupPerClientTable.getRecords(new Filter<DatabaseBackupPerClientTable.Record>() {
						@Override
						public boolean nextRecord(DatabaseBackupPerClientTable.Record _record) {
							if (_record.getClient().getClientID().equals(message.getHostSource()))
							{
								if (_record.getClient().getToRemoveOrderTimeUTCInMs()!=null)
									chosenBackup.set(null);
								else
									chosenBackup.set(_record);
								stopTableParsing();
							}
							else if (message.getAcceptedDataSources().contains(_record.getClient().getClientID()) && _record.getRemoveTimeUTC()==null) {
								if (chosenBackup.get() == null || chosenBackup.get().getLastFileBackupPartUTC() < _record.getLastFileBackupPartUTC()) {
									chosenBackup.set(_record);
								}
							}
							return false;
						}
					}, "packageString=%ps and lastFileBackupPartUTC>%minLong and lastValidatedAndEncryptedID is not null", "ps", message.getPackageString(), "minLong", Long.MIN_VALUE);

					if (chosenBackup.get() != null) {
						DatabaseBackupPerClientTable.Record database;
						Map<DecentralizedValue, byte[]> lastValidatedAndEncryptedDistantIdsPerHost=null;
						try {
							if (chosenBackup.get().getClient().getClientID().equals(message.getHostSource()))
								database=chosenBackup.get();
							else {
								database = centralDatabaseBackupReceiver.databaseBackupPerClientTable.addRecord(new DatabaseBackupPerClientTable.Record(clientSource, message.getPackageString(), chosenBackup.get().lastFileBackupPartUTC, chosenBackup.get().getLastValidatedAndEncryptedID()));
								long startPos = 0;
								long sizeRow = 1000;
								long rowNumber = centralDatabaseBackupReceiver.encryptedBackupPartReferenceTable.getRecordsNumber("database=%d", "d", chosenBackup.get());
								FileReferenceManager fileReferenceManager = centralDatabaseBackupReceiver.encryptedBackupPartReferenceTable.getDatabaseWrapper().getFileReferenceManager();
								do {
									List<EncryptedBackupPartReferenceTable.Record> l = centralDatabaseBackupReceiver.encryptedBackupPartReferenceTable.getPaginatedRecords(startPos, sizeRow, "database=%d", "d", chosenBackup.get());
									for (EncryptedBackupPartReferenceTable.Record r : l) {
										FileRecord fr = fileReferenceManager.incrementReferenceFile(r.getFileId());
										centralDatabaseBackupReceiver.encryptedBackupPartReferenceTable.addRecord(new EncryptedBackupPartReferenceTable.Record(database, r.getFileTimeUTC(), r.isReferenceFile(), fr.getFileId(), r.getMetaData()));
									}
									startPos += l.size();
								} while (startPos < rowNumber);
								lastValidatedAndEncryptedDistantIdsPerHost=new HashMap<>();
								for (LastValidatedDistantIDPerClientTable.Record r : centralDatabaseBackupReceiver.lastValidatedDistantIDPerClientTable.getRecords(new Filter<LastValidatedDistantIDPerClientTable.Record>() {
									@Override
									public boolean nextRecord(LastValidatedDistantIDPerClientTable.Record _record) {
										return message.getAcceptedDataSources().contains(_record.getDistantClient().getClientID());
									}
								}, "client=%c and distantClient!=%c and distantClient!=%c2", "c", chosenBackup.get().getClient(), "c2", clientSource)) {
									lastValidatedAndEncryptedDistantIdsPerHost.put(r.getDistantClient().getClientID(), r.getLastValidatedAndEncryptedDistantID());
									try {
										centralDatabaseBackupReceiver.lastValidatedDistantIDPerClientTable.addRecord(new LastValidatedDistantIDPerClientTable.Record(clientSource, r.getDistantClient(), r.getLastValidatedAndEncryptedDistantID()));
									}
									catch (ConstraintsNotRespectedDatabaseException ignored)
									{

									}
								}
								LastValidatedDistantIDPerClientTable.Record r = centralDatabaseBackupReceiver.lastValidatedDistantIDPerClientTable.getRecord("client", clientSource, "distantClient", chosenBackup.get().getClient());
								if (r == null)
									centralDatabaseBackupReceiver.lastValidatedDistantIDPerClientTable.addRecord(new LastValidatedDistantIDPerClientTable.Record(clientSource, chosenBackup.get().getClient(), chosenBackup.get().getLastValidatedAndEncryptedID()));
							}
						} catch (ConstraintsNotRespectedDatabaseException | DatabaseIntegrityException | RecordNotFoundDatabaseException e) {
							e.printStackTrace();
							cancelTransaction();
							return Integrity.FAIL;
						}

						//centralDatabaseBackupReceiver.databaseBackupPerClientTable.getRecords("client=%c and packageString=%ps", "c",chosenBackup.get().getClient(), "ps", message.getPackageString());
						Reference<Long> referenceUTC = new Reference<>();
						centralDatabaseBackupReceiver.encryptedBackupPartReferenceTable.getRecords(new Filter<EncryptedBackupPartReferenceTable.Record>() {
							@Override
							public boolean nextRecord(EncryptedBackupPartReferenceTable.Record _record) {
								if (referenceUTC.get() == null || referenceUTC.get() < _record.getFileTimeUTC()) {
									referenceUTC.set(_record.getFileTimeUTC());
								}
								return false;
							}
						}, "database=%d and isReferenceFile=%irf", "d", database, "irf", true);
						if (lastValidatedAndEncryptedDistantIdsPerHost==null) {
							Map<DecentralizedValue, byte[]> l = new HashMap<>();
							centralDatabaseBackupReceiver.lastValidatedDistantIDPerClientTable.getRecords(new Filter<LastValidatedDistantIDPerClientTable.Record>() {
								@Override
								public boolean nextRecord(LastValidatedDistantIDPerClientTable.Record _record) {
									if (_record.getLastValidatedAndEncryptedDistantID() != null && message.getAcceptedDataSources().contains(_record.getDistantClient().getClientID()))
										l.put(_record.getDistantClient().getClientID(), _record.getLastValidatedAndEncryptedDistantID());
									return false;
								}
							}, "client=%c and distantClient!=%c", "c", database.getClient());
							lastValidatedAndEncryptedDistantIdsPerHost=l;
						}

						sendMessageFromThisCentralDatabaseBackup(
								new SynchronizationPlanMessageComingFromCentralDatabaseBackup(
										message.getHostSource(), message.getPackageString(),
										chosenBackup.get().getClient().getClientID(),
										referenceUTC.get(),
										database.getLastFileBackupPartUTC(),
										lastValidatedAndEncryptedDistantIdsPerHost,
										//getLastValidatedAndEncryptedIDsPerHost(chosenBackup.get().getClient()),
										getLastValidatedTransactionsUTCForDestinationHost()

								));
					}
					else
					{
						sendMessageFromThisCentralDatabaseBackup(
								new SynchronizationPlanMessageComingFromCentralDatabaseBackup(
										message.getHostSource(), message.getPackageString()

								));
					}
					return Integrity.OK;
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
			if (i==Integrity.OK)
			{
				sendInitialMessages(message.getAcceptedDataSources(), true);
			}
			return i;
		}
		catch (TransactionCanceledException ignored)
		{
			return Integrity.FAIL;
		}
	}

	private void sendInitialMessages(Set<DecentralizedValue> accepted, boolean includeCurrentClient) throws DatabaseException {
		parseClients(new Filter<ClientTable.Record>() {
			@Override
			public boolean nextRecord(ClientTable.Record _record) throws DatabaseException {
				if (accepted.contains(_record.getClientID()) || (includeCurrentClient && _record.getClientID().equals(connectedClientID))) {
					InitialMessageComingFromCentralBackup m = centralDatabaseBackupReceiver.getInitialMessageComingFromCentralBackup(_record);
					if (m != null)
						sendMessage(m);
				}
				return false;
			}
		}, null, true);
	}
	private Integrity received(CompatibleDatabasesMessageDestinedToCentralDatabaseBackup message) throws DatabaseException {
		return centralDatabaseBackupReceiver.clientTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {

				ClientTable.Record r;
				try {
					r = getClientRecord(message.getHostSource());
				} catch (MessageExternalizationException e) {
					return e.getIntegrity();
				}
				if (r == null)
					return Integrity.FAIL;

				centralDatabaseBackupReceiver.clientTable.updateRecord(r, "encryptedCompatiblesDatabases", message.getEncryptedCompatibleDatabases());
				parseClients(
						new Filter<ClientTable.Record>() {

							@Override
							public boolean nextRecord(ClientTable.Record _record) throws DatabaseException {
								sendMessage(new CompatibleDatabasesMessageComingFromCentralDatabaseBackup(message.getEncryptedCompatibleDatabases(), message.getHostSource(), _record.getClientID()));
								return false;
							}
						}, r.getClientID(), false);
				return Integrity.OK;
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_READ_COMMITTED;
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
	void parseClients(Filter<ClientTable.Record> f, DecentralizedValue exceptThisClientID, boolean parseDisconnectedClients) throws DatabaseException {
		centralDatabaseBackupReceiver.parseClients(connectedClientRecord, f, exceptThisClientID, parseDisconnectedClients);
	}

	private Integrity received(IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup message) throws DatabaseException {
		return centralDatabaseBackupReceiver.clientTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				ClientTable.Record r;
				try {
					r = getClientRecord(message.getHostDestination());
				}
				catch (MessageExternalizationException e)
				{
					return e.getIntegrity();
				}
				if (r==null)
					return Integrity.OK;
				if (r.getAccount().getAccountID()==connectedClientRecord.getAccount().getAccountID())
				{
					if (centralDatabaseBackupReceiver.isConnectedIntoOneOfCentralDatabaseBackupServers(message.getHostDestination())) {

						sendMessage(message);
					}
					else {
						List<byte[]> encryptedAuthenticatedMessagesToSend = r.getEncryptedAuthenticatedMessagesToSend();
						if (encryptedAuthenticatedMessagesToSend == null)
							encryptedAuthenticatedMessagesToSend = new ArrayList<>();
						encryptedAuthenticatedMessagesToSend.addAll(message.getEncryptedAuthenticatedP2PMessages());
						centralDatabaseBackupReceiver.clientTable.updateRecord(r, "encryptedAuthenticatedMessagesToSend", encryptedAuthenticatedMessagesToSend);
					}
					return Integrity.OK;
				}
				else
					return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
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
	public abstract FileReference getFileReference(EncryptedDatabaseBackupMetaDataPerFile encryptedDatabaseBackupMetaDataPerFile);

	private Integrity received(EncryptedBackupPartDestinedToCentralDatabaseBackup message) throws DatabaseException{
		return centralDatabaseBackupReceiver.encryptedBackupPartReferenceTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				DatabaseBackupPerClientTable.Record database=getDatabaseBackupPerClientRecord( connectedClientRecord,  message.getMetaData().getPackageString());
				//boolean update=false;
				final boolean add=database==null;
				if (database==null)
				{
					database=new DatabaseBackupPerClientTable.Record(connectedClientRecord, message.getMetaData().getPackageString(), message.getMetaData().getFileTimestampUTC(), message.getLastValidatedAndEncryptedID());
				}
				else {
					long l = database.getLastFileBackupPartUTC();
					if (l >= message.getMetaData().getFileTimestampUTC())
						return Integrity.FAIL;
				}
				FileReference fileReference=getFileReference(message.getMetaData());
				if (fileReference==null)
					return Integrity.FAIL;
				FileRecord fr=centralDatabaseBackupReceiver.databaseBackupPerClientTable.getDatabaseWrapper().getFileReferenceManager().incrementReferenceFile(fileReference);
				EncryptedBackupPartReferenceTable.Record r=new EncryptedBackupPartReferenceTable.Record(database, fr, message);
				try {

					if (add)
						centralDatabaseBackupReceiver.databaseBackupPerClientTable.addRecord(database);
					else
						centralDatabaseBackupReceiver.databaseBackupPerClientTable.updateRecord(database, "lastFileBackupPartUTC", message.getMetaData().getFileTimestampUTC(), "lastValidatedAndEncryptedID", message.getLastValidatedAndEncryptedID());


					centralDatabaseBackupReceiver.encryptedBackupPartReferenceTable.addRecord(r);
					sendMessage(new EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup(message.getHostSource(), message.getMetaData().getFileTimestampUTC(), message.getMetaData().getLastTransactionTimestampUTC(), message.getMetaData().getPackageString()));
					parseClients(new Filter<ClientTable.Record>() {
						@Override
						public boolean nextRecord(ClientTable.Record _record) throws DatabaseException {
							byte[] lastValidatedAndEncryptedDistantID=centralDatabaseBackupReceiver.getLastValidatedAndEncryptedDistantID(connectedClientRecord, _record);
							if (lastValidatedAndEncryptedDistantID!=null)
							{
								/*if (add)
									sendMessage(
											new BackupChannelInitializationMessageFromCentralDatabaseBackup(
													_record.getClientID(),
													message.getHostSource(),
													lastValidatedAndEncryptedDistantID,
													message.getLastValidatedAndEncryptedID()
											));
								else*/
								sendMessage(
										new BackupChannelUpdateMessageFromCentralDatabaseBackup(
												_record.getClientID(),
												message.getHostSource(),
												message.getMetaData().getPackageString(),
												lastValidatedAndEncryptedDistantID,
												message.getLastValidatedAndEncryptedID()
										));
							}
							return false;
						}
					}, connectedClientID, false);

					return Integrity.OK;
				}
				catch (DatabaseException e)
				{
					fileReference.delete();
					throw e;
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
			public void initOrReset()  {

			}
		});

	}




	private Integrity received(PeerToRemoveMessageDestinedToCentralDatabaseBackup message) throws DatabaseException {
		try {
			DecentralizedValue hostID = message.getHostToRemove();
			ClientTable.Record r = getClientRecord(hostID);
			if (r==null) {
				return Integrity.OK;
			}
			if (r.getToRemoveOrderTimeUTCInMs()==null)
				centralDatabaseBackupReceiver.clientTable.updateRecord(r, "toRemoveOrderTimeUTCInMs", System.currentTimeMillis());
			return Integrity.OK;
		}
		catch (MessageExternalizationException e)
		{
			return e.getIntegrity();
		}
	}
	private Integrity received(PeerToAddMessageDestinedToCentralDatabaseBackup message) throws DatabaseException {
		try {
			DecentralizedValue hostID = message.getHostToAdd();
			ClientTable.Record r = getClientRecord(hostID);

			if (r==null)
				return Integrity.OK;
			if (r.getToRemoveOrderTimeUTCInMs()!=null)
				centralDatabaseBackupReceiver.clientTable.updateRecord(r, "toRemoveOrderTimeUTCInMs", null);
			//sendInitialMessageComingFromCentralBackup(r);
			return Integrity.OK;
		}
		catch (MessageExternalizationException e)
		{
			return e.getIntegrity();
		}
	}

	private Map<String, Long> getLastValidatedTransactionsUTCForDestinationHost() throws DatabaseException {
		return centralDatabaseBackupReceiver.getLastValidatedTransactionsUTCForDestinationHost(connectedClientRecord);
	}
	private Integrity received(DistantBackupCenterConnexionInitialisation message) throws DatabaseException {
		return centralDatabaseBackupReceiver.clientTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				try {
					connectedClientRecord = getClientRecord(message.getHostSource());
				}
				catch (MessageExternalizationException e)
				{
					return e.getIntegrity();
				}
				Integrity i=centralDatabaseBackupReceiver.lastValidatedDistantIDPerClientTable.received(message, connectedClientRecord);
				if (i!=Integrity.OK)
					return i;

				InitialMessageComingFromCentralBackup m=centralDatabaseBackupReceiver.getInitialMessageComingFromCentralBackup(connectedClientRecord);
				if (m!=null)
					sendMessage(m);

				return Integrity.OK;
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
			public void initOrReset()  {

			}
		});

	}
	private Integrity received(DistantBackupCenterConnexionUpdate message) throws DatabaseException {
		return centralDatabaseBackupReceiver.clientTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				try {
					connectedClientRecord = getClientRecord(message.getHostSource());
				}
				catch (MessageExternalizationException e)
				{
					return e.getIntegrity();
				}
				Integrity i=centralDatabaseBackupReceiver.lastValidatedDistantIDPerClientTable.received(message, connectedClientRecord);
				if (i!=Integrity.OK)
					return i;

				sendInitialMessages(message.getEncryptedDistantLastValidatedIDs().keySet(), true);

				return Integrity.OK;
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
			public void initOrReset()  {

			}
		});
	}

	private EncryptedBackupPartReferenceTable.Record getBackupMetaDataPerFile(DatabaseBackupPerClientTable.Record databaseBackup, FileCoordinate fileCoordinate) throws DatabaseException {
		assert databaseBackup!=null;
		assert fileCoordinate!=null;

		final Reference<EncryptedBackupPartReferenceTable.Record> found = new Reference<>();
		boolean upper=fileCoordinate.getBoundary()== FileCoordinate.Boundary.UPPER_LIMIT;
		if (!upper && fileCoordinate.getBoundary()!= FileCoordinate.Boundary.LOWER_LIMIT)
			throw new IllegalAccessError();

		centralDatabaseBackupReceiver.encryptedBackupPartReferenceTable.getRecords(new Filter<EncryptedBackupPartReferenceTable.Record>() {
			@Override
			public boolean nextRecord(EncryptedBackupPartReferenceTable.Record _record) {
				if (upper) {
					if (_record.getFileTimeUTC() < fileCoordinate.getTimeStampUTC()) {
						if (found.get() == null || found.get().getFileTimeUTC() < _record.getFileTimeUTC())
							found.set(_record);
					}
				}
				else
				{
					if (_record.getFileTimeUTC() > fileCoordinate.getTimeStampUTC()) {
						if (found.get() == null || found.get().getFileTimeUTC() > _record.getFileTimeUTC())
							found.set(_record);
					}
				}
				return false;
			}
		}, "database=%d", "d", databaseBackup);

		if (found.get()==null)
		{
			return null;
		}
		else
		{
			return found.get();
		}
	}

	protected DatabaseBackupPerClientTable.Record getMostAppropriateChannelHostIDToCreateNewChannel(String packageString) throws DatabaseException {
		Reference<DatabaseBackupPerClientTable.Record> res=new Reference<>(null);
		centralDatabaseBackupReceiver.databaseBackupPerClientTable.getRecords(new Filter<DatabaseBackupPerClientTable.Record>() {
			@Override
			public boolean nextRecord(DatabaseBackupPerClientTable.Record _record) {
				if (res.get()==null || _record.getLastFileBackupPartUTC()>res.get().getLastFileBackupPartUTC())
				{
					res.set(_record);
				}
				return false;
			}
		}, "client.account=%a and packageString=%p", "a", connectedClientRecord.getAccount(), "p", packageString);
		return res.get();
	}

	private Integrity received(AskForDatabaseBackupPartDestinedToCentralDatabaseBackup message) throws DatabaseException {
		return centralDatabaseBackupReceiver.databaseBackupPerClientTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				DecentralizedValue channelHostID=message.getChannelHost();
				ClientTable.Record channelHost;
				if (channelHostID==null)
				{
					assert message.getContext()!= AskForDatabaseBackupPartDestinedToCentralDatabaseBackup.Context.SYNCHRONIZATION;
					DatabaseBackupPerClientTable.Record r=getMostAppropriateChannelHostIDToCreateNewChannel(message.getPackageString());
					if (r==null) {
						centralDatabaseBackupReceiver.clientCloudAccountTable.getDatabaseWrapper().getCentralDatabaseLogger().fine("No account for restoration was found. Impossible to send encrypted backup part.");
						return Integrity.OK;
					}
					else {
						channelHost = r.getClient();
					}
				}
				else {
					try {
						channelHost = getClientRecord(channelHostID);
					} catch (MessageExternalizationException e) {
						return e.getIntegrity();
					}
				}
				if (channelHost==null) {
					centralDatabaseBackupReceiver.clientCloudAccountTable.getDatabaseWrapper().getCentralDatabaseLogger().fine("The asked channel host for restoration was not found. Impossible to send encrypted backup part.");
					return Integrity.OK;
				}
				DatabaseBackupPerClientTable.Record r=getDatabaseBackupPerClientRecord(channelHost, message.getPackageString());
				if (r!=null) {
					EncryptedBackupPartReferenceTable.Record e=getBackupMetaDataPerFile(r, message.getFileCoordinate());
					if (e!=null) {
						switch (message.getContext())
						{
							case SYNCHRONIZATION:
								sendMessage(e.readEncryptedBackupPart(centralDatabaseBackupReceiver.databaseBackupPerClientTable.getDatabaseWrapper(), message.getHostSource()));
								break;
							case RESTORATION:
								sendMessage(e.readEncryptedBackupPartForRestoration(centralDatabaseBackupReceiver.databaseBackupPerClientTable.getDatabaseWrapper(), message.getHostSource(), channelHost.getClientID()));
								break;
							default:
								throw new IllegalAccessError();
						}

					}
					else
						centralDatabaseBackupReceiver.clientCloudAccountTable.getDatabaseWrapper().getCentralDatabaseLogger().fine("The asked backup for restoration was not found. ");
				}
				else
					centralDatabaseBackupReceiver.clientCloudAccountTable.getDatabaseWrapper().getCentralDatabaseLogger().fine("The asked account for restoration was not found. Impossible to send encrypted backup part.");
				return Integrity.OK;
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_REPEATABLE_READ;
			}

			@Override
			public boolean doesWriteData() {
				return false;
			}

			@Override
			public void initOrReset()  {

			}
		});

	}
	private ClientTable.Record getClientRecord(DecentralizedValue clientID) throws DatabaseException, MessageExternalizationException {
		if (clientID==null)
			throw new NullPointerException();
		if (connectedClientRecord!=null && clientID.equals(connectedClientID))
			return connectedClientRecord;
		return centralDatabaseBackupReceiver.getClientRecord(connectedClientRecord==null?null:connectedClientRecord.getAccount(), clientID);

	}


	private DatabaseBackupPerClientTable.Record getDatabaseBackupPerClientRecord(ClientTable.Record client, String packageString) throws DatabaseException {
		if (packageString==null)
			throw new NullPointerException();
		if (client==null)
			throw new NullPointerException();
		return centralDatabaseBackupReceiver.databaseBackupPerClientTable.getRecord("client", client, "packageString", packageString);
	}
	private Integrity received(AskForMetaDataPerFileToCentralDatabaseBackup message) throws DatabaseException {
		return centralDatabaseBackupReceiver.databaseBackupPerClientTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				ClientTable.Record channelHost;
				try {
					channelHost = getClientRecord(message.getChannelHost());
				}
				catch (MessageExternalizationException e)
				{
					return e.getIntegrity();
				}
				if (channelHost==null)
					return Integrity.OK;

				DatabaseBackupPerClientTable.Record r=getDatabaseBackupPerClientRecord(channelHost, message.getPackageString());
				if (r!=null) {
					EncryptedBackupPartReferenceTable.Record e=getBackupMetaDataPerFile(r, message.getFileCoordinate());
					if (e!=null) {
						sendMessage(new EncryptedMetaDataFromCentralDatabaseBackup(message.getHostSource(), message.getChannelHost(), e.getMetaData()));
					}
				}
				return Integrity.OK;
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_REPEATABLE_READ;
			}

			@Override
			public boolean doesWriteData() {
				return false;
			}

			@Override
			public void initOrReset()  {

			}
		});

	}



	private Integrity received(LastValidatedDistantTransactionDestinedToCentralDatabaseBackup message) throws DatabaseException {

		return centralDatabaseBackupReceiver.clientTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				ClientTable.Record distantClient;
				try {
					distantClient = getClientRecord(message.getChannelHost());
				}
				catch (MessageExternalizationException e)
				{
					return e.getIntegrity();
				}
				if (distantClient==null)
					return Integrity.OK;
				LastValidatedDistantIDPerClientTable.Record r=centralDatabaseBackupReceiver.lastValidatedDistantIDPerClientTable.getRecord("client", connectedClientRecord, "distantClient", distantClient);
				if (r==null)
				{
					centralDatabaseBackupReceiver.lastValidatedDistantIDPerClientTable.addRecord(new LastValidatedDistantIDPerClientTable.Record(connectedClientRecord, distantClient, message.getEncryptedLastValidatedDistantID()));
				}
				else
					centralDatabaseBackupReceiver.lastValidatedDistantIDPerClientTable.updateRecord(r, "lastValidatedAndEncryptedDistantID", message.getEncryptedLastValidatedDistantID());

				sendInitialMessages(Collections.singleton(distantClient.getClientID()), false);
				return Integrity.OK;
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

	private Integrity received(DatabaseBackupToRemoveDestinedToCentralDatabaseBackup message) throws DatabaseException {
		DatabaseBackupPerClientTable.Record r=getDatabaseBackupPerClientRecord(connectedClientRecord, message.getPackageString());
		if (r!=null && r.getRemoveTimeUTC()==null)
		{
			centralDatabaseBackupReceiver.databaseBackupPerClientTable.updateRecord(r, "removeTimeUTC", System.currentTimeMillis());
		}
		return Integrity.OK;
	}

	public long getAccountID()
	{
		return clientCloud.getAccountID();
	}

	public IASymmetricPublicKey getExternalAccountID()
	{
		return clientCloud.getExternalAccountID();
	}
	public DecentralizedValue getCentralID()
	{
		return centralDatabaseBackupReceiver.getCentralID();
	}

	public CentralDatabaseBackupReceiver getCentralDatabaseBackupReceiver()
	{
		return centralDatabaseBackupReceiver;
	}
	public DecentralizedValue getHostID()
	{
		return this.connectedClientID;
	}

}
