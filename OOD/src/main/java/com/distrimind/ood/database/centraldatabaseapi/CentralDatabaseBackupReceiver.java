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
import com.distrimind.ood.database.exceptions.ConstraintsNotRespectedDatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.messages.*;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.Reference;
import com.distrimind.util.io.Integrity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public abstract class CentralDatabaseBackupReceiver {
	protected ClientTable.Record connectedClientRecord;
	protected DecentralizedValue connectedClientID;
	//final Map<DecentralizedValue, DatabaseBackupPerHost> databaseBackup=new HashMap<>();
	protected final ClientTable clientTable;
	protected final LastValidatedDistantIDPerClientTable lastValidatedDistantIDPerClientTable;
	protected final DatabaseBackupPerClientTable databaseBackupPerClientTable;
	protected final EncryptedBackupPartReferenceTable encryptedBackupPartReferenceTable;
	protected final ClientCloudAccountTable clientCloudAccountTable;
	protected final ConnectedClientsTable connectedClientsTable;
	protected ClientCloudAccountTable.Record clientCloud;
	protected volatile boolean connected;

	public CentralDatabaseBackupReceiver(DatabaseWrapper wrapper) throws DatabaseException {
		this.clientTable=wrapper.getTableInstance(ClientTable.class);
		this.clientCloudAccountTable=wrapper.getTableInstance(ClientCloudAccountTable.class);
		this.lastValidatedDistantIDPerClientTable=wrapper.getTableInstance(LastValidatedDistantIDPerClientTable.class);
		this.encryptedBackupPartReferenceTable=wrapper.getTableInstance(EncryptedBackupPartReferenceTable.class);
		this.databaseBackupPerClientTable=wrapper.getTableInstance(DatabaseBackupPerClientTable.class);
		this.connectedClientsTable=wrapper.getTableInstance(ConnectedClientsTable.class);
		this.connected=false;
	}
	public abstract void sendMessageFromCentralDatabaseBackup(MessageComingFromCentralDatabaseBackup message);

	public Integrity disconnect() throws DatabaseException {
		if (connectedClientID!=null)
			connectedClientsTable.removeRecord("clientID", connectedClientID);
		this.connectedClientID=null;
		this.clientCloud=null;
		this.connectedClientRecord=null;

		connected=false;
		return Integrity.OK;
	}
	public boolean isConnected(DecentralizedValue clientID) throws DatabaseException {
		return connectedClientsTable.hasRecordsWithAllFields("clientID", clientID);
	}
	public boolean isConnected()
	{
		return connected;
	}

	protected abstract boolean isValidCertificate(CentralDatabaseBackupCertificate certificate);

	public Integrity init(DistantBackupCenterConnexionInitialisation initialMessage) throws DatabaseException {
		CentralDatabaseBackupCertificate certificate=initialMessage.getCertificate();
		if (certificate==null) {
			disconnect();
			return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
		}
		return clientCloudAccountTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				List<ClientCloudAccountTable.Record> l=clientCloudAccountTable.getRecordsWithAllFields("externalAccountID", certificate.getCertifiedAccountPublicKey());
				CentralDatabaseBackupReceiver.this.clientCloud=null;
				if (l.size()>0)
					CentralDatabaseBackupReceiver.this.clientCloud=l.iterator().next();

				if (CentralDatabaseBackupReceiver.this.clientCloud==null || !isValidCertificate(certificate))
				{
					disconnect();
					return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
				}
				else {
					CentralDatabaseBackupReceiver.this.connectedClientID = initialMessage.getHostSource();
					CentralDatabaseBackupReceiver.this.connectedClientRecord=getClientRecord(initialMessage.getHostSource());
					if (CentralDatabaseBackupReceiver.this.connectedClientRecord==null) {
						if (clientTable.getRecordsNumber("account=%a", "a", clientCloud)>=clientCloud.getMaxClients())
							return Integrity.FAIL;
						try {
							connectedClientRecord = clientTable.addRecord(new ClientTable.Record(connectedClientID, CentralDatabaseBackupReceiver.this.clientCloud));
						}
						catch (ConstraintsNotRespectedDatabaseException e)
						{
							disconnect();
							return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
						}
					}
					else
					{
						if (CentralDatabaseBackupReceiver.this.connectedClientRecord.getAccount().getAccountID()!=clientCloud.getAccountID()) {
							disconnect();
							return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
						}
					}
					try {
						connectedClientsTable.addRecord(new ConnectedClientsTable.Record(connectedClientID));
					}
					catch (ConstraintsNotRespectedDatabaseException ignored)
					{

					}
					CentralDatabaseBackupReceiver.this.connected=true;

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

		if (message instanceof EncryptedBackupPartDestinedToCentralDatabaseBackup)
			return received((EncryptedBackupPartDestinedToCentralDatabaseBackup)message);
		else if (message instanceof AskForDatabaseBackupPartDestinedToCentralDatabaseBackup)
			return received((AskForDatabaseBackupPartDestinedToCentralDatabaseBackup)message);
		else if (message instanceof AskForMetaDataPerFileToCentralDatabaseBackup)
			return received((AskForMetaDataPerFileToCentralDatabaseBackup)message);
		else if (message instanceof DatabaseBackupToRemoveDestinedToCentralDatabaseBackup)
			return received((DatabaseBackupToRemoveDestinedToCentralDatabaseBackup)message);
		else if (message instanceof DistantBackupCenterConnexionInitialisation)
			return received((DistantBackupCenterConnexionInitialisation)message);
		else if (message instanceof LastValidatedDistantTransactionDestinedToCentralDatabaseBackup)
			return received((LastValidatedDistantTransactionDestinedToCentralDatabaseBackup)message);
		else if (message instanceof IndirectMessagesDestinedToCentralDatabaseBackup)
			return received((IndirectMessagesDestinedToCentralDatabaseBackup)message);
		else if (message instanceof DisconnectCentralDatabaseBackup) {
			return disconnect();
		}
		else
			return Integrity.FAIL_AND_CANDIDATE_TO_BAN;

	}
	private Integrity received(IndirectMessagesDestinedToCentralDatabaseBackup message) throws DatabaseException {
		return clientTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				ClientTable.Record r=getClientRecord(message.getDestination());
				if (r==null)
					return Integrity.OK;
				if (r.getAccount().getAccountID()==connectedClientRecord.getAccount().getAccountID())
				{
					List<byte[]> encryptedAuthenticatedMessagesToSend=r.getEncryptedAuthenticatedMessagesToSend();
					if (encryptedAuthenticatedMessagesToSend==null)
						encryptedAuthenticatedMessagesToSend=new ArrayList<>();
					encryptedAuthenticatedMessagesToSend.addAll(message.getEncryptedAuthenticatedP2PMessages());
					clientTable.updateRecord(r, "encryptedAuthenticatedMessagesToSend", encryptedAuthenticatedMessagesToSend);
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
		return encryptedBackupPartReferenceTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				DatabaseBackupPerClientTable.Record database=getDatabaseBackupPerClientRecord( connectedClientRecord,  message.getMetaData().getPackageString());
				boolean update=false;
				boolean add=false;
				if (database==null)
				{
					database=new DatabaseBackupPerClientTable.Record(connectedClientRecord, message.getMetaData().getPackageString(), message.getMetaData().getFileTimestampUTC());
					add=true;
				}
				else {
					long l = database.getLastFileBackupPartUTC();
					if (l == message.getMetaData().getFileTimestampUTC())
						return Integrity.FAIL;
					else if (l<message.getMetaData().getFileTimestampUTC()) {
						database.setLastFileBackupPartUTC(message.getMetaData().getFileTimestampUTC());
						update=true;
					}
				}
				FileReference fileReference=getFileReference(message.getMetaData());
				EncryptedBackupPartReferenceTable.Record r=new EncryptedBackupPartReferenceTable.Record(database, fileReference, message);
				try {
					encryptedBackupPartReferenceTable.addRecord(r);
					if (update)
						databaseBackupPerClientTable.updateRecord(database, "lastFileBackupPartUTC", message.getMetaData().getFileTimestampUTC());
					else if (add)
						databaseBackupPerClientTable.addRecord(database);
					sendMessageFromCentralDatabaseBackup(new EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup(message.getHostSource(), message.getMetaData().getFileTimestampUTC(), message.getMetaData().getLastTransactionTimestampUTC(), message.getMetaData().getPackageString()));
					if (update || add)
					{
						clientTable.getRecords(new Filter<ClientTable.Record>() {
							@Override
							public boolean nextRecord(ClientTable.Record _record) throws DatabaseException {
								if (!_record.getClientID().equals(connectedClientID) && isConnected(_record.getClientID()))
								{
									sendMessageFromCentralDatabaseBackup(
											new BackupChannelUpdateMessageFromCentralDatabaseBackup(
													_record.getClientID(),
													message.getHostSource(),
													getLastValidatedAndEncryptedDistantID(connectedClientRecord, _record),
													message.getLastValidatedAndEncryptedID()
											));
								}
								return false;
							}
						}, "account=%a", "a", clientCloud);

					}

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

	private byte[] getLastValidatedAndEncryptedDistantID(ClientTable.Record client, ClientTable.Record distantClient) throws DatabaseException {
		LastValidatedDistantIDPerClientTable.Record r2=lastValidatedDistantIDPerClientTable.getRecord("client", client, "distantClient", distantClient);
		if (r2==null)
			return null;
		else
			return r2.getLastValidatedAndEncryptedDistantID();
	}
	private Integrity received(DistantBackupCenterConnexionInitialisation message) throws DatabaseException {
		return clientTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				Integrity i=lastValidatedDistantIDPerClientTable.received(message, connectedClientRecord);
				if (i!=Integrity.OK)
					return i;

				Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost=new HashMap<>();
				Map<String, Long> lastValidatedTransactionsUTCForDestinationHost=new HashMap<>();

				for (ClientTable.Record r : clientTable.getRecords("account=%a", "a", clientCloud))
				{
					if (r.getClientID().equals(connectedClientID))
						continue;
					lastValidatedAndEncryptedIDsPerHost.put(r.getClientID(), new LastValidatedLocalAndDistantEncryptedID(getLastValidatedAndEncryptedDistantID(r, connectedClientRecord), r.getLastValidatedAndEncryptedID()));
				}
				databaseBackupPerClientTable.getRecords(new Filter<DatabaseBackupPerClientTable.Record>() {
					@Override
					public boolean nextRecord(DatabaseBackupPerClientTable.Record _record)  {
						lastValidatedTransactionsUTCForDestinationHost.put(_record.getPackageString(), _record.getLastFileBackupPartUTC());
						return false;
					}
				},"client=%c", "c", connectedClientRecord);
				sendMessageFromCentralDatabaseBackup(new InitialMessageComingFromCentralBackup(message.getHostSource(), lastValidatedAndEncryptedIDsPerHost, lastValidatedTransactionsUTCForDestinationHost));
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

		encryptedBackupPartReferenceTable.getRecords(new Filter<EncryptedBackupPartReferenceTable.Record>() {
			@Override
			public boolean nextRecord(EncryptedBackupPartReferenceTable.Record _record) {
				if (upper) {
					if (_record.getFileTimeUTC() < fileCoordinate.getTimeStamp()) {
						if (found.get() == null || found.get().getFileTimeUTC() < _record.getFileTimeUTC())
							found.set(_record);
					}
				}
				else
				{
					if (_record.getFileTimeUTC() > fileCoordinate.getTimeStamp()) {
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

	private Integrity received(AskForDatabaseBackupPartDestinedToCentralDatabaseBackup message) throws DatabaseException {
		return databaseBackupPerClientTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				ClientTable.Record channelHost=getClientRecord(message.getChannelHost());
				if (channelHost==null)
					return Integrity.OK;
				else if (channelHost.getAccount().getAccountID()!=connectedClientRecord.getAccount().getAccountID())
					return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
				DatabaseBackupPerClientTable.Record r=getDatabaseBackupPerClientRecord(channelHost, message.getPackageString());
				if (r!=null) {
					EncryptedBackupPartReferenceTable.Record e=getBackupMetaDataPerFile(r, message.getFileCoordinate());
					if (e!=null)
						sendMessageFromCentralDatabaseBackup(e.readEncryptedBackupPart(message.getHostSource()));
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
	private ClientTable.Record getClientRecord(DecentralizedValue clientID) throws DatabaseException {
		if (clientID==null)
			throw new NullPointerException();
		return clientTable.getRecord("clientID", clientID);
	}

	private DatabaseBackupPerClientTable.Record getDatabaseBackupPerClientRecord(ClientTable.Record client, String packageString) throws DatabaseException {
		if (packageString==null)
			throw new NullPointerException();
		if (client==null)
			throw new NullPointerException();
		return databaseBackupPerClientTable.getRecord("client", client, "packageString", packageString);
	}
	private Integrity received(AskForMetaDataPerFileToCentralDatabaseBackup message) throws DatabaseException {
		return databaseBackupPerClientTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				ClientTable.Record channelHost=getClientRecord(message.getChannelHost());
				if (channelHost==null)
					return Integrity.OK;
				else if (channelHost.getAccount().getAccountID()!=connectedClientRecord.getAccount().getAccountID())
					return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
				DatabaseBackupPerClientTable.Record r=getDatabaseBackupPerClientRecord(channelHost, message.getPackageString());
				if (r!=null) {
					EncryptedBackupPartReferenceTable.Record e=getBackupMetaDataPerFile(r, message.getFileCoordinate());
					if (e!=null)
						sendMessageFromCentralDatabaseBackup(new EncryptedMetaDataFromCentralDatabaseBackup(message.getHostSource(), message.getChannelHost(), e.getMetaData()));
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

		return clientTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				ClientTable.Record distantClient=getClientRecord(message.getChannelHost());
				if (distantClient==null)
					return Integrity.OK;
				if (distantClient.getAccount().getAccountID()!=connectedClientRecord.getAccount().getAccountID())
					return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
				LastValidatedDistantIDPerClientTable.Record r=lastValidatedDistantIDPerClientTable.getRecord("client", connectedClientRecord, "distantClient", distantClient);
				if (r==null)
				{
					lastValidatedDistantIDPerClientTable.addRecord(new LastValidatedDistantIDPerClientTable.Record(connectedClientRecord, distantClient, message.getEncryptedLastValidatedDistantID()));
				}
				else
					lastValidatedDistantIDPerClientTable.updateRecord(r, "lastValidatedAndEncryptedDistantID", message.getEncryptedLastValidatedDistantID());
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
	private void received(DatabaseBackupToRemoveDestinedToCentralDatabaseBackup message)
	{
		DatabaseBackupPerClientTable m=databaseBackup.get(message.getHostSource());
		if (m!=null)
			m.databaseBackupPerPackage.remove(message.getPackageString());
	}


}
