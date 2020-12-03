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

import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.messages.*;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.Integrity;
import com.distrimind.util.io.SecureExternalizable;

import java.io.FileNotFoundException;
import java.io.IOException;
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
	protected final ClientCloudAccountTable clientCloudAccountTable;
	protected ClientCloudAccountTable.Record clientCloud;
	protected volatile boolean connected;

	public CentralDatabaseBackupReceiver(DatabaseWrapper wrapper) throws DatabaseException {
		this.clientTable=wrapper.getTableInstance(ClientTable.class);
		this.clientCloudAccountTable=wrapper.getTableInstance(ClientCloudAccountTable.class);
		this.connected=false;
	}
	public void disconnect()
	{
		this.connectedClientID=null;
		this.clientCloud=null;
		this.connectedClientRecord=null;
		connected=false;
	}
	public boolean isConnected()
	{
		return connected;
	}
	public Integrity init(DecentralizedValue connectedClientID) throws DatabaseException {
		return init(connectedClientID, (ClientCloudAccountTable.Record)null);
	}
	public Integrity init(DecentralizedValue connectedClientID, long clientCloudID) throws DatabaseException {
		ClientCloudAccountTable.Record clientCloud=clientCloudAccountTable.getRecord("accountID", clientCloudID);
		return initPrivate(connectedClientID, clientCloud);
	}
	private Integrity initPrivate(DecentralizedValue connectedClientID, ClientCloudAccountTable.Record clientCloud) throws DatabaseException {
		if (clientCloud==null)
		{
			Integrity i=init(connectedClientID, (ClientCloudAccountTable.Record)null);
			if (connectedClientRecord!=null) {
				disconnect();
				return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
			}
			else
				return i;
		}
		else
			return init(connectedClientID, clientCloud);
	}
	public Integrity init(DecentralizedValue connectedClientID, SecureExternalizable clientCloudExternalID) throws DatabaseException {
		List<ClientCloudAccountTable.Record> l=clientCloudAccountTable.getRecordsWithAllFields("externalAccountID", clientCloudExternalID);
		ClientCloudAccountTable.Record clientCloud=null;
		if (l.size()>0)
			clientCloud=l.iterator().next();
		return initPrivate(connectedClientID, clientCloud);

	}
	public Integrity init(DecentralizedValue connectedClientID, ClientCloudAccountTable.Record clientCloud) throws DatabaseException {
		if (connectedClientID==null)
			throw new NullPointerException();
		this.connectedClientID = connectedClientID;

		this.connectedClientRecord=clientTable.getRecord("clientID", connectedClientID);
		if (this.connectedClientRecord!=null)
		{
			if (clientCloud==null)
				clientCloud=this.connectedClientRecord.getAccount();
			else if (this.connectedClientRecord.getAccount().getAccountID()!=clientCloud.getAccountID()))
				return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
		}
		this.clientCloud=clientCloud;
		this.connected=true;
		return Integrity.OK;
	}

	public abstract short getDefaultMaxClientsPerAccount();

	protected void createAccount() throws DatabaseException {
		assert clientCloud==null;
		clientCloud=clientCloudAccountTable.addRecord(new ClientCloudAccountTable.Record(getDefaultMaxClientsPerAccount()));
	}
	protected void createClient() throws DatabaseException {
		assert connectedClientRecord==null;
		assert clientCloud!=null;
		connectedClientRecord=clientTable.addRecord(new ClientTable.Record(connectedClientID, clientCloud));

	}
	private void checkCreationOfClient() throws DatabaseException {
		if (clientCloud==null)
			createAccount();
		if (connectedClientRecord==null)
			createClient();
	}

	public Integrity received(MessageDestinedToCentralDatabaseBackup message) throws DatabaseException, IOException {
		if (!connected)
			return Integrity.FAIL;
		if (!message.getHostSource().equals(connectedClientID))
			return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
		checkCreationOfClient();
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
			disconnect(message.getHostSource());
		}
		else
			return Integrity.FAIL_AND_CANDIDATE_TO_BAN;

	}
	private Integrity received(IndirectMessagesDestinedToCentralDatabaseBackup message) throws DatabaseException {
		return clientTable.addEncryptedAuthenticatedMessage(message, connectedClientRecord );
	}

	private void received(EncryptedBackupPartDestinedToCentralDatabaseBackup message) throws DatabaseException, IOException {




		DatabaseBackupPerClientTable m=databaseBackup.get(message.getHostSource());
		if (m==null)
			throw new DatabaseException("");
		byte[] lastValidatedDistantIDtoBroadcast=m.received(message);

		if (lastValidatedDistantIDtoBroadcast!=null)
		{
			for (Map.Entry<DecentralizedValue, DatabaseBackupPerClientTable> e : databaseBackup.entrySet())
			{
				if (e.getValue().connected && !e.getKey().equals(message.getHostSource()))
					sendMessageFromCentralDatabaseBackup(
							new BackupChannelUpdateMessageFromCentralDatabaseBackup(
									e.getKey(),
									message.getHostSource(),
									m.lastValidatedAndEncryptedDistantID.get(e.getKey()),
									lastValidatedDistantIDtoBroadcast
							)
							, true);
			}

		}
		else
			Assert.fail();
	}
	private DatabaseBackupPerClientTable addHost(DecentralizedValue host)
	{
		DatabaseBackupPerClientTable res=new DatabaseBackupPerClientTable(host);
		databaseBackup.put(host, res);
		return res;
	}
	private void disconnect(DecentralizedValue host)
	{
		DatabaseBackupPerClientTable m=databaseBackup.get(host);
		if (m!=null)
			m.connected=false;
	}
	private void received(DistantBackupCenterConnexionInitialisation message) throws DatabaseException {
		DatabaseBackupPerClientTable m=databaseBackup.get(message.getHostSource());
		if (m==null)
			m=addHost(message.getHostSource());
		m.received(message);
		m.connected=true;
		Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost=new HashMap<>();
		Map<String, Long> lastValidatedTransactionsUTCForDestinationHost=new HashMap<>();
		for (Map.Entry<DecentralizedValue, DatabaseBackupPerClientTable> e : databaseBackup.entrySet())
		{
			if (e.getKey().equals(message.getHostSource()))
				continue;
			lastValidatedAndEncryptedIDsPerHost.put(e.getKey(), new LastValidatedLocalAndDistantEncryptedID(e.getValue().lastValidatedAndEncryptedDistantID.get(message.getHostSource()), e.getValue().lastValidatedAndEncryptedID));
		}
		for (Map.Entry<String, DatabaseBackup> e : m.databaseBackupPerPackage.entrySet())
		{
			lastValidatedTransactionsUTCForDestinationHost.put(e.getKey(), e.getValue().getLastFileBackupPartUTC());
		}
		sendMessageFromCentralDatabaseBackup(new InitialMessageComingFromCentralBackup(message.getHostSource(), lastValidatedAndEncryptedIDsPerHost, lastValidatedTransactionsUTCForDestinationHost));
	}

	private void received(AskForDatabaseBackupPartDestinedToCentralDatabaseBackup message) throws FileNotFoundException, DatabaseException {
		DatabaseBackupPerClientTable m=databaseBackup.get(message.getChannelHost());
		if (m==null)
			return;
		m.received(message);
	}

	private void received(AskForMetaDataPerFileToCentralDatabaseBackup message) throws DatabaseException {
		DatabaseBackupPerClientTable m=databaseBackup.get(message.getChannelHost());
		if (m!=null)
			m.received(message);
	}


	private void received(DatabaseBackupToRemoveDestinedToCentralDatabaseBackup message)
	{
		DatabaseBackupPerClientTable m=databaseBackup.get(message.getHostSource());
		if (m!=null)
			m.databaseBackupPerPackage.remove(message.getPackageString());
	}
	private void received(LastValidatedDistantTransactionDestinedToCentralDatabaseBackup message)
	{
		DatabaseBackupPerClientTable m=databaseBackup.get(message.getHostSource());
		if (m!=null)
			m.received(message);
	}



}
