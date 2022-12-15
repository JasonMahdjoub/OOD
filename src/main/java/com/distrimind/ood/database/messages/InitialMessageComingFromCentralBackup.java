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
package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.*;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.io.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Initial message coming from central database backup
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class InitialMessageComingFromCentralBackup extends DatabaseEvent implements MessageComingFromCentralDatabaseBackup, SecureExternalizable {

	private DecentralizedValue hostDestination;
	private Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost;
	private Map<String, Long> lastValidatedTransactionsUTCForDestinationHost;
	private List<byte[]> encryptedAuthenticatedP2PMessages;
	private transient List<AuthenticatedP2PMessage> authenticatedP2PMessages=null;
	private Map<DecentralizedValue, byte[]> encryptedCompatibleDatabases;

	@SuppressWarnings("unused")
	private InitialMessageComingFromCentralBackup()
	{

	}
	static void checkLastValidatedLocalAndDistantEncryptedIDs(Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost)
	{
		if (lastValidatedAndEncryptedIDsPerHost==null)
			throw new NullPointerException();
		if (lastValidatedAndEncryptedIDsPerHost.containsValue(null))
			throw new NullPointerException();
		if (lastValidatedAndEncryptedIDsPerHost.containsKey(null))
			throw new NullPointerException();

	}
	static void checkLastValidatedTransactionsUTCForDestinationHost(Map<String, Long> lastValidatedTransactionsUTCForDestinationHost)
	{
		if (lastValidatedTransactionsUTCForDestinationHost==null)
			throw new NullPointerException();
	}
	public InitialMessageComingFromCentralBackup(DecentralizedValue hostDestination,
												 Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost,
												 Map<String, Long> lastValidatedTransactionsUTCForDestinationHost,
												 List<byte[]> encryptedAuthenticatedP2PMessages,
												 Map<DecentralizedValue, byte[]> encryptedCompatibleDatabases) {
		checkLastValidatedLocalAndDistantEncryptedIDs(lastValidatedAndEncryptedIDsPerHost);
		checkLastValidatedTransactionsUTCForDestinationHost(lastValidatedTransactionsUTCForDestinationHost);
		if (hostDestination==null)
			throw new NullPointerException();
		if (encryptedCompatibleDatabases==null)
			throw new NullPointerException();
		if (encryptedAuthenticatedP2PMessages!=null)
		{
			if (encryptedAuthenticatedP2PMessages.contains(null))
				throw new NullPointerException();
			if (encryptedAuthenticatedP2PMessages.size()> IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup.MAX_NUMBER_OF_P2P_MESSAGES_PER_PEER)
				throw new IllegalArgumentException();
		}


		this.lastValidatedAndEncryptedIDsPerHost = lastValidatedAndEncryptedIDsPerHost;
		this.lastValidatedTransactionsUTCForDestinationHost = lastValidatedTransactionsUTCForDestinationHost;
		this.hostDestination = hostDestination;
		this.encryptedAuthenticatedP2PMessages=encryptedAuthenticatedP2PMessages;
		this.encryptedCompatibleDatabases=encryptedCompatibleDatabases;
	}
	public List<AuthenticatedP2PMessage> getAuthenticatedP2PMessages(EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		if (authenticatedP2PMessages==null)
		{
			authenticatedP2PMessages= IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup.getAuthenticatedP2PMessages(encryptionProfileProvider, encryptedAuthenticatedP2PMessages);
		}
		return authenticatedP2PMessages;
	}

	public InitialMessageComingFromCentralBackup(Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost) {
		this.lastValidatedAndEncryptedIDsPerHost = lastValidatedAndEncryptedIDsPerHost;
	}

	public Set<String> getDecryptedCompatibleDatabases(DecentralizedValue hostID, EncryptionProfileProvider encryptionProfileProvider) throws DatabaseException {
		if (hostID==null)
			throw new NullPointerException();
		byte[] e=encryptedCompatibleDatabases.get(hostID);
		if (e==null)
			return null;
		return AbstractCompatibleEncryptedDatabaseMessage.getDecryptedCompatibleDatabases(e, encryptionProfileProvider);
	}

	static Map<DecentralizedValue, LastValidatedLocalAndDistantID> getLastValidatedIDsPerHost(EncryptionProfileProvider encryptionProfileProvider,
																							  Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost) throws IOException {
		Map<DecentralizedValue, LastValidatedLocalAndDistantID> res=new HashMap<>();
		for (Map.Entry<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> e : lastValidatedAndEncryptedIDsPerHost.entrySet())
		{
			long localID;
			Map<String, Long> distantIdsPerDatabase=new HashMap<>();
			if (e.getValue()==null || e.getValue().getLastValidatedLocalID()==null)
				localID=Long.MIN_VALUE;
			else
				localID=EncryptionTools.decryptID(encryptionProfileProvider, e.getValue().getLastValidatedLocalID());

			if (e.getValue()!=null) {
				for (Map.Entry<String, byte[]> e2 : e.getValue().getLastValidatedDistantIDPerDatabase().entrySet())
				{
					distantIdsPerDatabase.put(e2.getKey(), EncryptionTools.decryptID(encryptionProfileProvider, e2.getValue()));
				}
			}
			res.put(e.getKey(), new LastValidatedLocalAndDistantID(localID, distantIdsPerDatabase));
		}
		return res;
	}

	public Map<DecentralizedValue, LastValidatedLocalAndDistantID> getLastValidatedIDsPerHost(EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		return getLastValidatedIDsPerHost(encryptionProfileProvider, lastValidatedAndEncryptedIDsPerHost);
	}

	public Map<String, Long> getLastValidatedTransactionsUTCForDestinationHost() {
		return lastValidatedTransactionsUTCForDestinationHost;
	}

	@Override
	public DecentralizedValue getHostDestination() {
		return hostDestination;
	}

	@Override
	public int getInternalSerializedSize() {

		return SerializationTools.getInternalSize(hostDestination)
				+SerializationTools.getInternalSize(encryptedAuthenticatedP2PMessages, IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup.SIZE_IN_BYTES_AUTHENTICATED_MESSAGES_QUEUE_TO_SEND)
				+getInternalSerializedSize(lastValidatedAndEncryptedIDsPerHost, encryptedCompatibleDatabases)
				+getInternalSerializedSize(lastValidatedTransactionsUTCForDestinationHost);
	}
	static int getInternalSerializedSize(Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost,
										 Map<DecentralizedValue, byte[]> encryptedCompatibleDatabases)
	{
		int res=2;
		for (Map.Entry<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> e : lastValidatedAndEncryptedIDsPerHost.entrySet())
		{
			res+=SerializationTools.getInternalSize(e.getKey())+
					SerializationTools.getInternalSize(e.getValue().getLastValidatedLocalID(), EncryptionTools.MAX_ENCRYPTED_ID_SIZE)+
					(encryptedCompatibleDatabases==null?0:SerializationTools.getInternalSize(encryptedCompatibleDatabases.get(e.getKey()), AbstractCompatibleEncryptedDatabaseMessage.MAX_SIZE_OF_ENCRYPTED_PACKAGES_NAMES_IN_BYTES));
			for (Map.Entry<String, byte[]> e2 : e.getValue().getLastValidatedDistantIDPerDatabase().entrySet()) {
				res+=SerializationTools.getInternalSize(e2.getKey(), Table.MAX_DATABASE_PACKAGE_NAME_LENGTH)
						+SerializationTools.getInternalSize(e2.getValue(), EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
			}
		}
		return res;
	}
	static int getInternalSerializedSize(Map<String, Long> lastValidatedTransactionsUTCForDestinationHost)
	{
		int res=2+lastValidatedTransactionsUTCForDestinationHost.size()*8;
		for (String s : lastValidatedTransactionsUTCForDestinationHost.keySet())
			res+=SerializationTools.getInternalSize(s, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
		return res;
	}
	static void write(SecuredObjectOutputStream out,
					Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost,
					Map<DecentralizedValue, byte[]> encryptedCompatibleDatabases) throws IOException {
		out.writeUnsignedInt16Bits(lastValidatedAndEncryptedIDsPerHost.size());
		for (Map.Entry<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> e : lastValidatedAndEncryptedIDsPerHost.entrySet())
		{
			out.writeObject(e.getKey(), false);
			out.writeBytesArray(e.getValue().getLastValidatedLocalID(), true, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
			int s=e.getValue().getLastValidatedDistantIDPerDatabase().size();
			if (s>DatabaseWrapper.MAX_PACKAGE_TO_SYNCHRONIZE)
				throw new IOException();
			out.writeInt(s);
			for (Map.Entry<String, byte[]> e2 : e.getValue().getLastValidatedDistantIDPerDatabase().entrySet()) {
				out.writeString(e2.getKey(), false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
				out.writeBytesArray(e2.getValue(), false, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
			}
			if (encryptedCompatibleDatabases!=null)
				out.writeBytesArray(encryptedCompatibleDatabases.get(e.getKey()), true, AbstractCompatibleEncryptedDatabaseMessage.MAX_SIZE_OF_ENCRYPTED_PACKAGES_NAMES_IN_BYTES);
		}
	}
	static void write(SecuredObjectOutputStream out,
				 Map<String, Long> lastValidatedTransactionsUTCForDestinationHost) throws IOException {
		out.writeUnsignedInt16Bits(lastValidatedTransactionsUTCForDestinationHost.size());
		for (Map.Entry<String, Long> e : lastValidatedTransactionsUTCForDestinationHost.entrySet())
		{
			out.writeString(e.getKey(), false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
			out.writeLong(e.getValue());
		}
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostDestination, false);
		write(out, lastValidatedAndEncryptedIDsPerHost, encryptedCompatibleDatabases);
		write(out, this.lastValidatedTransactionsUTCForDestinationHost);
		out.writeCollection(encryptedAuthenticatedP2PMessages, true, IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup.SIZE_IN_BYTES_AUTHENTICATED_MESSAGES_QUEUE_TO_SEND);
	}
	static void read(SecuredObjectInputStream in,
					 Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost,
					 Map<DecentralizedValue, byte[]> encryptedCompatibleDatabases) throws IOException, ClassNotFoundException {
		int s=in.readUnsignedShort();
		if (s>DatabaseWrapper.getMaxHostNumbers())
			throw new MessageExternalizationException(Integrity.FAIL);
		for (int i=0;i<s;i++)
		{
			DecentralizedValue channelHost=in.readObject(false, DecentralizedValue.class);
			byte[] lastValidatedAndEncryptedLocalID=in.readBytesArray(true, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
			int s2=in.readInt();
			if (s2<0 || s2>DatabaseWrapper.MAX_PACKAGE_TO_SYNCHRONIZE)
				throw new MessageExternalizationException(Integrity.FAIL);
			Map<String, byte[]> lastValidatedDistantIDPerDatabase=new HashMap<>();
			for (int j=0;j<s2;j++)
			{
				String db=in.readString(false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
				byte[] lastValidatedDistantID=in.readBytesArray(false, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
				lastValidatedDistantIDPerDatabase.put(db, lastValidatedDistantID);
			}
			lastValidatedAndEncryptedIDsPerHost.put(channelHost, new LastValidatedLocalAndDistantEncryptedID(lastValidatedAndEncryptedLocalID, lastValidatedDistantIDPerDatabase));
			if (encryptedCompatibleDatabases!=null) {
				byte[] array = in.readBytesArray(true, AbstractCompatibleEncryptedDatabaseMessage.MAX_SIZE_OF_ENCRYPTED_PACKAGES_NAMES_IN_BYTES);
				if (array != null)
					encryptedCompatibleDatabases.put(channelHost, array);
			}
		}
	}
	static void read(SecuredObjectInputStream in,
					 Map<String, Long> lastValidatedTransactionsUTCForDestinationHost) throws IOException {
		int s=in.readUnsignedShort();
		if (s>DatabaseWrapper.getMaxHostNumbers())
			throw new MessageExternalizationException(Integrity.FAIL);
		for (int i=0;i<s;i++)
		{
			String packageString=in.readString(false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
			if (packageString.trim().length()==0)
				throw new MessageExternalizationException(Integrity.FAIL);
			long utc=in.readLong();
			lastValidatedTransactionsUTCForDestinationHost.put(packageString, utc);
		}
	}
	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		this.lastValidatedTransactionsUTCForDestinationHost=new HashMap<>();
		this.lastValidatedAndEncryptedIDsPerHost=new HashMap<>();
		this.encryptedCompatibleDatabases=new HashMap<>();
		hostDestination=in.readObject(false, DecentralizedValue.class);
		read(in, lastValidatedAndEncryptedIDsPerHost, encryptedCompatibleDatabases);
		read(in, lastValidatedTransactionsUTCForDestinationHost);
		encryptedAuthenticatedP2PMessages=in.readCollection(true, IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup.SIZE_IN_BYTES_AUTHENTICATED_MESSAGES_QUEUE_TO_SEND, byte[].class);
		authenticatedP2PMessages=null;
	}

	@Override
	public boolean cannotBeMerged() {
		return true;
	}

	public List<byte[]> getEncryptedAuthenticatedP2PMessages() {
		return encryptedAuthenticatedP2PMessages;
	}

	@Override
	public String toString() {
		return "InitialMessageComingFromCentralBackup{" +
				"hostDestination=" + DatabaseWrapper.toString(hostDestination) +
				", lastValidatedTransactionsUTCForDestinationHost=" + lastValidatedTransactionsUTCForDestinationHost +
				", authenticatedP2PMessages=" + authenticatedP2PMessages +
				'}';
	}
}
