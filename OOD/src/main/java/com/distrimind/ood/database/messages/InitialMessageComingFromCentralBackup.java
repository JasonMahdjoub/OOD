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
package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.DatabaseEvent;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.EncryptionTools;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.io.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class InitialMessageComingFromCentralBackup extends DatabaseEvent implements DatabaseEventToSend, MessageComingFromCentralDatabaseBackup, SecureExternalizable {

	private DecentralizedValue hostDestination;
	private Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost;
	private Map<String, Long> lastValidatedTransactionsUTCForDestinationHost;

	@SuppressWarnings("unused")
	private InitialMessageComingFromCentralBackup()
	{

	}
	public InitialMessageComingFromCentralBackup(DecentralizedValue hostDestination, Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost, Map<String, Long> lastValidatedTransactionsUTCForDestinationHost) {
		if (lastValidatedAndEncryptedIDsPerHost==null)
			throw new NullPointerException();
		if (lastValidatedTransactionsUTCForDestinationHost ==null)
			throw new NullPointerException();
		if (hostDestination==null)
			throw new NullPointerException();
		this.lastValidatedAndEncryptedIDsPerHost = lastValidatedAndEncryptedIDsPerHost;
		this.lastValidatedTransactionsUTCForDestinationHost = lastValidatedTransactionsUTCForDestinationHost;
		this.hostDestination = hostDestination;
	}

	public InitialMessageComingFromCentralBackup(Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost) {
		this.lastValidatedAndEncryptedIDsPerHost = lastValidatedAndEncryptedIDsPerHost;
	}


	public Map<DecentralizedValue, LastValidatedLocalAndDistantID> getLastValidatedIDsPerHost(EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		Map<DecentralizedValue, LastValidatedLocalAndDistantID> res=new HashMap<>();
		for (Map.Entry<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> e : lastValidatedAndEncryptedIDsPerHost.entrySet())
		{
			long localID, distantID;
			if (e.getValue()==null || e.getValue().getLastValidatedLocalID()==null)
				localID=Long.MIN_VALUE;
			else
				localID=EncryptionTools.decryptID(encryptionProfileProvider, e.getValue().getLastValidatedLocalID());
			if (e.getValue()==null || e.getValue().getLastValidatedDistantID()==null)
				distantID=Long.MIN_VALUE;
			else
				distantID=EncryptionTools.decryptID(encryptionProfileProvider, e.getValue().getLastValidatedDistantID());
			res.put(e.getKey(), new LastValidatedLocalAndDistantID(localID, distantID));
		}
		return res;
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
		int res=SerializationTools.getInternalSize((SecureExternalizable)hostDestination)+4+this.lastValidatedTransactionsUTCForDestinationHost.size()*8;
		for (Map.Entry<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> e : lastValidatedAndEncryptedIDsPerHost.entrySet())
		{
			res+=SerializationTools.getInternalSize((SecureExternalizable)e.getKey())+
					SerializationTools.getInternalSize(e.getValue().getLastValidatedLocalID(), EncryptionTools.MAX_ENCRYPTED_ID_SIZE)+
					SerializationTools.getInternalSize(e.getValue().getLastValidatedDistantID(), EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
		}
		for (String s : this.lastValidatedTransactionsUTCForDestinationHost.keySet())
			res+=SerializationTools.getInternalSize(s, SerializationTools.MAX_CLASS_LENGTH);
		return res;
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostDestination, false);
		out.writeUnsignedShort(this.lastValidatedAndEncryptedIDsPerHost.size());
		for (Map.Entry<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> e : lastValidatedAndEncryptedIDsPerHost.entrySet())
		{
			out.writeObject(e.getKey(), false);
			out.writeBytesArray(e.getValue().getLastValidatedLocalID(), true, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
			out.writeBytesArray(e.getValue().getLastValidatedDistantID(), true, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
		}
		out.writeUnsignedShort(this.lastValidatedTransactionsUTCForDestinationHost.size());
		for (Map.Entry<String, Long> e : lastValidatedTransactionsUTCForDestinationHost.entrySet())
		{
			out.writeString(e.getKey(), false, SerializationTools.MAX_CLASS_LENGTH);
			out.writeLong(e.getValue());
		}
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		this.lastValidatedTransactionsUTCForDestinationHost=new HashMap<>();
		this.lastValidatedAndEncryptedIDsPerHost=new HashMap<>();
		hostDestination=in.readObject(false, DecentralizedValue.class);
		int s=in.readUnsignedShort();
		if (s>DatabaseWrapper.getMaxHostNumbers())
			throw new MessageExternalizationException(Integrity.FAIL);
		for (int i=0;i<s;i++)
		{
			DecentralizedValue channelHost=in.readObject(false, DecentralizedValue.class);
			byte[] lastValidatedAndEncryptedLocalID=in.readBytesArray(true, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
			byte[] lastValidatedAndEncryptedDistantID=in.readBytesArray(true, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
			this.lastValidatedAndEncryptedIDsPerHost.put(channelHost, new LastValidatedLocalAndDistantEncryptedID(lastValidatedAndEncryptedLocalID, lastValidatedAndEncryptedDistantID));
		}
		s=in.readUnsignedShort();
		if (s>DatabaseWrapper.getMaxHostNumbers())
			throw new MessageExternalizationException(Integrity.FAIL);
		for (int i=0;i<s;i++)
		{
			String packageString=in.readString(false, SerializationTools.MAX_CLASS_LENGTH);
			if (packageString.trim().length()==0)
				throw new MessageExternalizationException(Integrity.FAIL);
			long utc=in.readLong();
			this.lastValidatedTransactionsUTCForDestinationHost.put(packageString, utc);
		}

	}
}
