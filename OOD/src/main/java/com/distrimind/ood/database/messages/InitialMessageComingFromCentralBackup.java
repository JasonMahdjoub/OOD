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
	private Map<DecentralizedValue, byte[]> lastValidatedAndEncryptedIDPerHost;
	private Map<String, Long> lastValidatedTransactionsUTCForDestinationHost;

	@SuppressWarnings("unused")
	private InitialMessageComingFromCentralBackup()
	{

	}
	public InitialMessageComingFromCentralBackup(DecentralizedValue hostDestination, Map<DecentralizedValue, byte[]> lastValidatedAndEncryptedIDPerHost, Map<String, Long> lastValidatedTransactionsUTCForDestinationHost) {
		if (lastValidatedAndEncryptedIDPerHost==null)
			throw new NullPointerException();
		if (lastValidatedTransactionsUTCForDestinationHost ==null)
			throw new NullPointerException();
		if (hostDestination==null)
			throw new NullPointerException();
		this.lastValidatedAndEncryptedIDPerHost = lastValidatedAndEncryptedIDPerHost;
		this.lastValidatedTransactionsUTCForDestinationHost = lastValidatedTransactionsUTCForDestinationHost;
		this.hostDestination = hostDestination;
	}

	public Map<DecentralizedValue, byte[]> getLastValidatedAndEncryptedIDPerHost() {
		return lastValidatedAndEncryptedIDPerHost;
	}

	public Map<DecentralizedValue, Long> getLastValidatedIDPerHost(EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		Map<DecentralizedValue, Long> res=new HashMap<>();
		for (Map.Entry<DecentralizedValue, byte[]> e : lastValidatedAndEncryptedIDPerHost.entrySet())
		{
			if (e.getValue()!=null)
			{
				res.put(e.getKey(), EncryptionTools.decryptID(encryptionProfileProvider, e.getValue()));
			}
			else
				res.put(e.getKey(), Long.MIN_VALUE);
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
		for (Map.Entry<DecentralizedValue, byte[]> e : this.lastValidatedAndEncryptedIDPerHost.entrySet())
		{
			res+=SerializationTools.getInternalSize((SecureExternalizable)e.getKey())+
					SerializationTools.getInternalSize(e.getValue(), EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
		}
		for (String s : this.lastValidatedTransactionsUTCForDestinationHost.keySet())
			res+=SerializationTools.getInternalSize(s, SerializationTools.MAX_CLASS_LENGTH);
		return res;
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostDestination, false);
		out.writeUnsignedShort(this.lastValidatedAndEncryptedIDPerHost.size());
		for (Map.Entry<DecentralizedValue, byte[]> e : this.lastValidatedAndEncryptedIDPerHost.entrySet())
		{
			out.writeObject(e.getKey(), false);
			out.writeBytesArray(e.getValue(), true, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
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
		this.lastValidatedAndEncryptedIDPerHost=new HashMap<>();
		hostDestination=in.readObject(false, DecentralizedValue.class);
		int s=in.readUnsignedShort();
		if (s>DatabaseWrapper.getMaxHostNumbers())
			throw new MessageExternalizationException(Integrity.FAIL);
		for (int i=0;i<s;i++)
		{
			DecentralizedValue channelHost=in.readObject(false, DecentralizedValue.class);
			byte[] lastValidatedAndEncryptedID=in.readBytesArray(true, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
			this.lastValidatedAndEncryptedIDPerHost.put(channelHost, lastValidatedAndEncryptedID);
		}
		s=in.readUnsignedShort();
		if (s>DatabaseWrapper.getMaxHostNumbers())
			throw new MessageExternalizationException(Integrity.FAIL);
		for (int i=0;i<s;i++)
		{
			String packageString=in.readString(false, SerializationTools.MAX_CLASS_LENGTH);
			long utc=in.readLong();
			this.lastValidatedTransactionsUTCForDestinationHost.put(packageString, utc);
		}

	}
}
