package com.distrimind.ood.database.messages;
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

import com.distrimind.ood.database.DatabaseEvent;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.EncryptionTools;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.io.*;

import java.io.IOException;

/**
 * Last validated distant transaction destined to central database backup
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class LastValidatedDistantTransactionDestinedToCentralDatabaseBackup extends DatabaseEvent implements MessageDestinedToCentralDatabaseBackup, SecureExternalizable {
	private DecentralizedValue hostSource;
	private DecentralizedValue channelHost;
	private byte[] encryptedLastValidatedDistantID;

	@SuppressWarnings("unused")
	private LastValidatedDistantTransactionDestinedToCentralDatabaseBackup()
	{

	}

	public LastValidatedDistantTransactionDestinedToCentralDatabaseBackup(DecentralizedValue hostSource, DecentralizedValue channelHost, long lastValidatedDistantID, AbstractSecureRandom random, EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		if (hostSource==null)
			throw new NullPointerException();
		if (channelHost==null)
			throw new NullPointerException();
		if (hostSource.equals(channelHost))
			throw new IllegalArgumentException();
		this.hostSource = hostSource;
		this.channelHost = channelHost;
		assert lastValidatedDistantID!=Long.MIN_VALUE;
		this.encryptedLastValidatedDistantID = EncryptionTools.encryptID(lastValidatedDistantID, random, encryptionProfileProvider);
	}

	@Override
	public DecentralizedValue getHostSource() {
		return hostSource;
	}

	@Override
	public boolean cannotBeMerged() {
		return false;
	}

	public DecentralizedValue getChannelHost() {
		return channelHost;
	}

	public byte[] getEncryptedLastValidatedDistantID() {
		return encryptedLastValidatedDistantID;
	}

	@Override
	public int getInternalSerializedSize() {
		return SerializationTools.getInternalSize(hostSource)
				+SerializationTools.getInternalSize(channelHost)
				+SerializationTools.getInternalSize(encryptedLastValidatedDistantID, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostSource, false);
		out.writeObject(channelHost, false);
		out.writeBytesArray(encryptedLastValidatedDistantID, false, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		hostSource=in.readObject(false, DecentralizedValue.class);
		channelHost=in.readObject(false, DecentralizedValue.class);
		if (hostSource.equals(channelHost))
			throw new MessageExternalizationException(Integrity.FAIL);
		encryptedLastValidatedDistantID=in.readBytesArray(false, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
	}

	@Override
	public String toString() {
		return "LastValidatedDistantTransactionDestinedToCentralDatabaseBackup{" +
				"hostSource=" + DatabaseWrapper.toString(hostSource) +
				", channelHost=" + DatabaseWrapper.toString(channelHost) +
				'}';
	}
}
