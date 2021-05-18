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
import com.distrimind.ood.database.EncryptionTools;
import com.distrimind.ood.database.Table;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.io.*;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class BackupChannelUpdateMessageFromCentralDatabaseBackup extends DatabaseEvent implements MessageComingFromCentralDatabaseBackup, SecureExternalizable {

	private DecentralizedValue hostDestination;
	private DecentralizedValue hostChannel;
	private byte[] lastValidatedAndEncryptedLocalID;
	private byte[] lastValidatedAndEncryptedDistantID;
	private String databasePackage;

	protected BackupChannelUpdateMessageFromCentralDatabaseBackup() {
	}

	public BackupChannelUpdateMessageFromCentralDatabaseBackup(DecentralizedValue hostDestination, DecentralizedValue hostChannel, String databasePackage, byte[] lastValidatedAndEncryptedLocalID, byte[] lastValidatedAndEncryptedDistantID) {
		if (hostChannel==null)
			throw new NullPointerException();
		if (hostDestination==null)
			throw new NullPointerException();
		if (lastValidatedAndEncryptedLocalID ==null)
			throw new NullPointerException();
		if (lastValidatedAndEncryptedDistantID ==null)
			throw new NullPointerException();
		if (databasePackage==null)
			throw new NullPointerException();
		this.hostDestination = hostDestination;
		this.hostChannel = hostChannel;
		this.lastValidatedAndEncryptedLocalID = lastValidatedAndEncryptedLocalID;
		this.lastValidatedAndEncryptedDistantID = lastValidatedAndEncryptedDistantID;
		this.databasePackage=databasePackage;
	}

	public String getDatabasePackage() {
		return databasePackage;
	}

	@Override
	public DecentralizedValue getHostDestination() {
		return hostDestination;
	}

	public DecentralizedValue getHostChannel() {
		return hostChannel;
	}

	public byte[] getLastValidatedAndEncryptedLocalID() {
		return lastValidatedAndEncryptedLocalID;
	}

	public byte[] getLastValidatedAndEncryptedDistantID() {
		return lastValidatedAndEncryptedDistantID;
	}

	public long getLastValidatedLocalID(EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		return lastValidatedAndEncryptedLocalID==null?Long.MIN_VALUE:EncryptionTools.decryptID(encryptionProfileProvider, lastValidatedAndEncryptedLocalID);
	}
	public long getLastValidatedDistantID(EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		return lastValidatedAndEncryptedDistantID==null?Long.MIN_VALUE:EncryptionTools.decryptID(encryptionProfileProvider, lastValidatedAndEncryptedDistantID);
	}

	@Override
	public int getInternalSerializedSize() {
		return SerializationTools.getInternalSize((SecureExternalizable)hostDestination)
				+SerializationTools.getInternalSize((SecureExternalizable)hostChannel)
				+SerializationTools.getInternalSize(lastValidatedAndEncryptedLocalID, EncryptionTools.MAX_ENCRYPTED_ID_SIZE)
				+SerializationTools.getInternalSize(lastValidatedAndEncryptedDistantID, EncryptionTools.MAX_ENCRYPTED_ID_SIZE)
				+SerializationTools.getInternalSize(databasePackage, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostDestination, false);
		out.writeObject(hostChannel, false);
		out.writeBytesArray(lastValidatedAndEncryptedLocalID, false, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
		out.writeBytesArray(lastValidatedAndEncryptedDistantID, false, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
		out.writeString(databasePackage, false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		hostDestination=in.readObject(false, DecentralizedValue.class);
		hostChannel=in.readObject(false, DecentralizedValue.class);
		if (hostChannel.equals(hostDestination))
			throw new MessageExternalizationException(Integrity.FAIL);
		lastValidatedAndEncryptedLocalID=in.readBytesArray(false, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
		lastValidatedAndEncryptedDistantID=in.readBytesArray(false, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
		databasePackage=in.readString(false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
	}

	@Override
	public boolean cannotBeMerged() {
		return true;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName()+"{" +
				"hostDestination=" + hostDestination +
				", hostChannel=" + hostChannel +
				'}';
	}
}
