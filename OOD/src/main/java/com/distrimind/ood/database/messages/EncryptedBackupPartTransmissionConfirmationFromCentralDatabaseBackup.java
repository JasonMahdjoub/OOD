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
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.*;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup extends DatabaseEvent implements MessageComingFromCentralDatabaseBackup, SecureExternalizable {

	private long fileUTC;
	private long lastTransactionUTC;
	private DecentralizedValue hostDestination;
	private String packageString;

	@SuppressWarnings("unused")
	private EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup()
	{

	}

	public EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup(DecentralizedValue hostDestination, long fileUTC, long lastTransactionUTC, String packageString) {
		if (hostDestination==null)
			throw new NullPointerException();
		if (fileUTC>lastTransactionUTC)
			throw new IllegalArgumentException();
		if (packageString==null)
			throw new NullPointerException();
		if (packageString.trim().length()==0)
			throw new IllegalArgumentException();
		this.hostDestination=hostDestination;
		this.fileUTC = fileUTC;
		this.lastTransactionUTC=lastTransactionUTC;
		this.packageString=packageString;
	}

	public long getFileUTC() {
		return fileUTC;
	}

	@Override
	public DecentralizedValue getHostDestination() {
		return hostDestination;
	}

	public long getLastTransactionUTC() {
		return lastTransactionUTC;
	}

	@Override
	public int getInternalSerializedSize() {
		return 16+ SerializationTools.getInternalSize((SecureExternalizable)hostDestination)+SerializationTools.getInternalSize(packageString, SerializationTools.MAX_CLASS_LENGTH);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostDestination, false);
		out.writeLong(fileUTC);
		out.writeLong(lastTransactionUTC);
		out.writeString(packageString, false, SerializationTools.MAX_CLASS_LENGTH);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		hostDestination=in.readObject(false, DecentralizedValue.class);
		fileUTC=in.readLong();
		lastTransactionUTC=in.readLong();
		packageString=in.readString(false, SerializationTools.MAX_CLASS_LENGTH);
		if (packageString.trim().length()==0)
			throw new MessageExternalizationException(Integrity.FAIL);
	}

	public String getPackageString() {
		return packageString;
	}

	@Override
	public boolean cannotBeMerged() {
		return true;
	}
}
