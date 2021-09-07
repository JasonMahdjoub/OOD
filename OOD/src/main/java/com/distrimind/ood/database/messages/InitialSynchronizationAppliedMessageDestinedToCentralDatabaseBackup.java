package com.distrimind.ood.database.messages;
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

import com.distrimind.ood.database.DatabaseEvent;
import com.distrimind.ood.database.EncryptionTools;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.centraldatabaseapi.CentralDatabaseBackupCertificate;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.io.*;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.1.0
 */
public class InitialSynchronizationAppliedMessageDestinedToCentralDatabaseBackup extends AuthenticatedMessageDestinedToCentralDatabaseBackup {

	private DecentralizedValue clonedHost;

	private long lastLocalTransactionUTC;
	private byte[] lastEncryptedLocalTransactionID;
	private String packageString;

	public InitialSynchronizationAppliedMessageDestinedToCentralDatabaseBackup(DecentralizedValue hostSource, DecentralizedValue clonedHost, CentralDatabaseBackupCertificate certificate, String packageString, long lastLocalTransactionUTC, long lastLocalTransactionID, AbstractSecureRandom random, EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		super(hostSource, certificate);
		this.clonedHost = clonedHost;
		this.lastLocalTransactionUTC = lastLocalTransactionUTC;
		this.lastEncryptedLocalTransactionID = EncryptionTools.encryptID(lastLocalTransactionID, random, encryptionProfileProvider);
		this.packageString=packageString;
	}

	protected InitialSynchronizationAppliedMessageDestinedToCentralDatabaseBackup() {
	}


	@Override
	public void writeExternalWithoutSignatures(SecuredObjectOutputStream out) throws IOException {
		super.writeExternalWithoutSignatures(out);
		out.writeObject(clonedHost, false);
		out.writeLong(lastLocalTransactionUTC);
		out.writeBytesArray(lastEncryptedLocalTransactionID, false, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
		out.writeString(packageString, false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
	}

	@Override
	public void readExternalWithoutSignatures(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		super.readExternalWithoutSignatures(in);
		clonedHost=in.readObject(false);
		if (getHostSource().equals(clonedHost))
			throw new MessageExternalizationException(Integrity.FAIL);
		lastLocalTransactionUTC =in.readLong();
		lastEncryptedLocalTransactionID=in.readBytesArray(false, EncryptionTools.MAX_ENCRYPTED_ID_SIZE);
		packageString=in.readObject(false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
	}

	@Override
	public int getInternalSerializedSizeWithoutSignatures() {
		return 8+super.getInternalSerializedSizeWithoutSignatures()
				+SerializationTools.getInternalSize(clonedHost)
				+SerializationTools.getInternalSize(lastEncryptedLocalTransactionID, EncryptionTools.MAX_ENCRYPTED_ID_SIZE)
				+SerializationTools.getInternalSize(packageString, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof InitialSynchronizationAppliedMessageDestinedToCentralDatabaseBackup)
		{
			InitialSynchronizationAppliedMessageDestinedToCentralDatabaseBackup i=(InitialSynchronizationAppliedMessageDestinedToCentralDatabaseBackup)o;
			return i.getHostSource().equals(getHostSource())
					&& i.getClonedHost().equals(getClonedHost())
					&& i.getPackageString().equals(getPackageString());
		}
		return false;
	}

	@Override
	public boolean cannotBeMerged() {
		return false;
	}


	public DecentralizedValue getClonedHost() {
		return clonedHost;
	}

	public long getLastLocalTransactionUTC() {
		return lastLocalTransactionUTC;
	}

	public byte[] getLastEncryptedLocalTransactionID() {
		return lastEncryptedLocalTransactionID;
	}

	@Override
	public DatabaseEvent.MergeState mergeWithP2PDatabaseEventToSend(DatabaseEvent newEvent) throws DatabaseException {
		DatabaseEvent.MergeState res= DatabaseEventToSend.mergeWithP2PDatabaseEventToSend(getHostSource(), null, newEvent);
		if (res!=MergeState.NO_FUSION)
			return res;
		if (newEvent instanceof InitialSynchronizationAppliedMessageDestinedToCentralDatabaseBackup)
		{
			InitialSynchronizationAppliedMessageDestinedToCentralDatabaseBackup ne=(InitialSynchronizationAppliedMessageDestinedToCentralDatabaseBackup)newEvent;
			if (ne.packageString.equals(packageString) && (ne.getHostSource().equals(getHostSource()) || ne.getClonedHost().equals(getClonedHost())))
			{
				throw new DatabaseException("Got two messages that should be unique");
			}
		}
		return DatabaseEvent.MergeState.NO_FUSION;
	}

	public String getPackageString() {
		return packageString;
	}
}
