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
package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.crypto.SymmetricAuthenticatedSignatureType;
import com.distrimind.util.io.*;

import java.io.IOException;
import java.util.Objects;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class HookRemoveRequest extends DatabaseEvent implements AuthenticatedP2PMessage {
	//static final int MAX_HOOK_ADD_REQUEST_LENGTH_IN_BYTES=DecentralizedValue.MAX_SIZE_IN_BYTES_OF_DECENTRALIZED_VALUE*3+SymmetricAuthenticatedSignatureType.MAX_SYMMETRIC_SIGNATURE_SIZE+3;

	private DecentralizedValue removedHookID, hostDestination, hostSource;
	private short encryptionProfileIdentifier;
	private byte[] symmetricSignature;
	private long messageID;

	@SuppressWarnings("unused")
	private HookRemoveRequest() {
	}

	HookRemoveRequest(DecentralizedValue hostSource, DecentralizedValue hostDestination, DecentralizedValue removedHookID) {
		if (hostSource==null)
			throw new NullPointerException();
		if (hostDestination==null)
			throw new NullPointerException();
		if (removedHookID==null)
			throw new NullPointerException();
		if (hostDestination.equals(hostSource))
			throw new IllegalArgumentException();
		this.hostSource=hostSource;
		this.hostDestination=hostDestination;
		this.removedHookID = removedHookID;
	}

	@Override
	public void updateSignature(EncryptionProfileProvider encryptionProfileProvider) throws DatabaseException {
		if (encryptionProfileProvider==null)
			throw new NullPointerException();
		this.encryptionProfileIdentifier = encryptionProfileProvider.getDefaultKeyID();
		this.symmetricSignature=sign(encryptionProfileProvider);
	}

	@Override
	public long getMessageID() {
		return messageID;
	}

	@Override
	public void setMessageID(long messageID) {
		if (messageID<0)
			throw new IllegalArgumentException();
		this.messageID=messageID;
	}
	@Override
	public void writeExternalWithoutSignature(SecuredObjectOutputStream out) throws IOException {
		out.writeLong(messageID);
		out.writeObject(hostSource, false);
		out.writeObject(hostDestination, false);
		out.writeObject(removedHookID, false);
		out.writeShort(encryptionProfileIdentifier);
	}



	@Override
	public DecentralizedValue getHostDestination() {
		return hostDestination;
	}

	@Override
	public DecentralizedValue getHostSource() {
		return hostSource;
	}

	@Override
	public int getInternalSerializedSize() {
		return 10+ SerializationTools.getInternalSize((SecureExternalizable)hostSource)
				+SerializationTools.getInternalSize((SecureExternalizable)hostDestination)
				+SerializationTools.getInternalSize((SecureExternalizable)removedHookID);
	}

	@Override
	public void writeExternalSignature(SecuredObjectOutputStream out) throws IOException {
		out.writeBytesArray(symmetricSignature, false, SymmetricAuthenticatedSignatureType.MAX_SYMMETRIC_SIGNATURE_SIZE);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		messageID=in.readLong();
		if (messageID<0)
			throw new MessageExternalizationException(Integrity.FAIL_AND_CANDIDATE_TO_BAN);
		hostSource=in.readObject(false, DecentralizedValue.class);
		hostDestination=in.readObject(false, DecentralizedValue.class);
		if (hostDestination.equals(hostSource))
			throw new MessageExternalizationException(Integrity.FAIL);
		removedHookID=in.readObject(false, DecentralizedValue.class);
		encryptionProfileIdentifier=in.readShort();
		symmetricSignature=in.readBytesArray(false, SymmetricAuthenticatedSignatureType.MAX_SYMMETRIC_SIGNATURE_SIZE);
	}

	@Override
	public byte[] getSymmetricSignature() {
		return symmetricSignature;
	}

	@Override
	public short getEncryptionProfileIdentifier() {
		return encryptionProfileIdentifier;
	}

	public DecentralizedValue getRemovedHookID() {
		return removedHookID;
	}
	@Override
	public DatabaseEvent.MergeState tryToMergeWithNewAuthenticatedMessage(DatabaseEvent newEvent) {
		if (newEvent instanceof AuthenticatedP2PMessage)
		{
			if (newEvent instanceof AbstractHookRequest)
			{
				AbstractHookRequest ne=(AbstractHookRequest)newEvent;
				if (ne.getConcernedPeers().contains(removedHookID))
					return MergeState.DELETE_OLD;
				else if (removedHookID.equals(ne.getHostSource()) || removedHookID.equals(ne.getHostDestination()))
					return MergeState.DELETE_NEW;
				else
					return MergeState.NO_FUSION;
			}
			else if (newEvent instanceof HookRemoveRequest)
			{
				HookRemoveRequest hrr=(HookRemoveRequest)newEvent;
				if (this.removedHookID.equals(this.hostDestination) && this.removedHookID.equals(hrr.getHostDestination()) || this.removedHookID.equals(hrr.hostSource))
					return MergeState.DELETE_NEW;
				else
					return MergeState.NO_FUSION;
			}
			else
				throw new IllegalAccessError();
		}
		else
			return MergeState.NO_FUSION;
	}

	@Override
	public void messageSentImpl(DatabaseWrapper wrapper) throws DatabaseException {
		if (this.removedHookID.equals(this.hostDestination))
		{
			wrapper.getSynchronizer().removeHook(this.hostDestination);
		}
	}



	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		HookRemoveRequest that = (HookRemoveRequest) o;
		return messageID == that.messageID;
	}

	@Override
	public int hashCode() {
		return Objects.hash(messageID);
	}
}
