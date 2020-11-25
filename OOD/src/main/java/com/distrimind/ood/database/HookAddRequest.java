
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
import java.util.HashSet;
import java.util.Set;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public class HookAddRequest extends DatabaseEvent implements AuthenticatedP2PMessage {

	static final int MAX_HOOK_ADD_REQUEST_LENGTH_IN_BYTES=DatabaseHooksTable.PACKAGES_TO_SYNCHRONIZE_LENGTH*2+DecentralizedValue.MAX_SIZE_IN_BYTES_OF_DECENTRALIZED_VALUE*(2+DatabaseWrapper.MAX_DISTANT_PEERS)+SymmetricAuthenticatedSignatureType.MAX_SYMMETRIC_SIGNATURE_SIZE+5;
	private DecentralizedValue hostSource;
	private DecentralizedValue hostDestination;
	private DecentralizedValue hostAdded;
	private Set<String> packagesToSynchronize;

	private boolean mustReturnMessage;
	private boolean replaceDistantConflictualData;
	private byte[] symmetricSignature=null;
	private short encryptionProfileIdentifier;
	private long messageID;
	private transient DatabaseWrapper databaseWrapper=null;


	@Override
	public int getInternalSerializedSize() {
		int res=20+SerializationTools.getInternalSize(hostSource, 0)
				+SerializationTools.getInternalSize(hostDestination, 0)
				+SerializationTools.getInternalSize(hostAdded, 0)
				+SerializationTools.getInternalSize(symmetricSignature, SymmetricAuthenticatedSignatureType.MAX_SYMMETRIC_SIGNATURE_SIZE);
		for (String s : packagesToSynchronize)
			res+=SerializationTools.getInternalSize(s, SerializationTools.MAX_CLASS_LENGTH);

		return res;
	}

	@Override
	public byte[] getSymmetricSignature() {
		return symmetricSignature;
	}

	@Override
	public short getEncryptionProfileIdentifier() {
		return encryptionProfileIdentifier;
	}

	@Override
	public void writeExternalWithoutSignature(SecuredObjectOutputStream out) throws IOException {
		out.writeLong(messageID);
		out.writeObject(hostSource, false);
		out.writeObject(hostDestination, false);
		out.writeObject(hostAdded, false);
		out.writeInt(packagesToSynchronize.size());
		for (String p : packagesToSynchronize)
			out.writeString(p, false, SerializationTools.MAX_CLASS_LENGTH);
		out.writeBoolean(mustReturnMessage);
		out.writeBoolean(replaceDistantConflictualData);
		out.writeShort(encryptionProfileIdentifier);
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
		hostAdded=in.readObject(false, DecentralizedValue.class);
		int s=in.readInt();
		if (s<1 || s>DatabaseWrapper.MAX_PACKAGE_TO_SYNCHRONIZE)
			throw new MessageExternalizationException(Integrity.FAIL_AND_CANDIDATE_TO_BAN, ""+s);
		packagesToSynchronize=new HashSet<>(s);
		for (int i=0;i<s;i++)
			packagesToSynchronize.add(in.readString(false, SerializationTools.MAX_CLASS_LENGTH));
		mustReturnMessage=in.readBoolean();
		replaceDistantConflictualData=in.readBoolean();
		encryptionProfileIdentifier =in.readShort();
		symmetricSignature=in.readBytesArray(false, SymmetricAuthenticatedSignatureType.MAX_SYMMETRIC_SIGNATURE_SIZE);
	}

	@SuppressWarnings("unused")
	HookAddRequest()
	{

	}

	HookAddRequest(DecentralizedValue _hostSource, DecentralizedValue _hostDestination, DecentralizedValue hostAdded,
			Set<String> packagesToSynchronize,
			boolean mustReturnMessage, boolean replaceDistantConflictualData) {
		super();
		if (_hostSource == null)
			throw new NullPointerException();
		if (_hostDestination == null)
			throw new NullPointerException();
		if (hostAdded ==null)
			throw new NullPointerException();

		hostSource = _hostSource;
		hostDestination = _hostDestination;
		this.hostAdded = hostAdded;
		this.packagesToSynchronize = packagesToSynchronize;

		this.replaceDistantConflictualData = replaceDistantConflictualData;
		this.mustReturnMessage = mustReturnMessage;
	}

	@Override
	public void updateSignature(EncryptionProfileProvider encryptionProfileProvider) throws DatabaseException {
		if (encryptionProfileProvider==null)
			throw new NullPointerException();
		this.encryptionProfileIdentifier =encryptionProfileProvider.getDefaultKeyID();
		this.symmetricSignature=sign(encryptionProfileProvider);
	}

	public DecentralizedValue getHostAdded() {
		return hostAdded;
	}

	@Override
	public DecentralizedValue getHostDestination() {
		return hostDestination;
	}

	@Override
	public DecentralizedValue getHostSource() {
		return hostSource;
	}

	public boolean mustReturnsMessage() {
		return mustReturnMessage;
	}

	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	public boolean isReplaceDistantConflictualData() {
		return replaceDistantConflictualData;
	}

	public Set<String> getPackagesToSynchronize() {
		return packagesToSynchronize;
	}

	@Override
	public DatabaseEvent.MergeState tryToMergeWithNewAuthenticatedMessage(DatabaseEvent newEvent) {

		if (newEvent instanceof AuthenticatedP2PMessage)
		{
			if (newEvent instanceof HookAddRequest)
			{
				HookAddRequest nhar=(HookAddRequest)newEvent;
				if (nhar.hostDestination.equals(this.hostDestination))
				{
					if (nhar.hostAdded.equals(this.hostAdded)) {
						return MergeState.DELETE_NEW;
					}
					else
						return MergeState.NO_FUSION;
				}
				else
					return MergeState.NO_FUSION;
			}
			else if (newEvent instanceof HookRemoveRequest)
			{
				HookRemoveRequest nhrr=(HookRemoveRequest)newEvent;
				if (nhrr.getRemovedHookID().equals(getHostAdded()))
					return MergeState.DELETE_BOTH;
				else if (nhrr.getRemovedHookID().equals(hostDestination) || nhrr.getRemovedHookID().equals(hostAdded))
				{
					return MergeState.DELETE_OLD;
				}
				else
				{
					return MergeState.NO_FUSION;
				}
			}
			else
				throw new IllegalAccessError();
		}
		else
			return MergeState.NO_FUSION;
	}

	@Override
	public void messageSent() throws DatabaseException {
		if (this.hostAdded.equals(this.hostDestination))
		{
			getDatabaseWrapper().getSynchronizer().removeHook(this.hostDestination);
		}
	}

	@Override
	public DatabaseWrapper getDatabaseWrapper() {
		return databaseWrapper;
	}

	@Override
	public void setDatabaseWrapper(DatabaseWrapper databaseWrapper) {
		this.databaseWrapper = databaseWrapper;
	}
}
