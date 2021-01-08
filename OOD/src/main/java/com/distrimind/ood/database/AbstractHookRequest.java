
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
import java.util.Set;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
public abstract class AbstractHookRequest extends DatabaseEvent implements AuthenticatedP2PMessage {



	static final int MAX_PEERS_DESCRIPTION_SIZE_IN_BYTES= DatabaseWrapper.MAX_ACCEPTED_SIZE_IN_BYTES_OF_DECENTRALIZED_VALUE *DatabaseWrapper.MAX_DISTANT_PEERS;
	static final int MAX_PACKAGE_ENCODING_SIZE_IN_BYTES=(DatabaseHooksTable.PACKAGES_TO_SYNCHRONIZE_LENGTH+1)*2+10;
	static final int MAX_HOOK_ADD_REQUEST_LENGTH_IN_BYTES=MAX_PACKAGE_ENCODING_SIZE_IN_BYTES+DatabaseWrapper.MAX_ACCEPTED_SIZE_IN_BYTES_OF_DECENTRALIZED_VALUE*2+MAX_PEERS_DESCRIPTION_SIZE_IN_BYTES+SymmetricAuthenticatedSignatureType.MAX_SYMMETRIC_SIGNATURE_SIZE+5;
	private DecentralizedValue hostSource;
	private DecentralizedValue hostDestination;
	//private DecentralizedValue hostAdded;

	protected Set<DecentralizedValue> concernedPeers;

	private byte[] symmetricSignature=null;
	private short encryptionProfileIdentifier;
	private long messageID;
	private transient DatabaseWrapper databaseWrapper=null;


	@Override
	public int getInternalSerializedSize() {
		return 18+SerializationTools.getInternalSize(hostSource, 0)
				+SerializationTools.getInternalSize(hostDestination, 0)
				+SerializationTools.getInternalSize(symmetricSignature, SymmetricAuthenticatedSignatureType.MAX_SYMMETRIC_SIGNATURE_SIZE)
				+SerializationTools.getInternalSize(concernedPeers, MAX_PEERS_DESCRIPTION_SIZE_IN_BYTES);

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
		out.writeCollection(concernedPeers, false, MAX_PEERS_DESCRIPTION_SIZE_IN_BYTES, false);
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
		if (hostSource.equals(hostDestination))
			throw new MessageExternalizationException(Integrity.FAIL);
		try {
			concernedPeers =in.readCollection(false,MAX_PEERS_DESCRIPTION_SIZE_IN_BYTES, false, DecentralizedValue.class);
		}
		catch (ClassCastException e)
		{
			throw new MessageExternalizationException(Integrity.FAIL_AND_CANDIDATE_TO_BAN, e);
		}
		encryptionProfileIdentifier =in.readShort();
		symmetricSignature=in.readBytesArray(false, SymmetricAuthenticatedSignatureType.MAX_SYMMETRIC_SIGNATURE_SIZE);
	}

	public Set<DecentralizedValue> getConcernedPeers() {
		return concernedPeers;
	}

	AbstractHookRequest()
	{

	}

	AbstractHookRequest(DecentralizedValue _hostSource, DecentralizedValue _hostDestination,
						Set<DecentralizedValue> concernedPeers) {
		super();
		if (_hostSource == null)
			throw new NullPointerException();
		if (_hostDestination == null)
			throw new NullPointerException();
		if (concernedPeers ==null)
			throw new NullPointerException();

		hostSource = _hostSource;
		hostDestination = _hostDestination;
		this.concernedPeers = concernedPeers;
	}

	@Override
	public void updateSignature(EncryptionProfileProvider encryptionProfileProvider) throws DatabaseException {
		if (encryptionProfileProvider==null)
			throw new NullPointerException();
		this.encryptionProfileIdentifier =encryptionProfileProvider.getDefaultKeyID();
		this.symmetricSignature=sign(encryptionProfileProvider);
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
	public DatabaseEvent.MergeState tryToMergeWithNewAuthenticatedMessage(DatabaseEvent newEvent) {

		if (newEvent instanceof AuthenticatedP2PMessage)
		{
			if (newEvent instanceof AbstractHookRequest)
			{
				AbstractHookRequest nhar=(AbstractHookRequest)newEvent;
				if (nhar.hostDestination.equals(this.hostDestination))
				{
					if (nhar instanceof HookSynchronizeRequest) {
						if (this instanceof HookSynchronizeRequest && ((HookSynchronizeRequest) this).packagesToSynchronize.keySet().equals(((HookSynchronizeRequest) nhar).packagesToSynchronize.keySet()))
							return MergeState.DELETE_OLD;
						else
							return MergeState.NO_FUSION;
					}
					else if (nhar instanceof HookUnsynchronizeRequest) {
						if (this instanceof HookUnsynchronizeRequest && ((HookUnsynchronizeRequest) this).packagesToUnsynchronize.equals(((HookUnsynchronizeRequest) nhar).packagesToUnsynchronize))
							return MergeState.DELETE_OLD;
						else
							return MergeState.NO_FUSION;
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
				if (nhrr.getRemovedHookID().equals(getHostDestination()) && concernedPeers.contains(nhrr.getRemovedHookID()))
					return MergeState.DELETE_OLD;
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
	public DatabaseWrapper getDatabaseWrapper() {
		return databaseWrapper;
	}

	@Override
	public void setDatabaseWrapper(DatabaseWrapper databaseWrapper) {
		this.databaseWrapper = databaseWrapper;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		AbstractHookRequest that = (AbstractHookRequest) o;
		return messageID == that.messageID;
	}

	@Override
	public int hashCode() {
		return Objects.hash(messageID);
	}
}
