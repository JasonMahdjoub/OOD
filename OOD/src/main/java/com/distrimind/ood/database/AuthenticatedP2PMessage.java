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
import com.distrimind.ood.database.messages.AuthenticatedMessage;
import com.distrimind.ood.database.messages.P2PDatabaseEventToSend;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.crypto.SymmetricAuthenticatedSignatureCheckerAlgorithm;
import com.distrimind.util.crypto.SymmetricAuthenticatedSignerAlgorithm;
import com.distrimind.util.io.*;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public interface AuthenticatedP2PMessage extends P2PDatabaseEventToSend, AuthenticatedMessage {
	int MAX_NUMBER_OF_AUTHENTICATED_P2P_MESSAGES_PER_PEER =18;
	int MAX_AUTHENTICATED_P2P_MESSAGE_SIZE_IN_BYTES=AbstractHookRequest.MAX_HOOK_ADD_REQUEST_LENGTH_IN_BYTES+4;

	@Override
	default byte[] sign(EncryptionProfileProvider encryptionProfileProvider) throws DatabaseException {
		try {
			SymmetricAuthenticatedSignerAlgorithm signer = new SymmetricAuthenticatedSignerAlgorithm(encryptionProfileProvider.getSecretKeyForSignature(getEncryptionProfileIdentifier(), false));
			signer.init();
			try(SignerRandomOutputStream out=new SignerRandomOutputStream(new NullRandomOutputStream(), signer)) {
				writeExternalWithoutSignature(out);
			}
			return signer.getSignature();
		} catch (NoSuchAlgorithmException | NoSuchProviderException | IOException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}



	@Override
	default void writeExternal(SecuredObjectOutputStream out) throws IOException
	{
		AuthenticatedMessage.super.writeExternal(out);
		try {
			DatabaseHooksTable.Record concernedDestinationHook=getDatabaseWrapper().getDatabaseHooksTable().authenticatedMessageSent(this);
			messageSent();
			if (concernedDestinationHook!=null && !(this instanceof HookRemoveRequest) && (concernedDestinationHook.getAuthenticatedMessagesQueueToSend()==null || concernedDestinationHook.getAuthenticatedMessagesQueueToSend().isEmpty()))
			{
				getDatabaseWrapper().getSynchronizer().connectPeerIfAvailable(concernedDestinationHook);
			}
		} catch (DatabaseException e) {
			throw new IOException(e);
		}
	}

	@Override
	default boolean cannotBeMerged()
	{
		return false;
	}

	@Override
	default boolean checkAuthentication(EncryptionProfileProvider profileProvider) throws DatabaseException {
		try {
			SymmetricAuthenticatedSignatureCheckerAlgorithm checker=new SymmetricAuthenticatedSignatureCheckerAlgorithm(profileProvider.getSecretKeyForSignature(this.getEncryptionProfileIdentifier(), true));
			try(RandomByteArrayOutputStream out=new RandomByteArrayOutputStream())
			{
				writeExternalWithoutSignature(out);
				out.flush();
				return checker.verify(out.getBytes(), getSymmetricSignature());
			}
		} catch (NoSuchAlgorithmException | NoSuchProviderException | IOException e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	byte[] getSymmetricSignature();





	long getMessageID();

	void setMessageID(long messageID);

	DatabaseEvent.MergeState tryToMergeWithNewAuthenticatedMessage(DatabaseEvent newEvent) throws DatabaseException ;

	DatabaseWrapper getDatabaseWrapper();
	void setDatabaseWrapper(DatabaseWrapper wrapper);

	@SuppressWarnings("RedundantThrows")
	default void messageReadyToSend() throws DatabaseException {

	}
	default void messageSent() throws DatabaseException
	{

	}


}
