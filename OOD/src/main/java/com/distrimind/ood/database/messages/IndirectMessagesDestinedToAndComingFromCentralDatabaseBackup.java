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

import com.distrimind.ood.database.AuthenticatedP2PMessage;
import com.distrimind.ood.database.DatabaseEvent;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.*;
import com.distrimind.util.io.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup extends DatabaseEvent implements MessageDestinedToCentralDatabaseBackup, MessageComingFromCentralDatabaseBackup, SecureExternalizable {

	public static final int MAX_AUTHENTICATED_P2P_MESSAGE_SIZE_IN_BYTES= SymmetricAuthenticatedSignatureType.MAX_SYMMETRIC_SIGNATURE_SIZE+HybridASymmetricAuthenticatedSignatureType.MAX_HYBRID_ASYMMETRIC_SIGNATURE_SIZE+MessageDigestType.MAX_HASH_LENGTH+AuthenticatedP2PMessage.MAX_AUTHENTICATED_P2P_MESSAGE_SIZE_IN_BYTES+31+SymmetricEncryptionType.MAX_IV_SIZE_IN_BYTES;
	public static final int SIZE_IN_BYTES_AUTHENTICATED_MESSAGES_QUEUE_TO_SEND =MAX_AUTHENTICATED_P2P_MESSAGE_SIZE_IN_BYTES*AuthenticatedP2PMessage.MAX_NUMBER_OF_AUTHENTICATED_P2P_MESSAGES_PER_PEER +4;
	public static final int MAX_NUMBER_OF_P2P_MESSAGES_PER_PEER=AuthenticatedP2PMessage.MAX_NUMBER_OF_AUTHENTICATED_P2P_MESSAGES_PER_PEER;//(int)(SIZE_IN_BYTES_AUTHENTICATED_MESSAGES_QUEUE_TO_SEND/MAX_AUTHENTICATED_P2P_MESSAGE_SIZE_IN_BYTES);
	private DecentralizedValue hostSource;
	private DecentralizedValue hostDestination;
	private List<byte[]> encryptedAuthenticatedP2PMessages;
	private transient List<AuthenticatedP2PMessage> authenticatedP2PMessages;

	@SuppressWarnings("unused")
	private IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup()
	{

	}
	public IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup(DecentralizedValue hostSource, DecentralizedValue hostDestination, List<byte[]> encryptedAuthenticatedP2PMessages) {
		if (hostSource==null)
			throw new NullPointerException();
		if (hostDestination==null)
			throw new NullPointerException();
		if (encryptedAuthenticatedP2PMessages==null)
			throw new NullPointerException();
		if (encryptedAuthenticatedP2PMessages.contains(null))
			throw new NullPointerException();
		this.hostSource=hostSource;
		this.hostDestination=hostDestination;
		this.encryptedAuthenticatedP2PMessages=encryptedAuthenticatedP2PMessages;
		authenticatedP2PMessages=null;
	}
	public IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup(List<AuthenticatedP2PMessage> authenticatedP2PMessages, AbstractSecureRandom random, EncryptionProfileProvider encryptionProfileProvider) throws DatabaseException {
		if (authenticatedP2PMessages==null)
			throw new NullPointerException();
		if (authenticatedP2PMessages.size()==0)
			throw new IllegalArgumentException();
		if (authenticatedP2PMessages.size()>MAX_NUMBER_OF_P2P_MESSAGES_PER_PEER)
			throw new IllegalArgumentException();

		this.hostSource = null;
		this.authenticatedP2PMessages = authenticatedP2PMessages;
		this.hostDestination=null;
		for (AuthenticatedP2PMessage a : authenticatedP2PMessages)
		{
			if (a==null)
				throw new NullPointerException();

			if (this.hostDestination==null) {
				this.hostDestination = a.getHostDestination();
				this.hostSource = a.getHostSource();
			}
			else if (!this.hostDestination.equals(a.getHostDestination()))
				throw new IllegalArgumentException();
			else if (!this.hostSource.equals(a.getHostSource()))
				throw new IllegalArgumentException();
		}
		this.encryptedAuthenticatedP2PMessages=new ArrayList<>(authenticatedP2PMessages.size());
		for (AuthenticatedP2PMessage a : authenticatedP2PMessages)
		{


			try(RandomByteArrayOutputStream out=new RandomByteArrayOutputStream();
				RandomByteArrayOutputStream eout=new RandomByteArrayOutputStream())
			{
				out.writeObject(a, false);
				out.flush();
				EncryptionSignatureHashEncoder encoder=new EncryptionSignatureHashEncoder()
						.withEncryptionProfileProvider(random, encryptionProfileProvider)
						.withRandomInputStream(out.getRandomInputStream());
				encoder.encode(eout);
				eout.flush();
				this.encryptedAuthenticatedP2PMessages.add(eout.getBytes());
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}

		}
	}

	public List<byte[]> getEncryptedAuthenticatedP2PMessages() {
		return encryptedAuthenticatedP2PMessages;
	}

	@Override
	public DecentralizedValue getHostSource() {
		return hostSource;
	}

	@Override
	public DecentralizedValue getHostDestination()
	{
		return hostDestination;
	}

	@Override
	public boolean cannotBeMerged() {
		return true;
	}

	@Override
	public int getInternalSerializedSize() {
		return SerializationTools.getInternalSize(hostSource, 0)
				+SerializationTools.getInternalSize(hostDestination, 0)
				+SerializationTools.getInternalSize(encryptedAuthenticatedP2PMessages, SIZE_IN_BYTES_AUTHENTICATED_MESSAGES_QUEUE_TO_SEND);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostSource, false);
		out.writeObject(hostDestination, false);
		out.writeCollection(encryptedAuthenticatedP2PMessages, false, SIZE_IN_BYTES_AUTHENTICATED_MESSAGES_QUEUE_TO_SEND,false);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		hostSource=in.readObject(false, DecentralizedValue.class );
		hostDestination=in.readObject(false, DecentralizedValue.class );
		if (hostSource.equals(hostDestination))
			throw new MessageExternalizationException(Integrity.FAIL_AND_CANDIDATE_TO_BAN);
		encryptedAuthenticatedP2PMessages=in.readCollection(false, SIZE_IN_BYTES_AUTHENTICATED_MESSAGES_QUEUE_TO_SEND, false, byte[].class);
		authenticatedP2PMessages=null;
	}

	static List<AuthenticatedP2PMessage> getAuthenticatedP2PMessages(EncryptionProfileProvider encryptionProfileProvider, List<byte[]> encryptedAuthenticatedP2PMessages) throws IOException {
		ArrayList<AuthenticatedP2PMessage> tmp=new ArrayList<>();
		if (encryptedAuthenticatedP2PMessages==null)
			return tmp;
		for (byte[] e : encryptedAuthenticatedP2PMessages)
		{
			try(RandomByteArrayInputStream ris=new RandomByteArrayInputStream(e);RandomByteArrayOutputStream out=new RandomByteArrayOutputStream())
			{
				EncryptionSignatureHashDecoder decoder=new EncryptionSignatureHashDecoder()
						.withEncryptionProfileProvider(encryptionProfileProvider)
						.withRandomInputStream(ris);
				decoder.decodeAndCheckHashAndSignaturesIfNecessary(out);
				out.flush();
				try {
					tmp.add(out.getRandomInputStream().readObject(false, AuthenticatedP2PMessage.class));
				} catch (ClassNotFoundException classNotFoundException) {
					throw new MessageExternalizationException(Integrity.FAIL);
				}
			}
		}
		return tmp;
	}

	public List<AuthenticatedP2PMessage> getAuthenticatedP2PMessages(EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		if (authenticatedP2PMessages==null)
		{
			authenticatedP2PMessages=getAuthenticatedP2PMessages(encryptionProfileProvider, encryptedAuthenticatedP2PMessages);
		}
		return authenticatedP2PMessages;
	}

	@Override
	public String toString() {
		return "IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup{" +
				"hostSource=" + DatabaseWrapper.toString(hostSource) +
				", hostDestination=" + DatabaseWrapper.toString(hostDestination) +
				", authenticatedP2PMessages=" + authenticatedP2PMessages +
				'}';
	}
}
