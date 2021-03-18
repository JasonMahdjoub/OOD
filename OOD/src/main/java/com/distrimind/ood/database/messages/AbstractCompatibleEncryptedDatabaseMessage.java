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
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.*;
import com.distrimind.util.io.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 3.0.0
 */
public class AbstractCompatibleEncryptedDatabaseMessage extends DatabaseEvent implements SecureExternalizable {
	public static final int MAX_SIZE_OF_ENCRYPTED_PACKAGES_NAMES_IN_BYTES=AbstractCompatibleDatabasesMessage.MAX_SIZE_OF_PACKAGES_NAMES_IN_BYTES
			+32+ SymmetricAuthenticatedSignatureType.MAX_SYMMETRIC_SIGNATURE_SIZE
			+ ASymmetricAuthenticatedSignatureType.MAX_ASYMMETRIC_SIGNATURE_SIZE
			+MessageDigestType.MAX_HASH_LENGTH+4;
	private byte[] encryptedCompatibleDatabases;
	private DecentralizedValue hostSource;

	protected AbstractCompatibleEncryptedDatabaseMessage(Set<String> compatibleDatabases, DecentralizedValue hostSource, AbstractSecureRandom random, EncryptionProfileProvider encryptionProfileProvider) throws DatabaseException {
		if (hostSource==null)
			throw new NullPointerException();
		if (compatibleDatabases ==null)
			compatibleDatabases =new HashSet<>();
		try(RandomByteArrayOutputStream out=new RandomByteArrayOutputStream()) {
			try(RandomOutputStream cipherOut=new EncryptionSignatureHashEncoder()
					.withEncryptionProfileProvider(random, encryptionProfileProvider)
					.withRandomInputStream(out.getRandomInputStream())
					.getRandomOutputStream(out))
			{
				cipherOut.writeCollection(compatibleDatabases, false, AbstractCompatibleDatabasesMessage.MAX_SIZE_OF_PACKAGES_NAMES_IN_BYTES, false);
			}
			out.flush();
			encryptedCompatibleDatabases=out.getBytes();
		}
		catch (IOException e)
		{
			throw DatabaseException.getDatabaseException(e);
		}

		this.hostSource=hostSource;
	}

	public AbstractCompatibleEncryptedDatabaseMessage(byte[] encryptedCompatibleDatabases, DecentralizedValue hostSource) {
		if (hostSource==null)
			throw new NullPointerException();
		if (encryptedCompatibleDatabases ==null)
			throw new NullPointerException();
		this.encryptedCompatibleDatabases = encryptedCompatibleDatabases;
		this.hostSource = hostSource;
	}

	public byte[] getEncryptedCompatibleDatabases() {
		return encryptedCompatibleDatabases;
	}

	static Set<String> getDecryptedCompatibleDatabases(byte[] encryptedCompatibleDatabases, EncryptionProfileProvider encryptionProfileProvider) throws DatabaseException {
		try(RandomByteArrayInputStream in=new RandomByteArrayInputStream(encryptedCompatibleDatabases);
			RandomInputStream cipherIn=new EncryptionSignatureHashDecoder()
					.withEncryptionProfileProvider(encryptionProfileProvider)
					.withRandomInputStream(in)
					.decodeAndCheckHashAndSignaturesIfNecessary()) {
			return cipherIn.readCollection(false, AbstractCompatibleDatabasesMessage.MAX_SIZE_OF_PACKAGES_NAMES_IN_BYTES, false, String.class);
		}
		catch (IOException | ClassNotFoundException e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
	}
	public Set<String> getDecryptedCompatibleDatabases(EncryptionProfileProvider encryptionProfileProvider) throws DatabaseException {
		return getDecryptedCompatibleDatabases(encryptedCompatibleDatabases, encryptionProfileProvider);
	}


	protected AbstractCompatibleEncryptedDatabaseMessage() {
	}

	@Override
	public int getInternalSerializedSize() {
		return SerializationTools.getInternalSize(encryptedCompatibleDatabases, MAX_SIZE_OF_ENCRYPTED_PACKAGES_NAMES_IN_BYTES)
				+ SerializationTools.getInternalSize(hostSource);
	}

	public DecentralizedValue getHostSource() {
		return hostSource;
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeBytesArray(encryptedCompatibleDatabases, false, MAX_SIZE_OF_ENCRYPTED_PACKAGES_NAMES_IN_BYTES);
		out.writeObject(hostSource, false);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		encryptedCompatibleDatabases =in.readBytesArray(false, MAX_SIZE_OF_ENCRYPTED_PACKAGES_NAMES_IN_BYTES);
		hostSource=in.readObject(false);
	}


}
