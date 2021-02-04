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


import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.crypto.*;
import com.distrimind.util.io.*;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public interface AuthenticatedMessage extends SecureExternalizable, DatabaseEventToSend {
	int MAX_SIGNATURES_SIZE= SymmetricAuthenticatedSignatureType.MAX_SYMMETRIC_SIGNATURE_SIZE+MessageDigestType.MAX_HASH_LENGTH+ASymmetricAuthenticatedSignatureType.MAX_ASYMMETRIC_SIGNATURE_SIZE+EncryptionSignatureHashEncoder.headSize+10;
	default void writeExternal(SecuredObjectOutputStream out) throws IOException
	{
		writeExternalWithoutSignatures(out);
		out.writeBytesArray(getSignatures(), false, MAX_SIGNATURES_SIZE);
	}
	default void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException
	{
		readExternalWithoutSignatures(in);
		setSignatures(in.readBytesArray(false, MAX_SIGNATURES_SIZE));
	}
	default int getInternalSerializedSize()
	{
		return getInternalSerializedSizeWithoutSignatures()+ SerializationTools.getInternalSize(getSignatures(), MAX_SIGNATURES_SIZE);
	}
	void writeExternalWithoutSignatures(SecuredObjectOutputStream out) throws IOException;
	void readExternalWithoutSignatures(SecuredObjectInputStream in) throws IOException, ClassNotFoundException;
	int getInternalSerializedSizeWithoutSignatures();

	default void generateAndSetSignature(AbstractSecureRandom random, EncryptionProfileProvider encryptionProfileProvider) throws DatabaseException {
		if (encryptionProfileProvider==null)
			throw new NullPointerException();
		if (random==null)
			throw new NullPointerException();
		try(RandomByteArrayOutputStream out=new RandomByteArrayOutputStream(); RandomByteArrayOutputStream outSig=new RandomByteArrayOutputStream())
		{
			writeExternalWithoutSignatures(out);
			out.flush();
			new EncryptionSignatureHashEncoder()
					.withEncryptionProfileProvider(random, encryptionProfileProvider)
					.withRandomInputStream(out.getRandomInputStream())
					.generatesOnlyHashAndSignatures(outSig);
			outSig.flush();
			setSignatures(outSig.getBytes());
		} catch (IOException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}
	default Integrity checkHashAndSignatures(EncryptionProfileProvider encryptionProfileProvider) {
		if (encryptionProfileProvider==null)
			throw new NullPointerException();
		try(RandomByteArrayOutputStream out=new RandomByteArrayOutputStream())
		{
			writeExternalWithoutSignatures(out);
			out.flush();
			return new EncryptionSignatureHashDecoder()
					.withEncryptionProfileProvider(encryptionProfileProvider)
					.checkHashAndSignatures(new RandomByteArrayInputStream(getSignatures()), out.getRandomInputStream());
		} catch (IOException e) {
			return Integrity.FAIL;
		}
	}
	default Integrity checkHashAndPublicSignature(EncryptionProfileProvider encryptionProfileProvider) {
		if (encryptionProfileProvider==null)
			throw new NullPointerException();
		try(RandomByteArrayOutputStream out=new RandomByteArrayOutputStream())
		{
			writeExternalWithoutSignatures(out);
			out.flush();
			return new EncryptionSignatureHashDecoder()
					.withEncryptionProfileProvider(encryptionProfileProvider)
					.checkHashAndPublicSignature(new RandomByteArrayInputStream(getSignatures()), out.getRandomInputStream());
		} catch (IOException e) {
			return Integrity.FAIL;
		}
	}
	void setSignatures(byte[] signatures);
	byte[] getSignatures();
}
