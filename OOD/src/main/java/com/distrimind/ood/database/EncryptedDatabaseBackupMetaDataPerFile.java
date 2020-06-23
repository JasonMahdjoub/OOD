package com.distrimind.ood.database;
/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

jason.mahdjoub@distri-mind.fr

This software (Object Oriented Database (OOD)) is a computer program 
whose purpose is to manage a local database with the object paradigm 
and the java langage 

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

import com.distrimind.util.crypto.*;
import com.distrimind.util.io.*;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 3.0.0
 */
public class EncryptedDatabaseBackupMetaDataPerFile implements SecureExternalizable {
	private long timeStampUTC;
	private boolean referenceFile;
	private byte[] encryptedMetaData;
	public static final int MAX_ENCRYPTED_DATA_LENGTH_IN_BYTES=(int)EncryptionSignatureHashEncoder.getMaximumOutputLengthWhateverParameters(DatabaseBackupMetaDataPerFile.MAXIMUM_INTERNAL_DATA_SIZE_IN_BYTES);
	@SuppressWarnings("unused")
	private EncryptedDatabaseBackupMetaDataPerFile()
	{

	}

	public EncryptedDatabaseBackupMetaDataPerFile(long timeStampUTC, boolean referenceFile, byte[] encryptedMetaData) {
		if (encryptedMetaData==null)
			throw new NullPointerException();
		if (encryptedMetaData.length==0)
			throw new IllegalArgumentException();
		this.timeStampUTC = timeStampUTC;
		this.referenceFile = referenceFile;
		this.encryptedMetaData = encryptedMetaData;
	}

	public EncryptedDatabaseBackupMetaDataPerFile(DatabaseBackupMetaDataPerFile databaseBackupMetaDataPerFile, AbstractSecureRandom random, EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		try(RandomByteArrayOutputStream out=new RandomByteArrayOutputStream(); RandomByteArrayOutputStream out2=new RandomByteArrayOutputStream()) {
			databaseBackupMetaDataPerFile.writeExternal(out);

			new EncryptionSignatureHashEncoder()
					.withEncryptionProfileProvider(random, encryptionProfileProvider)
					.withRandomInputStream(out.getRandomInputStream())
					.encode(out2);
			encryptedMetaData=out2.getBytes();
		}
	}

	public DatabaseBackupMetaDataPerFile decodeMetaData(EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		try(RandomInputStream in=new RandomByteArrayInputStream(encryptedMetaData); RandomByteArrayOutputStream out=new RandomByteArrayOutputStream()) {
			new EncryptionSignatureHashDecoder()
					.withEncryptionProfileProvider(encryptionProfileProvider)
					.withRandomInputStream(in)
					.decodeAndCheckHashAndSignaturesIfNecessary(out);
			DatabaseBackupMetaDataPerFile res=new DatabaseBackupMetaDataPerFile(timeStampUTC, referenceFile);
			res.readMetaData(out.getRandomInputStream());
			return res;
		}
	}

	public long getTimeStampUTC() {
		return timeStampUTC;
	}

	public boolean isReferenceFile() {
		return referenceFile;
	}

	public byte[] getEncryptedMetaData() {
		return encryptedMetaData;
	}

	@Override
	public int getInternalSerializedSize() {
		return 9+SerializationTools.getInternalSize(encryptedMetaData, MAX_ENCRYPTED_DATA_LENGTH_IN_BYTES);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeLong(timeStampUTC);
		out.writeBoolean(referenceFile);
		out.writeBytesArray(encryptedMetaData, false, MAX_ENCRYPTED_DATA_LENGTH_IN_BYTES);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		timeStampUTC=in.readLong();
		referenceFile=in.readBoolean();
		encryptedMetaData=in.readBytesArray(false, MAX_ENCRYPTED_DATA_LENGTH_IN_BYTES);
	}
}
