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

import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.crypto.EncryptionSignatureHashDecoder;
import com.distrimind.util.crypto.EncryptionSignatureHashEncoder;
import com.distrimind.util.io.*;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 3.0.0
 */
public class EncryptedDatabaseBackupMetaDataPerFile implements SecureExternalizable {
	private long fileTimestampUTC;
	private long lastTransactionTimestampUTC;
	private boolean referenceFile;
	private String packageString;
	private byte[] encryptedMetaData;
	private byte[] associatedData;
	public static final int MAX_ENCRYPTED_DATA_LENGTH_IN_BYTES=(int)EncryptionSignatureHashEncoder.getMaximumOutputLengthWhateverParameters(DatabaseBackupMetaDataPerFile.MAXIMUM_INTERNAL_DATA_SIZE_IN_BYTES);
	@SuppressWarnings("unused")
	private EncryptedDatabaseBackupMetaDataPerFile()
	{

	}

	public EncryptedDatabaseBackupMetaDataPerFile(long fileTimestampUTC, long lastTransactionTimestampUTC, boolean referenceFile, String packageString, byte[] encryptedMetaData) {
		if (encryptedMetaData==null)
			throw new NullPointerException();
		if (encryptedMetaData.length==0)
			throw new IllegalArgumentException();
		if (fileTimestampUTC>lastTransactionTimestampUTC)
			throw new IllegalArgumentException();
		if (packageString==null)
			throw new NullPointerException();
		if (packageString.trim().length()==0)
			throw new IllegalArgumentException();
		this.fileTimestampUTC = fileTimestampUTC;
		this.lastTransactionTimestampUTC=lastTransactionTimestampUTC;
		this.referenceFile = referenceFile;
		this.packageString=packageString;
		this.encryptedMetaData = encryptedMetaData;

	}

	public EncryptedDatabaseBackupMetaDataPerFile(String packageString, DatabaseBackupMetaDataPerFile databaseBackupMetaDataPerFile, AbstractSecureRandom random, EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		if (packageString==null)
			throw new NullPointerException();
		this.fileTimestampUTC=databaseBackupMetaDataPerFile.getFileTimestampUTC();
		this.lastTransactionTimestampUTC=databaseBackupMetaDataPerFile.getLastTransactionTimestampUTC();
		if (fileTimestampUTC>lastTransactionTimestampUTC)
			throw new IllegalArgumentException();
		this.referenceFile=databaseBackupMetaDataPerFile.isReferenceFile();
		this.packageString=packageString;

		try(RandomByteArrayOutputStream out=new RandomByteArrayOutputStream(); RandomByteArrayOutputStream out2=new RandomByteArrayOutputStream()) {
			databaseBackupMetaDataPerFile.writeExternal(out);
			out.flush();

			new EncryptionSignatureHashEncoder()
					.withEncryptionProfileProvider(random, encryptionProfileProvider)
					.withRandomInputStream(out.getRandomInputStream())
					.withAssociatedData(getAssociatedData())
					.encode(out2);
			out2.flush();
			encryptedMetaData=out2.getBytes();
		}
	}

	private byte[] getAssociatedData() throws IOException {
		if (associatedData==null){
			RandomByteArrayOutputStream out=new RandomByteArrayOutputStream();
			out.writeLong(this.fileTimestampUTC);
			out.writeLong(this.lastTransactionTimestampUTC);
			out.writeString(this.packageString, false, SerializationTools.MAX_CLASS_LENGTH);
			out.flush();
			associatedData=out.getBytes();
		}
		return associatedData;
	}

	public Integrity checkSignature(EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		try(RandomInputStream in=new RandomByteArrayInputStream(encryptedMetaData)) {
			return new EncryptionSignatureHashDecoder()
					.withEncryptionProfileProvider(encryptionProfileProvider)
					.withRandomInputStream(in)
					.withAssociatedData(getAssociatedData())
					.checkHashAndSignatures();
		}

	}

	public String getPackageString() {
		return packageString;
	}

	public DatabaseBackupMetaDataPerFile decodeMetaData(EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		try(RandomInputStream in=new RandomByteArrayInputStream(encryptedMetaData); RandomByteArrayOutputStream out=new RandomByteArrayOutputStream()) {
			new EncryptionSignatureHashDecoder()
					.withEncryptionProfileProvider(encryptionProfileProvider)
					.withRandomInputStream(in)
					.withAssociatedData(getAssociatedData())
					.decodeAndCheckHashAndSignaturesIfNecessary(out);
			DatabaseBackupMetaDataPerFile res=new DatabaseBackupMetaDataPerFile(fileTimestampUTC, referenceFile);
			res.readExternal(out.getRandomInputStream());
			return res;
		}
	}

	public long getFileTimestampUTC() {
		return fileTimestampUTC;
	}

	public boolean isReferenceFile() {
		return referenceFile;
	}

	public byte[] getEncryptedMetaData() {
		return encryptedMetaData;
	}

	public long getLastTransactionTimestampUTC() {
		return lastTransactionTimestampUTC;
	}


	@Override
	public int getInternalSerializedSize() {
		return 17+SerializationTools.getInternalSize(encryptedMetaData, MAX_ENCRYPTED_DATA_LENGTH_IN_BYTES)+SerializationTools.getInternalSize(packageString, SerializationTools.MAX_CLASS_LENGTH);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeLong(fileTimestampUTC);
		out.writeLong(lastTransactionTimestampUTC);
		out.writeBoolean(referenceFile);
		out.writeString(packageString, false, SerializationTools.MAX_CLASS_LENGTH);
		out.writeBytesArray(encryptedMetaData, false, MAX_ENCRYPTED_DATA_LENGTH_IN_BYTES);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		associatedData=null;
		fileTimestampUTC =in.readLong();
		lastTransactionTimestampUTC=in.readLong();
		if (fileTimestampUTC>lastTransactionTimestampUTC)
			throw new MessageExternalizationException(Integrity.FAIL);
		referenceFile=in.readBoolean();
		packageString=in.readString(false, SerializationTools.MAX_CLASS_LENGTH);
		if (packageString.trim().length()==0)
			throw new MessageExternalizationException(Integrity.FAIL);
		encryptedMetaData=in.readBytesArray(false, MAX_ENCRYPTED_DATA_LENGTH_IN_BYTES);
	}
}
