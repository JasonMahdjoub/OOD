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

import com.distrimind.util.io.*;

import java.io.IOException;
import java.util.*;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.5.0
 */
public class DatabaseBackupMetaData implements Iterable<DatabaseBackupMetaData.DatabaseBackupMetaPerFile>, SecureExternalizable {
	public static final int MAX_DATA_LENGTH_IN_BYTES=10484736;
	public static final int MAX_BLOCK_CHAIN_LENGTH_IN_BYTES=80484736;
	public static int MAX_INCREMENTAL_FILES=MAX_BLOCK_CHAIN_LENGTH_IN_BYTES/(44+64);
	public static int MAX_TRANSACTIONS_NUMBER_PER_FILE=MAX_DATA_LENGTH_IN_BYTES/BackupRestoreManager.MIN_TRANSACTION_SIZE_IN_BYTES;
	final List<DatabaseBackupMetaPerFile> metaDataPerFiles;
	DatabaseBackupMetaData()
	{
		this.metaDataPerFiles = new ArrayList<>();
	}



	public DatabaseBackupMetaData(List<DatabaseBackupMetaPerFile> metaDataPerFiles) {
		this.metaDataPerFiles = new ArrayList<>(metaDataPerFiles);
		checkMetaData();
	}

	private void checkMetaData()
	{
		Collections.sort(metaDataPerFiles);
		int m=metaDataPerFiles.size()-1;
		for (int i=0;i<m;i++)
		{
			DatabaseBackupMetaPerFile d1=this.metaDataPerFiles.get(i);
			DatabaseBackupMetaPerFile d2=this.metaDataPerFiles.get(i+1);
			if (d1.timeStampUTC==d2.timeStampUTC)
				throw new IllegalArgumentException();
			if (d1.transactionsMetaData.get(d1.transactionsMetaData.size()-1).transactionUTC>d2.timeStampUTC)
				throw new IllegalArgumentException();
		}
	}

	@Override
	public Iterator<DatabaseBackupMetaPerFile> iterator() {
		return metaDataPerFiles.iterator();
	}

	@Override
	public int getInternalSerializedSize() {
		int s=4;
		for (DatabaseBackupMetaPerFile m : metaDataPerFiles)
			s+=m.getInternalSerializedSize();
		return s;
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		if (metaDataPerFiles.size()>MAX_INCREMENTAL_FILES)
			throw new IOException();
		out.writeInt(metaDataPerFiles.size());
		for (DatabaseBackupMetaPerFile m : metaDataPerFiles)
			m.writeExternal(out);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException {
		int s=in.readInt();
		if (s<0 || s>MAX_INCREMENTAL_FILES)
			throw new MessageExternalizationException(Integrity.FAIL);
		for (int i=0;i<s;i++)
		{
			DatabaseBackupMetaPerFile m=new DatabaseBackupMetaPerFile();
			m.readExternal(in);
			metaDataPerFiles.add(m);
		}
		try
		{
			checkMetaData();
		}
		catch (IllegalArgumentException e)
		{
			throw new MessageExternalizationException(Integrity.FAIL, e);
		}
	}

	Long getLastUpdatedTransactionID()
	{
		if (metaDataPerFiles.size()==0)
			return null;
		List<TransactionMetaData> tmd=metaDataPerFiles.get(metaDataPerFiles.size()-1).transactionsMetaData;
		return tmd.get(tmd.size()-1).transactionID;
	}

	Long getLastUpdatedTransactionUTC()
	{
		if (metaDataPerFiles.size()==0)
			return null;
		List<TransactionMetaData> tmd=metaDataPerFiles.get(metaDataPerFiles.size()-1).transactionsMetaData;
		return tmd.get(tmd.size()-1).transactionUTC;
	}


	public static class DatabaseBackupMetaPerFile implements Comparable<DatabaseBackupMetaPerFile>, Iterable<TransactionMetaData>, SecureExternalizable
	{
		private long timeStampUTC;
		private boolean referenceFile;
		private List<TransactionMetaData> transactionsMetaData;

		private DatabaseBackupMetaPerFile()
		{

		}

		public DatabaseBackupMetaPerFile(long timeStampUTC, boolean referenceFile, List<TransactionMetaData> transactionsMetaData) {
			if (transactionsMetaData.size()==0)
				throw new IllegalArgumentException();
			this.timeStampUTC = timeStampUTC;
			this.referenceFile = referenceFile;
			this.transactionsMetaData = new ArrayList<>(transactionsMetaData);
			checkMetaData();
		}

		private void checkMetaData()
		{
			Collections.sort(this.transactionsMetaData);
			if (this.transactionsMetaData.get(0).transactionUTC<timeStampUTC)
				throw new IllegalArgumentException();
		}

		@Override
		public int compareTo(DatabaseBackupMetaPerFile o) {
			return Long.compare(timeStampUTC, o.timeStampUTC);
		}
		@Override
		public Iterator<TransactionMetaData> iterator() {
			return transactionsMetaData.iterator();
		}

		public long getTimeStampUTC() {
			return timeStampUTC;
		}

		public boolean isReferenceFile() {
			return referenceFile;
		}


		@Override
		public int getInternalSerializedSize() {
			int s=9;
			for (TransactionMetaData t : transactionsMetaData)
				s+=t.getInternalSerializedSize();
			return s;
		}

		@Override
		public void writeExternal(SecuredObjectOutputStream out) throws IOException {
			out.writeLong(timeStampUTC);
			out.writeBoolean(referenceFile);
			if (this.transactionsMetaData.size()>MAX_TRANSACTIONS_NUMBER_PER_FILE)
				throw new IOException();
			out.writeInt(this.transactionsMetaData.size());
			for (TransactionMetaData t : transactionsMetaData)
			{
				t.writeExternal(out);
			}
		}

		@Override
		public void readExternal(SecuredObjectInputStream in) throws IOException {
			timeStampUTC=in.readInt();
			referenceFile=in.readBoolean();
			int s=in.readInt();
			if (s<=0 || s>MAX_TRANSACTIONS_NUMBER_PER_FILE)
				throw new MessageExternalizationException(Integrity.FAIL);
			this.transactionsMetaData=new ArrayList<>(s);
			for (int i=0;i<s;i++)
			{
				TransactionMetaData t=new TransactionMetaData();
				t.readExternal(in);
				this.transactionsMetaData.add(t);
			}
			try
			{
				checkMetaData();
			}
			catch (IllegalArgumentException e)
			{
				throw new MessageExternalizationException(Integrity.FAIL, e);
			}
		}
	}

	public static class TransactionMetaData implements Comparable<TransactionMetaData>, SecureExternalizable
	{
		private long transactionUTC;
		private long transactionID;

		@Override
		public int compareTo(TransactionMetaData o) {
			if (transactionUTC==o.transactionUTC)
				return Long.compare(transactionID, o.transactionID);
			else
				return Long.compare(transactionUTC, o.transactionUTC);
		}

		public long getTransactionUTC() {
			return transactionUTC;
		}

		public long getTransactionID() {
			return transactionID;
		}

		@Override
		public int getInternalSerializedSize() {
			return 16;
		}

		@Override
		public void writeExternal(SecuredObjectOutputStream out) throws IOException {
			out.writeLong(transactionUTC);
			out.writeLong(transactionID);
		}

		@Override
		public void readExternal(SecuredObjectInputStream in) throws IOException {
			transactionUTC=in.readLong();
			transactionID=in.readLong();
		}
	}



}
