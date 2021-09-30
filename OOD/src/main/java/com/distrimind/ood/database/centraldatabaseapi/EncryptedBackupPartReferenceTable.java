package com.distrimind.ood.database.centraldatabaseapi;
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

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.EncryptedDatabaseBackupMetaDataPerFile;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.filemanager.FileRecord;
import com.distrimind.ood.database.filemanager.FileReference;
import com.distrimind.ood.database.messages.*;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.RandomInputStream;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public final class EncryptedBackupPartReferenceTable extends Table<EncryptedBackupPartReferenceTable.Record> {
	@SuppressWarnings("ProtectedMemberInFinalClass")
	protected EncryptedBackupPartReferenceTable() throws DatabaseException {
	}

	public static class Record extends DatabaseRecord
	{
		@PrimaryKey
		@ForeignKey
		@NotNull
		private DatabaseBackupPerClientTable.Record database;

		@PrimaryKey
		private long fileTimeUTC;

		@Field
		private boolean isReferenceFile;

		@Field
		private long fileId;

		@Field(limit = EncryptedDatabaseBackupMetaDataPerFile.MAX_ENCRYPTED_DATABASE_BACKUP_META_DATA_SIZE_IN_BYTES)
		@NotNull
		private EncryptedDatabaseBackupMetaDataPerFile metaData;

		@SuppressWarnings("unused")
		private Record()
		{

		}

		Record(DatabaseBackupPerClientTable.Record database, FileRecord fileRecord, EncryptedBackupPartDestinedToCentralDatabaseBackup message) throws IOException {
			if (database==null)
				throw new NullPointerException();
			if (fileRecord==null)
				throw new NullPointerException();
			if (message==null)
				throw new NullPointerException();

			this.database=database;
			this.fileId=fileRecord.getFileId();
			this.fileTimeUTC=message.getMetaData().getFileTimestampUTC();
			this.isReferenceFile=message.getMetaData().isReferenceFile();
			this.metaData=message.getMetaData();
			try (RandomInputStream ris = message.getPartInputStream()) {
				fileRecord.getFileReference().save(ris);
			}
		}

		public Record(DatabaseBackupPerClientTable.Record database, long fileTimeUTC, boolean isReferenceFile, long fileId, EncryptedDatabaseBackupMetaDataPerFile metaData) {
			this.database = database;
			this.fileTimeUTC = fileTimeUTC;
			this.isReferenceFile = isReferenceFile;
			this.fileId = fileId;
			this.metaData = metaData;
		}

		public AbstractEncryptedBackupPartComingFromCentralDatabaseBackup readEncryptedBackupPartForInitialSynchronization(DatabaseWrapper wrapper, DecentralizedValue hostDestination, DecentralizedValue channelHost) throws DatabaseException, IOException {
			return new EncryptedBackupPartForInitialSynchronizationComingFromCentralDatabaseBackup(database.getClient().getClientID(),hostDestination, metaData, getFileReference(wrapper).getRandomInputStream(), channelHost);
		}
		public AbstractEncryptedBackupPartComingFromCentralDatabaseBackup readEncryptedBackupPartForRestoration(DatabaseWrapper wrapper, DecentralizedValue hostDestination, DecentralizedValue channelHost) throws IOException, DatabaseException {
			return new EncryptedBackupPartForRestorationComingFromCentralDatabaseBackup(database.getClient().getClientID(),hostDestination, metaData, getFileReference(wrapper).getRandomInputStream(), channelHost);
		}

		public EncryptedBackupPartComingFromCentralDatabaseBackup readEncryptedBackupPart(DatabaseWrapper wrapper, DecentralizedValue hostDestination) throws IOException, DatabaseException {
			return new EncryptedBackupPartComingFromCentralDatabaseBackup(database.getClient().getClientID(),hostDestination, metaData, getFileReference(wrapper).getRandomInputStream());
		}

		public DatabaseBackupPerClientTable.Record getDatabase() {
			return database;
		}

		public long getFileTimeUTC() {
			return fileTimeUTC;
		}

		public boolean isReferenceFile() {
			return isReferenceFile;
		}

		public FileReference getFileReference(DatabaseWrapper wrapper) throws DatabaseException {
			return wrapper.getFileReferenceManager().getFileRecord(fileId).getFileReference();
		}
		public void delete(DatabaseWrapper wrapper) throws DatabaseException {
			wrapper.getFileReferenceManager().decrementReferenceFile(fileId);
		}
		public EncryptedDatabaseBackupMetaDataPerFile getMetaData() {
			return metaData;
		}

		@Override
		public String toString() {
			return "Record{" +
					"database=" + database +
					", fileTimeUTC=" + fileTimeUTC +
					", isReferenceFile=" + isReferenceFile +
					", fileReferenceId=" + fileId +
					", metaData=" + metaData +
					'}';
		}

		public long getFileId() {
			return fileId;
		}
	}
	/*Integrity addEncryptedBackupPartReference(DatabaseBackupPerClientTable databaseBackupPerClientTable, ClientTable.Record clientRecord, FileReference fileReference, EncryptedBackupPartDestinedToCentralDatabaseBackup message) throws DatabaseException {
		return getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Integrity>() {
			@Override
			public Integrity run() throws Exception {
				DatabaseBackupPerClientTable.Record database=databaseBackupPerClientTable.getRecord("client", clientRecord, "packageString", message.getMetaData().getPackageString());
				boolean update=true;
				if (database==null)
				{
					database=new DatabaseBackupPerClientTable.Record(clientRecord, message.getMetaData().getPackageString(), message.getMetaData().getFileTimestampUTC(), message.getLastValidatedAndEncryptedID());
					update=false;
				}
				long l=database.getLastFileBackupPartUTC();
				if (l>=message.getMetaData().getFileTimestampUTC())
					return Integrity.FAIL;
				Record r=new Record(database, fileReference, message);
				try {
					addRecord(r);
					if (update)
						databaseBackupPerClientTable.updateRecord(database, "lastFileBackupPartUTC", message.getMetaData().getFileTimestampUTC());
					else
						databaseBackupPerClientTable.addRecord(database);
					return Integrity.OK;
				}
				catch (DatabaseException e)
				{
					fileReference.delete();
					throw e;
				}
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_REPEATABLE_READ;
			}

			@Override
			public boolean doesWriteData() {
				return true;
			}

			@Override
			public void initOrReset()  {

			}
		});

	}*/
}
