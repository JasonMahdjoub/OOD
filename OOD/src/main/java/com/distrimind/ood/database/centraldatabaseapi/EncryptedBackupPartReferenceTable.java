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
import com.distrimind.ood.database.EncryptedDatabaseBackupMetaDataPerFile;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.messages.EncryptedBackupPartComingFromCentralDatabaseBackup;
import com.distrimind.ood.database.messages.EncryptedBackupPartDestinedToCentralDatabaseBackup;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.RandomInputStream;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class EncryptedBackupPartReferenceTable extends Table<EncryptedBackupPartReferenceTable.Record> {
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
		@NotNull
		private FileReference fileReference=null;

		@Field
		@NotNull
		private EncryptedDatabaseBackupMetaDataPerFile metaData;

		@SuppressWarnings("unused")
		private Record()
		{

		}

		public Record(DatabaseBackupPerClientTable.Record database, FileReference fileReference, EncryptedBackupPartDestinedToCentralDatabaseBackup message) throws IOException {
			if (database==null)
				throw new NullPointerException();
			if (fileReference==null)
				throw new NullPointerException();
			if (message==null)
				throw new NullPointerException();

			this.database=database;
			this.fileReference=fileReference;
			this.fileTimeUTC=message.getMetaData().getFileTimestampUTC();
			this.isReferenceFile=message.getMetaData().isReferenceFile();
			this.metaData=message.getMetaData();
			try (RandomInputStream ris = message.getPartInputStream()) {
				fileReference.save(ris);
			}
		}

		public EncryptedBackupPartComingFromCentralDatabaseBackup readEncryptedBackupPart(DecentralizedValue hostDestination) throws IOException {

			return new EncryptedBackupPartComingFromCentralDatabaseBackup(database.getClient().getClientID(),hostDestination, metaData, fileReference.getRandomInputStream());
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

		public FileReference getFileReference() {
			return fileReference;
		}

		public EncryptedDatabaseBackupMetaDataPerFile getMetaData() {
			return metaData;
		}
	}
}
