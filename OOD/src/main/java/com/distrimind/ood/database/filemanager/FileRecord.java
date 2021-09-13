package com.distrimind.ood.database.filemanager;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.annotations.AutoPrimaryKey;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.annotations.Unique;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.1.0
 */
public class FileRecord extends DatabaseRecord {
	@SuppressWarnings("unused")
	@AutoPrimaryKey
	private long fileId;

	@SuppressWarnings("unused")
	@Field
	private long creationDateUTC;

	@Field
	long lastModificationDateUTC;

	@SuppressWarnings("unused")
	@Unique
	@Field(limit= FileReference.MAX_FILE_REFERENCE_SIZE_IN_BYTES)
	@NotNull
	private FileReference fileReference;

	@Field
	int referenceNumber;

	FileRecord() {
	}

	public long getFileId() {
		return fileId;
	}


	public FileReference getFileReference() {
		return fileReference;
	}

	public int getReferenceNumber() {
		return referenceNumber;
	}

	public long getCreationDateUTC() {
		return creationDateUTC;
	}

	public long getLastModificationDateUTC() {
		return lastModificationDateUTC;
	}
}
