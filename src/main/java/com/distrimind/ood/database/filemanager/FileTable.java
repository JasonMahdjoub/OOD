package com.distrimind.ood.database.filemanager;

import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.1.0
 */
public final class FileTable extends Table<FileTable.Record> {
	@SuppressWarnings("ProtectedMemberInFinalClass")
	protected FileTable() throws DatabaseException {
	}
	public static class Record extends FileRecord {
		Record() {
		}
	}




}
