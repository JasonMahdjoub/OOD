package com.distrimind.ood.database.filemanager;

import com.distrimind.ood.database.*;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.RecordNotFoundDatabaseException;

import java.io.IOException;
import java.util.*;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.1.0
 */
public class FileReferenceManager {
	public static final Set<Class<?>> internalFileManagerClassesList=new HashSet<>(Collections.singletonList(FileTable.class));
	private final FileTable fileTable;

	public FileReferenceManager(DatabaseWrapper wrapper) throws DatabaseException {
		fileTable=wrapper.getTableInstance(FileTable.class);
	}

	public FileRecord getFileRecord(long fileId) throws DatabaseException {
		return fileTable.getRecord("fileId", fileId);

	}
	public FileRecord getFileRecord(FileReference fileReference) throws DatabaseException {
		return fileTable.getRecords("fileReference", fileReference).stream().findAny().orElse(null);
	}
	public FileRecord incrementReferenceFile(FileReference fileReference) throws DatabaseException {
		return fileTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<FileRecord>() {
			@Override
			public FileTable.Record run() throws Exception {
				FileTable.Record r=fileTable.getRecordsWithAllFields("fileReference", fileReference).stream().findAny().orElse(null);
				if (r==null)
				{
					long t=System.currentTimeMillis();
					r=fileTable.addRecord("fileReference", fileReference,
							"creationDateUTC", t,
							"lastModificationDateUTC", t,
							"referenceNumber", 1);
					r.getFileReference().linkToDatabase(fileTable, r);
				}
				else
				{
					r.getFileReference().linkToDatabase(fileTable, r);
					fileTable.updateRecord(r, "referenceNumber", ++r.referenceNumber);
				}
				return r;
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

	}
	public FileRecord incrementReferenceFile(long fileId) throws DatabaseException {
		return fileTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<FileRecord>() {
			@Override
			public FileTable.Record run() throws Exception {
				FileTable.Record r = fileTable.getRecord("fileId", fileId);
				if (r == null) {
					throw new RecordNotFoundDatabaseException("Record " + fileId + " not found !");
				} else {
					r.getFileReference().linkToDatabase(fileTable, r);
					fileTable.updateRecord(r, "referenceNumber", ++r.referenceNumber);
					return r;
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
			public void initOrReset() {

			}
		});
	}

	public FileRecord decrementReferenceFile(FileReference fileReference) throws DatabaseException {
		return fileTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<FileRecord>() {
			@Override
			public FileRecord run() throws Exception {
				FileTable.Record r = fileTable.getRecordsWithAllFields("fileReference", fileReference).stream().findAny().orElse(null);
				if (r == null) {
					throw new RecordNotFoundDatabaseException("Record not found !");
				} else {
					return decrementReferenceFile(r);
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
			public void initOrReset() {

			}
		});
	}
	public FileRecord decrementReferenceFile(long fileId) throws DatabaseException {
		return fileTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<FileRecord>() {
			@Override
			public FileRecord run() throws Exception {
				FileTable.Record r = fileTable.getRecord("fileId", fileId);
				if (r == null) {
					throw new RecordNotFoundDatabaseException("Record not found !");
				} else {
					return decrementReferenceFile(r);
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
			public void initOrReset() {

			}
		});
	}
	public FileRecord decrementReferenceFile(FileRecord fr) throws DatabaseException {
		if (fr==null)
			throw new NullPointerException();
		if (!(fr instanceof FileTable.Record))
			throw new IllegalArgumentException();
		FileTable.Record r=(FileTable.Record) fr;

		try {
			r.getFileReference().linkToDatabase(fileTable, r);
			if (--r.referenceNumber <= 0) {
				r.getFileReference().deleteImplementation();
				fileTable.removeRecord(r);
				r.getFileReference().linkToDatabase(null, null);
			} else
				fileTable.updateRecord(r, "referenceNumber", r.referenceNumber);
			return r;
		}
		catch (IOException e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
	}

	public FileRecord removeFileAndAllItsReferences(FileReference fileReference) throws DatabaseException {
		return fileTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<FileRecord>() {
			@Override
			public FileRecord run() throws Exception {
				FileTable.Record r = fileTable.getRecordsWithAllFields("fileReference", fileReference).stream().findAny().orElse(null);
				if (r == null) {
					throw new RecordNotFoundDatabaseException("Record not found !");
				} else {
					return removeFileAndAllItsReferences(r);
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
			public void initOrReset() {

			}
		});
	}
	public FileRecord removeFileAndAllItsReferences(long fileId) throws DatabaseException {
		return fileTable.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<FileRecord>() {
			@Override
			public FileRecord run() throws Exception {
				FileTable.Record r = fileTable.getRecord("fileId", fileId);
				if (r == null) {
					throw new RecordNotFoundDatabaseException("Record not found !");
				} else {
					return removeFileAndAllItsReferences(r);
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
			public void initOrReset() {

			}
		});
	}
	public FileRecord removeFileAndAllItsReferences(FileRecord fr) throws DatabaseException {
		if (fr==null)
			throw new NullPointerException();
		if (!(fr instanceof FileTable.Record))
			throw new IllegalArgumentException();
		FileTable.Record r=(FileTable.Record) fr;
		try {
			r.getFileReference().linkToDatabase(null, null);
			if (r.referenceNumber>0) {
				r.getFileReference().deleteImplementation();
			}

			r.referenceNumber = 0;
			fileTable.removeRecord(r);
			return r;
		}
		catch (IOException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	public boolean isCached() {
		return fileTable.isCached();
	}

	public int getDatabaseVersion() {
		return fileTable.getDatabaseVersion();
	}

	public DatabaseCollisionsNotifier<FileTable.Record, Table<FileTable.Record>> getDatabaseCollisionsNotifier() {
		return fileTable.getDatabaseCollisionsNotifier();
	}

	public void setDatabaseCollisionsNotifier(DatabaseCollisionsNotifier<FileTable.Record, Table<FileTable.Record>> databaseCollisionsNotifier) {
		fileTable.setDatabaseCollisionsNotifier(databaseCollisionsNotifier);
	}

	public DatabaseAnomaliesNotifier<FileTable.Record, Table<FileTable.Record>> getDatabaseAnomaliesNotifier() {
		return fileTable.getDatabaseAnomaliesNotifier();
	}

	public void setDatabaseAnomaliesNotifier(DatabaseAnomaliesNotifier<FileTable.Record, Table<FileTable.Record>> databaseAnomaliesNotifier) {
		fileTable.setDatabaseAnomaliesNotifier(databaseAnomaliesNotifier);
	}

	public DatabaseConfiguration getDatabaseConfiguration() {
		return fileTable.getDatabaseConfiguration();
	}

	public boolean supportSynchronizationWithOtherPeers() {
		return fileTable.supportSynchronizationWithOtherPeers();
	}

	public long getRecordsNumber() throws DatabaseException {
		return fileTable.getRecordsNumber();
	}
	private static class FilterProxy extends com.distrimind.ood.database.Filter<FileTable.Record> {
		private final com.distrimind.ood.database.Filter<FileRecord> filter;
		private FilterProxy(com.distrimind.ood.database.Filter<FileRecord> filter)
		{
			this.filter=filter;
		}
		@Override
		public boolean nextRecord(FileTable.Record _record) throws DatabaseException {
			_record.getFileReference().linkToDatabase(null, _record);
			return filter.nextRecord(_record);
		}
	}
	private FileRecord getFileRecord(FileTable.Record r)
	{
		r.getFileReference().linkToDatabase(fileTable, r);
		return r;
	}
	private ArrayList<FileRecord> getRecords(ArrayList<FileTable.Record> records)
	{
		for (FileTable.Record fr : records)
			fr.getFileReference().linkToDatabase(fileTable, fr);
		return new ArrayList<>(records);
	}

	public static class CursorProxy
	{
		private final Cursor<FileTable.Record> cursor;

		private CursorProxy(Cursor<FileTable.Record> cursor) {
			this.cursor = cursor;
		}

		public long getTotalRecordsNumber() throws DatabaseException {
			return cursor.getTotalRecordsNumber();
		}

		public void refreshData() {
			cursor.refreshData();
		}

		public FileRecord getRecord(int position) throws DatabaseException {
			return cursor.getRecord(position);
		}
	}
	public long getRecordsNumber(Filter<FileRecord> _filter, String whereCondition, Map<String, Object> parameters) throws DatabaseException {
		return fileTable.getRecordsNumber(new FilterProxy(_filter), whereCondition, parameters);
	}

	public long getRecordsNumber(Filter<FileRecord> _filter, String whereCondition, Object... parameters) throws DatabaseException {
		return fileTable.getRecordsNumber(new FilterProxy(_filter), whereCondition, parameters);
	}

	public long getRecordsNumber(String whereCondition, Object... parameters) throws DatabaseException {
		return fileTable.getRecordsNumber(whereCondition, parameters);
	}

	public long getRecordsNumber(String whereCondition, Map<String, Object> parameters) throws DatabaseException {
		return fileTable.getRecordsNumber(whereCondition, parameters);
	}

	public long getRecordsNumber(Filter<FileRecord> _filter) throws DatabaseException {
		return fileTable.getRecordsNumber(new FilterProxy(_filter));
	}

	@SafeVarargs
	public final long getRecordsNumberWithAllFields(Map<String, Object>... _records) throws DatabaseException {
		return fileTable.getRecordsNumberWithAllFields(_records);
	}

	public long getRecordsNumberWithAllFields(Object[]... _records) throws DatabaseException {
		return fileTable.getRecordsNumberWithAllFields(_records);
	}

	public long getRecordsNumberWithAllFields(Map<String, Object> _records) throws DatabaseException {
		return fileTable.getRecordsNumberWithAllFields(_records);
	}

	public long getRecordsNumberWithAllFields(Object... _records) throws DatabaseException {
		return fileTable.getRecordsNumberWithAllFields(_records);
	}

	@SafeVarargs
	public final long getRecordsNumberWithOneOfFields(Map<String, Object>... _records) throws DatabaseException {
		return fileTable.getRecordsNumberWithOneOfFields(_records);
	}

	public long getRecordsNumberWithOneOfFields(Object[]... _records) throws DatabaseException {
		return fileTable.getRecordsNumberWithOneOfFields(_records);
	}

	public long getRecordsNumberWithOneOfFields(Map<String, Object> _records) throws DatabaseException {
		return fileTable.getRecordsNumberWithOneOfFields(_records);
	}

	public long getRecordsNumberWithOneOfFields(Object... _records) throws DatabaseException {
		return fileTable.getRecordsNumberWithOneOfFields(_records);
	}

	public boolean isLoadedInMemory() {
		return fileTable.isLoadedInMemory();
	}

	public ArrayList<FileRecord> getOrderedRecords(boolean _ascendant, String... _fields) throws DatabaseException {
		return getRecords(fileTable.getOrderedRecords(_ascendant, _fields));
	}

	public ArrayList<FileRecord> getOrderedRecords(Filter<FileRecord> _filter, boolean _ascendant, String... _fields) throws DatabaseException {
		return getRecords(fileTable.getOrderedRecords(new FilterProxy(_filter), _ascendant, _fields));
	}

	public ArrayList<FileRecord> getOrderedRecords(Filter<FileRecord> _filter, String whereCondition, Object[] parameters, boolean _ascendant, String... _fields) throws DatabaseException {
		return getRecords(fileTable.getOrderedRecords(new FilterProxy(_filter), whereCondition, parameters, _ascendant, _fields));
	}

	public ArrayList<FileRecord> getOrderedRecords(Filter<FileRecord> _filter, String whereCondition, Map<String, Object> parameters, boolean _ascendant, String... _fields) throws DatabaseException {
		return getRecords(fileTable.getOrderedRecords(new FilterProxy(_filter), whereCondition, parameters, _ascendant, _fields));
	}

	public ArrayList<FileRecord> getPaginatedOrderedRecords(long rowPos, long rowLength, Filter<FileRecord> _filter, String whereCondition, Map<String, Object> parameters, boolean _ascendant, String... _fields) throws DatabaseException {
		return getRecords(fileTable.getPaginatedOrderedRecords(rowPos, rowLength, new FilterProxy(_filter), whereCondition, parameters, _ascendant, _fields));
	}


	public ArrayList<FileRecord> getOrderedRecords(String whereCondition, Map<String, Object> parameters, boolean _ascendant, String... _fields) throws DatabaseException {
		return getRecords(fileTable.getOrderedRecords(whereCondition, parameters, _ascendant, _fields));
	}

	public CursorProxy getCursor() {
		return new CursorProxy(fileTable.getCursor());
	}

	public CursorProxy getCursor(int cacheSize) {
		return new CursorProxy(fileTable.getCursor(cacheSize));
	}

	public CursorProxy getCursor(String whereCondition, Map<String, Object> parameters) {
		return new CursorProxy(fileTable.getCursor(whereCondition, parameters));
	}

	public CursorProxy getCursor(String whereCondition, Map<String, Object> parameters, int cacheSize) {
		return new CursorProxy(fileTable.getCursor(whereCondition, parameters, cacheSize));
	}

	public CursorProxy getCursorWithOrderedResults(String whereCondition, Map<String, Object> parameters, boolean ascendant, String... fields) {
		return new CursorProxy(fileTable.getCursorWithOrderedResults(whereCondition, parameters, ascendant, fields));
	}

	public CursorProxy getCursorWithOrderedResults(boolean ascendant, String... fields) {
		return new CursorProxy(fileTable.getCursorWithOrderedResults(ascendant, fields));
	}

	public CursorProxy getCursorWithOrderedResults(int cacheSize, boolean ascendant, String... fields) {
		return new CursorProxy(fileTable.getCursorWithOrderedResults(cacheSize, ascendant, fields));
	}

	public CursorProxy getCursorWithOrderedResults(String whereCondition, Map<String, Object> parameters, int cacheSize, boolean ascendant, String... fields) {
		return new CursorProxy(fileTable.getCursorWithOrderedResults(whereCondition, parameters, cacheSize, ascendant, fields));
	}

	public ArrayList<FileRecord> getPaginatedOrderedRecords(long rowPos, long rowLength, String whereCondition, Map<String, Object> parameters, boolean _ascendant, String... _fields) throws DatabaseException {
		return getRecords(fileTable.getPaginatedOrderedRecords(rowPos, rowLength, whereCondition, parameters, _ascendant, _fields));
	}

	public ArrayList<FileRecord> getRecords() throws DatabaseException {
		return getRecords(fileTable.getRecords());
	}

	public ArrayList<FileRecord> getPaginatedRecords(long rowPos, long rowLength) throws DatabaseException {
		return getRecords(fileTable.getPaginatedRecords(rowPos, rowLength));
	}

	public ArrayList<FileRecord> getRecords(Filter<FileRecord> _filter) throws DatabaseException {
		return getRecords(fileTable.getRecords(new FilterProxy(_filter)));
	}

	public ArrayList<FileRecord> getPaginatedRecords(long rowPos, long rowLength, Filter<FileRecord> _filter) throws DatabaseException {
		return getRecords(fileTable.getPaginatedRecords(rowPos, rowLength, new FilterProxy(_filter)));
	}

	public ArrayList<FileRecord> getRecords(Filter<FileRecord> _filter, String whereCondition, Object... parameters) throws DatabaseException {
		return getRecords(fileTable.getRecords(new FilterProxy(_filter), whereCondition, parameters));
	}

	public ArrayList<FileRecord> getRecords(Filter<FileRecord> _filter, String whereCondition, Map<String, Object> parameters) throws DatabaseException {
		return getRecords(fileTable.getRecords(new FilterProxy(_filter), whereCondition, parameters));
	}

	public ArrayList<FileRecord> getPaginatedRecords(long rowPos, long rowLength, Filter<FileRecord> _filter, String whereCondition, Object... parameters) throws DatabaseException {
		return getRecords(fileTable.getPaginatedRecords(rowPos, rowLength, new FilterProxy(_filter), whereCondition, parameters));
	}

	public ArrayList<FileRecord> getPaginatedRecords(long rowPos, long rowLength, Filter<FileRecord> _filter, String whereCondition, Map<String, Object> parameters) throws DatabaseException {
		return getRecords(fileTable.getPaginatedRecords(rowPos, rowLength, new FilterProxy(_filter), whereCondition, parameters));
	}

	public ArrayList<FileRecord> getRecords(String whereCondition, Object... parameters) throws DatabaseException {
		return getRecords(fileTable.getRecords(whereCondition, parameters));
	}

	public ArrayList<FileRecord> getRecords(String whereCondition, Map<String, Object> parameters) throws DatabaseException {
		return getRecords(fileTable.getRecords(whereCondition, parameters));
	}

	public ArrayList<FileRecord> getPaginatedRecords(long rowPos, long rowLength, String whereCondition, Object... parameters) throws DatabaseException {
		return getRecords(fileTable.getPaginatedRecords(rowPos, rowLength, whereCondition, parameters));
	}

	public ArrayList<FileRecord> getPaginatedRecords(long rowPos, long rowLength, String whereCondition, Map<String, Object> parameters) throws DatabaseException {
		return getRecords(fileTable.getPaginatedRecords(rowPos, rowLength, whereCondition, parameters));
	}

	public boolean hasRecords(Filter<FileRecord> _filter) throws DatabaseException {
		return fileTable.hasRecords(new FilterProxy(_filter));
	}

	public boolean hasRecords(Filter<FileRecord> _filter, String whereCondition, Map<String, Object> parameters) throws DatabaseException {
		return fileTable.hasRecords(new FilterProxy(_filter), whereCondition, parameters);
	}

	public boolean hasRecords(Filter<FileRecord> _filter, String whereCondition, Object... parameters) throws DatabaseException {
		return fileTable.hasRecords(new FilterProxy(_filter), whereCondition, parameters);
	}

	public boolean hasRecords(String whereCondition, Object... parameters) throws DatabaseException {
		return fileTable.hasRecords(whereCondition, parameters);
	}

	public boolean hasRecords(String whereCondition, Map<String, Object> parameters) throws DatabaseException {
		return fileTable.hasRecords(whereCondition, parameters);
	}


	public ArrayList<FileRecord> getRecordsWithAllFields(Object... _fields) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithAllFields(_fields));
	}

	public ArrayList<FileRecord> getRecordsWithAllFieldsOrdered(boolean ascendant, String[] orderByFields, Object... _fields) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithAllFieldsOrdered(ascendant, orderByFields, _fields));
	}

	public ArrayList<FileRecord> getRecordsWithAllFields(Map<String, Object> _fields) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithAllFields(_fields));
	}

	public ArrayList<FileRecord> getRecordsWithAllFieldsOrdered(boolean ascendant, String[] orderByFields, Map<String, Object> _fields) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithAllFieldsOrdered(ascendant, orderByFields, _fields));
	}

	public ArrayList<FileRecord> getPaginatedRecordsWithAllFieldsOrdered(long rowPos, long rowLength, boolean ascendant, String[] orderByFields, Object... _fields) throws DatabaseException {
		return getRecords(fileTable.getPaginatedRecordsWithAllFieldsOrdered(rowPos, rowLength, ascendant, orderByFields, _fields));
	}

	public ArrayList<FileRecord> getPaginatedRecordsWithAllFieldsOrdered(long rowPos, long rowLength, boolean ascendant, String[] orderByFields, Map<String, Object> _fields) throws DatabaseException {
		return getRecords(fileTable.getPaginatedRecordsWithAllFieldsOrdered(rowPos, rowLength, ascendant, orderByFields, _fields));
	}

	public boolean hasRecordsWithAllFields(Object... _fields) throws DatabaseException {
		return fileTable.hasRecordsWithAllFields(_fields);
	}

	public boolean hasRecordsWithAllFields(Map<String, Object> _fields) throws DatabaseException {
		return fileTable.hasRecordsWithAllFields(_fields);
	}

	public ArrayList<FileRecord> getRecordsWithAllFields(Object[]... _records) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithAllFields(_records));
	}

	public ArrayList<FileRecord> getRecordsWithAllFieldsOrdered(boolean ascendant, String[] orderByFields, Object[]... _records) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithAllFieldsOrdered(ascendant, orderByFields, _records));
	}

	@SafeVarargs
	public final ArrayList<FileRecord> getRecordsWithAllFields(Map<String, Object>... _records) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithAllFields(_records));
	}

	@SafeVarargs
	public final ArrayList<FileRecord> getRecordsWithAllFieldsOrdered(boolean ascendant, String[] orderByFields, Map<String, Object>... _records) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithAllFieldsOrdered(ascendant, orderByFields, _records));
	}

	public ArrayList<FileRecord> getPaginatedRecordsWithAllFieldsOrdered(long rowPos, long rowLength, boolean ascendant, String[] orderByFields, Object[]... _records) throws DatabaseException {
		return getRecords(fileTable.getPaginatedRecordsWithAllFieldsOrdered(rowPos, rowLength, ascendant, orderByFields, _records));
	}

	@SafeVarargs
	public final ArrayList<FileRecord> getPaginatedRecordsWithAllFieldsOrdered(long rowPos, long rowLength, boolean ascendant, String[] orderByFields, Map<String, Object>... _records) throws DatabaseException {
		return getRecords(fileTable.getPaginatedRecordsWithAllFieldsOrdered(rowPos, rowLength, ascendant, orderByFields, _records));
	}

	public boolean hasRecordsWithAllFields(Object[]... _records) throws DatabaseException {
		return fileTable.hasRecordsWithAllFields(_records);
	}

	@SafeVarargs
	public final boolean hasRecordsWithAllFields(Map<String, Object>... _records) throws DatabaseException {
		return fileTable.hasRecordsWithAllFields(_records);
	}

	public ArrayList<FileRecord> getRecordsWithOneOfFields(Object... _fields) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithOneOfFields(_fields));
	}

	public ArrayList<FileRecord> getRecordsWithOneOfFieldsOrdered(boolean ascendant, String[] orderByFields, Object... _fields) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithOneOfFieldsOrdered(ascendant, orderByFields, _fields));
	}

	public ArrayList<FileRecord> getRecordsWithOneOfFields(Map<String, Object> _fields) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithOneOfFields(_fields));
	}

	public ArrayList<FileRecord> getRecordsWithOneOfFieldsOrdered(boolean ascendant, String[] orderByFields, Map<String, Object> _fields) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithOneOfFieldsOrdered(ascendant, orderByFields, _fields));
	}

	public ArrayList<FileRecord> getRecordsWithOneOfFieldsOrdered(long rowPos, long rowLength, boolean ascendant, String[] orderByFields, Object... _fields) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithOneOfFieldsOrdered(rowPos, rowLength, ascendant, orderByFields, _fields));
	}

	public ArrayList<FileRecord> getPaginatedRecordsWithOneOfFieldsOrdered(long rowPos, long rowLength, boolean ascendant, String[] orderByFields, Map<String, Object> _fields) throws DatabaseException {
		return getRecords(fileTable.getPaginatedRecordsWithOneOfFieldsOrdered(rowPos, rowLength, ascendant, orderByFields, _fields));
	}

	public boolean hasRecordsWithOneOfFields(Object... _fields) throws DatabaseException {
		return fileTable.hasRecordsWithOneOfFields(_fields);
	}

	public boolean hasRecordsWithOneOfFields(Map<String, Object> _fields) throws DatabaseException {
		return fileTable.hasRecordsWithOneOfFields(_fields);
	}

	public ArrayList<FileRecord> getRecordsWithOneOfFields(Object[]... _records) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithOneOfFields(_records));
	}

	public ArrayList<FileRecord> getRecordsWithOneOfFieldsOrdered(Object[]... _records) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithOneOfFieldsOrdered(_records));
	}

	@SafeVarargs
	public final ArrayList<FileRecord> getRecordsWithOneOfFields(Map<String, Object>... _records) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithOneOfFields(_records));
	}

	@SafeVarargs
	public final ArrayList<FileRecord> getRecordsWithOneOfFieldsOrdered(boolean ascendant, String[] orderByFields, Map<String, Object>... _records) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithOneOfFieldsOrdered(ascendant, orderByFields, _records));
	}

	public ArrayList<FileRecord> getRecordsWithOneOfFieldsOrdered(long rowPos, long rowLength, Object[]... _records) throws DatabaseException {
		return getRecords(fileTable.getRecordsWithOneOfFieldsOrdered(rowPos, rowLength, _records));
	}

	@SafeVarargs
	public final ArrayList<FileRecord> getPaginatedRecordsWithOneOfFieldsOrdered(long rowPos, long rowLength, boolean ascendant, String[] orderByFields, Map<String, Object>... _records) throws DatabaseException {
		return getRecords(fileTable.getPaginatedRecordsWithOneOfFieldsOrdered(rowPos, rowLength, ascendant, orderByFields, _records));
	}

	public boolean hasRecordsWithOneOfFields(Object[]... _records) throws DatabaseException {
		return fileTable.hasRecordsWithOneOfFields(_records);
	}

	@SafeVarargs
	public final boolean hasRecordsWithOneOfFields(Map<String, Object>... _records) throws DatabaseException {
		return fileTable.hasRecordsWithOneOfFields(_records);
	}

	public long removeRecordsWithAllFields(Object... _fields) throws DatabaseException {
		return fileTable.removeRecordsWithAllFields(_fields);
	}

	public long removeRecordsWithAllFields(Map<String, Object> _fields) throws DatabaseException {
		return fileTable.removeRecordsWithAllFields(_fields);
	}

	public long removeAllRecordsWithCascade() throws DatabaseException {
		return fileTable.removeAllRecordsWithCascade();
	}

	public long removeRecordsWithAllFields(Object[]... _records) throws DatabaseException {
		return fileTable.removeRecordsWithAllFields(_records);
	}

	@SafeVarargs
	public final long removeRecordsWithAllFields(Map<String, Object>... _records) throws DatabaseException {
		return fileTable.removeRecordsWithAllFields(_records);
	}

	public long removeRecordsWithOneOfFields(Object... _fields) throws DatabaseException {
		return fileTable.removeRecordsWithOneOfFields(_fields);
	}

	public long removeRecordsWithOneOfFields(Map<String, Object> _fields) throws DatabaseException {
		return fileTable.removeRecordsWithOneOfFields(_fields);
	}

	public long removeRecordsWithOneOfFields(Object[]... _records) throws DatabaseException {
		return fileTable.removeRecordsWithOneOfFields(_records);
	}

	@SafeVarargs
	public final long removeRecordsWithOneOfFields(Map<String, Object>... _records) throws DatabaseException {
		return fileTable.removeRecordsWithOneOfFields(_records);
	}

	public long removeRecordsWithAllFieldsWithCascade(Object... _fields) throws DatabaseException {
		return fileTable.removeRecordsWithAllFieldsWithCascade(_fields);
	}

	public long removeRecordsWithAllFieldsWithCascade(Map<String, Object> _fields) throws DatabaseException {
		return fileTable.removeRecordsWithAllFieldsWithCascade(_fields);
	}

	public long removeRecordsWithAllFieldsWithCascade(Object[]... _records) throws DatabaseException {
		return fileTable.removeRecordsWithAllFieldsWithCascade(_records);
	}

	@SafeVarargs
	public final long removeRecordsWithAllFieldsWithCascade(Map<String, Object>... _records) throws DatabaseException {
		return fileTable.removeRecordsWithAllFieldsWithCascade(_records);
	}

	public long removeRecordsWithOneOfFieldsWithCascade(Object... _fields) throws DatabaseException {
		return fileTable.removeRecordsWithOneOfFieldsWithCascade(_fields);
	}

	public long removeRecordsWithOneOfFieldsWithCascade(Map<String, Object> _fields) throws DatabaseException {
		return fileTable.removeRecordsWithOneOfFieldsWithCascade(_fields);
	}

	public long removeRecordsWithOneOfFieldsWithCascade(Object[]... _records) throws DatabaseException {
		return fileTable.removeRecordsWithOneOfFieldsWithCascade(_records);
	}

	@SafeVarargs
	public final long removeRecordsWithOneOfFieldsWithCascade(Map<String, Object>... _records) throws DatabaseException {
		return fileTable.removeRecordsWithOneOfFieldsWithCascade(_records);
	}

	public long removeRecords(Filter<FileRecord> _filter) throws DatabaseException {
		return fileTable.removeRecords(new FilterProxy(_filter));
	}

	public long removeRecords(String whereCommand, Object... parameters) throws DatabaseException {
		return fileTable.removeRecords(whereCommand, parameters);
	}

	public long removeRecords(Filter<FileRecord> _filter, String whereCommand, Object... parameters) throws DatabaseException {
		return fileTable.removeRecords(new FilterProxy(_filter), whereCommand, parameters);
	}

	public long removeRecords(Filter<FileRecord> _filter, String whereCommand, Map<String, Object> parameters) throws DatabaseException {
		return fileTable.removeRecords(new FilterProxy(_filter), whereCommand, parameters);
	}

	public long removeRecords(String whereCommand, Map<String, Object> parameters) throws DatabaseException {
		return fileTable.removeRecords(whereCommand, parameters);
	}

	public void checkDataIntegrity() throws DatabaseException {
		fileTable.checkDataIntegrity();
	}

	public long removeRecordsWithCascade(Filter<FileRecord> _filter) throws DatabaseException {
		return fileTable.removeRecordsWithCascade(new FilterProxy(_filter));
	}

	public long removeRecordsWithCascade(Filter<FileRecord> _filter, String whereCommand, Object... parameters) throws DatabaseException {
		return fileTable.removeRecordsWithCascade(new FilterProxy(_filter), whereCommand, parameters);
	}

	public long removeRecordsWithCascade(Filter<FileRecord> _filter, String whereCommand, Map<String, Object> parameters) throws DatabaseException {
		return fileTable.removeRecordsWithCascade(new FilterProxy(_filter), whereCommand, parameters);
	}

	public long removeRecordsWithCascade(String whereCommand, Object... parameters) throws DatabaseException {
		return fileTable.removeRecordsWithCascade(whereCommand, parameters);
	}

	public long removeWithCascadeRecordsPointingToThisRecord(FileRecord record) throws DatabaseException {
		return fileTable.removeWithCascadeRecordsPointingToThisRecord((FileTable.Record) record);
	}

	public void removeRecord(FileRecord _record) throws DatabaseException {
		fileTable.removeRecord(_record);
	}

	public boolean removeRecord(Map<String, Object> keys) throws DatabaseException {
		return fileTable.removeRecord(keys);
	}

	public boolean removeRecord(Object... keys) throws DatabaseException {
		return fileTable.removeRecord(keys);
	}

	public boolean removeRecordWithCascade(Map<String, Object> keys) throws DatabaseException {
		return fileTable.removeRecordWithCascade(keys);
	}

	public boolean removeRecordWithCascade(Object... keys) throws DatabaseException {
		return fileTable.removeRecordWithCascade(keys);
	}

	public void removeRecordWithCascade(FileRecord _record) throws DatabaseException {
		fileTable.removeRecordWithCascade(_record);
	}

	private ArrayList<FileTable.Record> getInternalFRList(Collection<FileRecord> _records)
	{
		if (_records == null)
			throw new NullPointerException("The parameter _records is a null pointer !");
		ArrayList<FileTable.Record> al=new ArrayList<>(_records.size());
		for (FileRecord fr : _records)
			al.add((FileTable.Record) fr);
		return al;
	}
	public void removeRecords(Collection<FileRecord> _records) throws DatabaseException {

		fileTable.removeRecords(getInternalFRList(_records));
	}

	public void removeRecordsWithCascade(Collection<FileRecord> _records) throws DatabaseException {
		fileTable.removeRecordsWithCascade(getInternalFRList(_records));
	}

	public boolean contains(FileRecord _record) throws DatabaseException {
		return fileTable.contains((FileTable.Record) _record);
	}

	public FileRecord getRecord(Object... keys) throws DatabaseException {
		return getFileRecord(fileTable.getRecord(keys));
	}

	public FileRecord getRecord(Map<String, Object> keys) throws DatabaseException {
		return getFileRecord(fileTable.getRecord(keys));
	}

	public DatabaseWrapper getDatabaseWrapper() {
		return fileTable.getDatabaseWrapper();
	}

}
