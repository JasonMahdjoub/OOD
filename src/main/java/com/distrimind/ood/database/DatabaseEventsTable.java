
/*
Copyright or © or Corp. Jason Mahdjoub (01/04/2013)

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
package com.distrimind.ood.database;

import com.distrimind.ood.database.annotations.AutoPrimaryKey;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.io.*;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
final class DatabaseEventsTable extends Table<DatabaseEventsTable.Record> {

	static class Record extends AbstractRecord {
		@NotNull
		@ForeignKey
		DatabaseTransactionEventsTable.Record transaction;

		Record() {

		}

		<T extends DatabaseRecord> Record(DatabaseTransactionEventsTable.Record transaction, TableEvent<T> _de,
				DatabaseWrapper wrapper) throws DatabaseException {
			super(_de, wrapper);
			if (transaction == null)
				throw new NullPointerException("transaction");
			this.transaction = transaction;
		}

		DatabaseTransactionEventsTable.Record getTransaction() {
			return transaction;
		}

		void setTransaction(DatabaseTransactionEventsTable.Record _transaction) {
			transaction = _transaction;
		}

		void export(RandomOutputStream oos) throws DatabaseException {
			try {
				export(oos,DatabaseTransactionsPerHostTable.EXPORT_DIRECT_TRANSACTION_EVENT);
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}

		}

	}

	public static abstract class DatabaseEventsIterator {
		
		private RandomInputStream dis;
		private RandomInputStream cachedInputStream=null;
		private RandomCacheFileOutputStream cachedOutputStream;

		private int nextEvent=-1;

		protected DatabaseEventsIterator(RandomInputStream dis) throws IOException {
			this(dis, true);
		}

		protected void readNext(DatabaseEventsTable.AbstractRecord event, int position) throws IOException {
			event.setPosition(position);
			final byte eventTypeByte=getDataInputStream().readByte();
			event.setType(eventTypeByte);
			if (getDataOutputStream()!=null)
				getDataOutputStream().writeByte(eventTypeByte);
			final DatabaseEventType type = DatabaseEventType.getEnum(eventTypeByte);

			if (type==null)
				throw new MessageExternalizationException(Integrity.FAIL);

			String concernedTable=getDataInputStream().readString(false, Table.MAX_TABLE_NAME_SIZE_IN_BYTES);
			if (getDataOutputStream()!=null)
				getDataOutputStream().writeString(concernedTable, false, Table.MAX_TABLE_NAME_SIZE_IN_BYTES);
			event.setConcernedTable(concernedTable);
			if (type!=DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE) {
				byte[] spks=getDataInputStream().readBytesArray(false, Table.MAX_PRIMARY_KEYS_SIZE_IN_BYTES);
				if (getDataOutputStream() != null)
					getDataOutputStream().writeBytesArray(spks, false, Table.MAX_PRIMARY_KEYS_SIZE_IN_BYTES);
				event.setConcernedSerializedPrimaryKey(spks);

				if (type.needsNewValue()) {
					byte[] foreignKeys=getDataInputStream().readBytesArray(true, Table.MAX_PRIMARY_KEYS_SIZE_IN_BYTES);
					if (getDataOutputStream() != null)
						getDataOutputStream().writeBytesArray(foreignKeys, true, Table.MAX_PRIMARY_KEYS_SIZE_IN_BYTES);
					event.setConcernedSerializedNewForeignKey(foreignKeys);

					byte[] nonPk=getDataInputStream().readBytesArray(true, Table.MAX_NON_KEYS_SIZE_IN_BYTES);
					if (getDataOutputStream() != null)
						getDataOutputStream().writeBytesArray(nonPk, true, Table.MAX_NON_KEYS_SIZE_IN_BYTES);

					event.setConcernedSerializedNewNonKey(nonPk);

				}
			}
		}
		protected DatabaseEventsIterator(RandomInputStream dis, boolean useCache) throws IOException {
			if (dis==null)
				throw new NullPointerException();
			this.dis=dis;
			if (useCache)
				cachedOutputStream=RandomCacheFileCenter.getSingleton().getNewBufferedRandomCacheFileOutputStream(true, RandomFileOutputStream.AccessMode.READ_AND_WRITE, BufferedRandomInputStream.DEFAULT_MAX_BUFFER_SIZE, 1);
			else
				cachedOutputStream=null;

		}
		
		protected RandomOutputStream getDataOutputStream()
		{
			return cachedOutputStream;
		}
		
		public void reset() throws IOException
		{
			if (cachedOutputStream==null)
				dis.seek(0);
			else if (cachedInputStream==null)
			{
				
				while ((((byte)nextEvent) & DatabaseTransactionsPerHostTable.EXPORT_FINISHED)!=DatabaseTransactionsPerHostTable.EXPORT_FINISHED)
				{
					cachedOutputStream.writeByte(dis.read());
				}
				cachedInputStream=cachedOutputStream.getRandomInputStream();
				if (cachedInputStream.currentPosition()!=0)
					cachedInputStream.seek(0);
				dis=cachedInputStream;
				cachedOutputStream=null;
			}
			else
			{
				cachedInputStream.seek(0);
				dis=cachedInputStream;
			}
		}
		
		protected void setNextEvent(int b) throws IOException
		{
			nextEvent=b;
			if (cachedOutputStream!=null)
				cachedOutputStream.writeByte(b);
		}
		
		protected RandomInputStream getDataInputStream()
		{
			return dis;
		}
		
		public void close() throws IOException
		{
			if (cachedInputStream!=null)
				cachedInputStream.close();
			cachedInputStream=null;
			cachedOutputStream=null;
			dis=null;
		}
		
		public abstract AbstractRecord next() throws DatabaseException;

		public abstract boolean hasNext() throws DatabaseException;
		
		
	}

	abstract static class AbstractRecord extends DatabaseRecord {
		@NotNull
		@AutoPrimaryKey
		private int id;

		@Field
		private int position;

		@Field
		private byte type;
		@NotNull
		@Field(limit = 400, index = true)
		private String concernedTable;

		@Field(limit = Table.MAX_PRIMARY_KEYS_SIZE_IN_BYTES, index = true)
		private byte[] concernedSerializedPrimaryKey;

		@Field(limit = Table.MAX_PRIMARY_KEYS_SIZE_IN_BYTES)
		private byte[] concernedSerializedNewForeignKey;

		@Field(limit = Table.MAX_NON_KEYS_SIZE_IN_BYTES, forceUsingBlobOrClob = true)
		private byte[] concernedSerializedNewNonKey;

		AbstractRecord() {

		}

		AbstractRecord(AbstractRecord record) {
			this.id = record.id;
			this.position = record.position;
			this.type = record.type;
			this.concernedTable = record.concernedTable;
			this.concernedSerializedPrimaryKey = record.concernedSerializedPrimaryKey;
			this.concernedSerializedNewForeignKey = record.concernedSerializedNewForeignKey;
			this.concernedSerializedNewNonKey = record.concernedSerializedNewNonKey;
		}

		<T extends DatabaseRecord> AbstractRecord(TableEvent<T> _de, DatabaseWrapper wrapper) throws DatabaseException {
			if (_de == null)
				throw new NullPointerException("_de");
			if (wrapper == null)
				throw new NullPointerException("wrapper");

			type = _de.getType().getByte();
			//noinspection unchecked
			Table<T> table = (Table<T>)_de.getTable();
			concernedTable = table.getClass().getName();
			if (_de.getType()==DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE)
			{
				concernedSerializedPrimaryKey=null;
				concernedSerializedNewForeignKey=null;
				concernedSerializedNewNonKey=null;
			}
			else {
				if (_de.getMapKeys() != null) {
					concernedSerializedPrimaryKey = table.serializePrimaryKeys(_de.getMapKeys());
				} else if (_de.getOldDatabaseRecord() != null) {
					concernedSerializedPrimaryKey = table.serializePrimaryKeys(_de.getOldDatabaseRecord());
				} else {
					concernedSerializedPrimaryKey = table.serializePrimaryKeys(_de.getNewDatabaseRecord());
				}
				if (concernedSerializedPrimaryKey == null)
					throw new NullPointerException();

				if (_de.getType().needsNewValue()) {
					this.concernedSerializedNewForeignKey = table.serializeFields(_de.getNewDatabaseRecord(), false, true,
							false);
					concernedSerializedNewNonKey = table.serializeFields(_de.getNewDatabaseRecord(), false, false, true);
				} else {
					concernedSerializedNewForeignKey = null;
					concernedSerializedNewNonKey = null;
				}
			}

		}

		void export(RandomOutputStream oos, byte transactionType) throws IOException {
			oos.writeByte(transactionType);
			byte type=getType();
			oos.writeByte(getType());
			oos.writeString(getConcernedTable(), false, Table.MAX_TABLE_NAME_SIZE_IN_BYTES);
			if (type!=DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE.getByte()) {
				oos.writeBytesArray(getConcernedSerializedPrimaryKey(), false, Table.MAX_PRIMARY_KEYS_SIZE_IN_BYTES);
				if (Objects.requireNonNull(DatabaseEventType.getEnum(type)).needsNewValue()) {
					byte[] foreignKeys = getConcernedSerializedNewForeignKey();
					oos.writeBytesArray(foreignKeys, true, Table.MAX_PRIMARY_KEYS_SIZE_IN_BYTES);

					byte[] nonKey = getConcernedSerializedNewNonKey();
					oos.writeBytesArray(nonKey, true, Table.MAX_NON_KEYS_SIZE_IN_BYTES);
				}
			}
		}

		byte getType() {
			return type;
		}

		void setType(byte _type) {
			type = _type;
		}

		String getConcernedTable() {
			return concernedTable;
		}

		String getConcernedPackage()
		{
			return concernedTable.substring(0, concernedTable.lastIndexOf("."));
		}

		void setConcernedTable(String _concernedTable) {
			concernedTable = _concernedTable;
		}

		byte[] getConcernedSerializedPrimaryKey() {
			return concernedSerializedPrimaryKey;
		}

		void setConcernedSerializedPrimaryKey(byte[] _concernedSerializedPrimaryKey) {
			concernedSerializedPrimaryKey = _concernedSerializedPrimaryKey;
		}

		byte[] getConcernedSerializedNewNonKey() {
			return concernedSerializedNewNonKey;
		}

		void setConcernedSerializedNewNonKey(byte[] _concernedSerializedNewNonKey) {
			concernedSerializedNewNonKey = _concernedSerializedNewNonKey;
		}

		int getId() {
			return id;
		}

		int getPosition() {
			return position;
		}

		void setPosition(int order) {
			this.position = order;
		}

		byte[] getConcernedSerializedNewForeignKey() {
			return concernedSerializedNewForeignKey;
		}

		void setConcernedSerializedNewForeignKey(byte[] _concernedSerializedNewForeignKey) {
			concernedSerializedNewForeignKey = _concernedSerializedNewForeignKey;
		}

	}

	DatabaseEventsIterator eventsTableIterator(RandomInputStream ois) throws IOException {
		return new DatabaseEventsIterator(ois) {

			private byte next = 0;
			private int position = 0;

			@Override
			public AbstractRecord next() throws DatabaseException {
				try {
					if (next != DatabaseTransactionsPerHostTable.EXPORT_DIRECT_TRANSACTION_EVENT)
						throw new NoSuchElementException();
					DatabaseEventsTable.Record event = new DatabaseEventsTable.Record();

					readNext(event, position++);
					next = 0;
					return event;
				} catch (Exception e) {
					throw DatabaseException.getDatabaseException(e);
				}
			}

			@Override
			public boolean hasNext() throws DatabaseException {
				try {
					next = getDataInputStream().readByte();
					if (getDataOutputStream()!=null)
						setNextEvent(next);
					return next == DatabaseTransactionsPerHostTable.EXPORT_DIRECT_TRANSACTION_EVENT;
				} catch (Exception e) {
					throw DatabaseException.getDatabaseException(e);
				}
			}
		};

	}

	DatabaseEventsTable() throws DatabaseException {
		super();
	}

}
