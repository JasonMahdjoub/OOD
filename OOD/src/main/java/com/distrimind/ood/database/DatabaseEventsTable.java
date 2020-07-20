
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
package com.distrimind.ood.database;

import com.distrimind.ood.database.annotations.AutoPrimaryKey;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.SerializationDatabaseException;
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
	static final int EVENT_MAX_SIZE_BYTES = 33554432;

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
				oos.writeByte(DatabaseTransactionsPerHostTable.EXPORT_DIRECT_TRANSACTION_EVENT);
				oos.writeByte(getType());
				oos.writeInt(getConcernedTable().length());
				oos.writeChars(getConcernedTable());
				oos.writeInt(getConcernedSerializedPrimaryKey().length);
				oos.write(getConcernedSerializedPrimaryKey());

				if (Objects.requireNonNull(DatabaseEventType.getEnum(getType())).needsNewValue()) {
					byte[] foreignKeys = getConcernedSerializedNewForeignKey();
					oos.writeInt(foreignKeys.length);
					oos.write(foreignKeys);

					byte[] nonkey = getConcernedSerializedNewNonKey();
					oos.writeInt(nonkey.length);
					oos.write(nonkey);
				}

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
		@Field(limit = 400)
		private String concernedTable;

		@NotNull
		@Field(limit = Table.maxPrimaryKeysSizeBytes, index = true)
		private byte[] concernedSerializedPrimaryKey;

		@Field(limit = Table.maxPrimaryKeysSizeBytes)
		private byte[] concernedSerializedNewForeignKey;

		@Field(limit = Integer.MAX_VALUE, forceUsingBlobOrClob = true)
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
			Table<T> table = _de.getTable(wrapper);
			concernedTable = table.getClass().getName();
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

		byte getType() {
			return type;
		}

		void setType(byte _type) {
			type = _type;
		}

		String getConcernedTable() {
			return concernedTable;
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
					event.setPosition(position++);
					byte b=getDataInputStream().readByte();
					if (getDataOutputStream()!=null)
						getDataOutputStream().writeByte(b);
					event.setType(b);

					int size = getDataInputStream().readInt();
					if (size > Table.maxTableNameSizeBytes)
						throw new SerializationDatabaseException("Table name too big");
					if (getDataOutputStream()!=null)
						getDataOutputStream().writeInt(size);
					
					
					char[] chrs = new char[size];
					for (int i = 0; i < size; i++)
					{
						chrs[i] = getDataInputStream().readChar();
						if (getDataOutputStream()!=null)
							getDataOutputStream().writeChar(chrs[i]);
					}
					event.setConcernedTable(String.valueOf(chrs));
					size = getDataInputStream().readInt();
					if (getDataOutputStream()!=null)
						getDataOutputStream().writeInt(size);
					if (size > Table.maxPrimaryKeysSizeBytes)
						throw new SerializationDatabaseException("Table name too big");
					byte[] spks = new byte[size];
					if (getDataInputStream().read(spks) != size)
						throw new SerializationDatabaseException(
								"Impossible to read the expected bytes number : " + size);
					if (getDataOutputStream()!=null)
						getDataOutputStream().write(spks);
					event.setConcernedSerializedPrimaryKey(spks);
					DatabaseEventType type = DatabaseEventType.getEnum(event.getType());
					if (type == null)
						throw new SerializationDatabaseException(
								"Impossible to decode database event type : " + event.getType());

					if (type.needsNewValue()) {
						size = getDataInputStream().readInt();
						if (size > Table.maxPrimaryKeysSizeBytes)
							throw new SerializationDatabaseException("Transaction  event is too big : " + size);
						if (getDataOutputStream()!=null)
							getDataOutputStream().writeInt(size);
						byte[] foreignKeys = new byte[size];
						if (getDataInputStream().read(foreignKeys) != size)
							throw new SerializationDatabaseException(
									"Impossible to read the expected bytes number : " + size);
						if (getDataOutputStream()!=null)
							getDataOutputStream().write(foreignKeys);

						event.setConcernedSerializedNewForeignKey(foreignKeys);

						size = getDataInputStream().readInt();
						if (getDataOutputStream()!=null)
							getDataOutputStream().writeInt(size);
						if (size > EVENT_MAX_SIZE_BYTES)
							throw new SerializationDatabaseException("Transaction  event is too big : " + size);
						byte[] nonpk = new byte[size];
						if (getDataInputStream().read(nonpk) != size)
							throw new SerializationDatabaseException(
									"Impossible to read the expected bytes number : " + size);
						if (getDataOutputStream()!=null)
							getDataOutputStream().write(nonpk);

						event.setConcernedSerializedNewNonKey(nonpk);
					}

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

	protected DatabaseEventsTable() throws DatabaseException {
		super();
	}

}
