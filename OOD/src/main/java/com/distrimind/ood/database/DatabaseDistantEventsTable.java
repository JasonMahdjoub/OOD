
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

import com.distrimind.ood.database.DatabaseEventsTable.AbstractRecord;
import com.distrimind.ood.database.DatabaseEventsTable.DatabaseEventsIterator;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.SerializationDatabaseException;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
final class DatabaseDistantEventsTable extends Table<DatabaseDistantEventsTable.Record> {
	static final int EVENT_MAX_SIZE_BYTES = DatabaseEventsTable.EVENT_MAX_SIZE_BYTES;

	protected DatabaseDistantEventsTable() throws DatabaseException {
		super();
	}

	static class Record extends DatabaseEventsTable.AbstractRecord {
		@NotNull
		@ForeignKey
		DatabaseDistantTransactionEvent.Record transaction;

		Record() {

		}

		Record(DatabaseEventsTable.AbstractRecord record,
				DatabaseDistantTransactionEvent.Record transaction) {
			super(record);
			if (transaction == null)
				throw new NullPointerException("transaction");
			this.transaction = transaction;
		}

		DatabaseDistantTransactionEvent.Record getTransaction() {
			return transaction;
		}

		void setTransaction(DatabaseDistantTransactionEvent.Record _transaction) {
			transaction = _transaction;
		}

		void export(RandomOutputStream oos) throws DatabaseException {
			try {
				oos.writeByte(DatabaseTransactionsPerHostTable.EXPORT_INDIRECT_TRANSACTION_EVENT);
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

	DatabaseEventsIterator distantEventTableIterator(RandomInputStream ois) throws IOException {
		return new DatabaseEventsIterator(ois) {
			private int index = 0;
			private int next = 0;

			@Override
			public boolean hasNext() throws DatabaseException {
				try {
					next = getDataInputStream().readByte();
					if (getDataOutputStream()!=null)
						setNextEvent(next);
						
					
					return next == DatabaseTransactionsPerHostTable.EXPORT_INDIRECT_TRANSACTION_EVENT;
				} catch (Exception e) {
					throw DatabaseException.getDatabaseException(e);
				}
			}

			@Override
			public AbstractRecord next() throws DatabaseException {
				try {
					if (next != DatabaseTransactionsPerHostTable.EXPORT_INDIRECT_TRANSACTION_EVENT)
						throw new NoSuchElementException();
					DatabaseDistantEventsTable.Record event = new DatabaseDistantEventsTable.Record();
					event.setPosition(index++);
					byte b=getDataInputStream().readByte();
					event.setType(b);
					if (getDataOutputStream()!=null)
						getDataOutputStream().writeByte(b);
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
					if (size > Table.maxPrimaryKeysSizeBytes)
						throw new SerializationDatabaseException("Table name too big");
					if (getDataOutputStream()!=null)
						getDataOutputStream().writeInt(size);
					byte[] spks = new byte[size];
					if (getDataInputStream().read(spks) != size)
						throw new SerializationDatabaseException(
								"Impossible to read the expected bytes number : " + size);
					if (getDataOutputStream()!=null)
						getDataOutputStream().write(spks);
					
					event.setConcernedSerializedPrimaryKey(spks);
					DatabaseEventType type = DatabaseEventType.getEnum(event.getType());

					assert type != null;
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
						if (size > EVENT_MAX_SIZE_BYTES)
							throw new SerializationDatabaseException("Transaction  event is too big : " + size);
						if (getDataOutputStream()!=null)
							getDataOutputStream().writeInt(size);
						
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

		};

	}

}
