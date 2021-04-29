
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
package com.distrimind.ood.database;

import com.distrimind.ood.database.DatabaseEventsTable.AbstractRecord;
import com.distrimind.ood.database.DatabaseEventsTable.DatabaseEventsIterator;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
final class DatabaseDistantEventsTable extends Table<DatabaseDistantEventsTable.Record> {
	//static final int EVENT_MAX_SIZE_BYTES = DatabaseEventsTable.EVENT_MAX_SIZE_BYTES;

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
				export(oos,DatabaseTransactionsPerHostTable.EXPORT_INDIRECT_TRANSACTION_EVENT);

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
					readNext(event, index++);
					next = 0;
					return event;
				} catch (Exception e) {
					throw DatabaseException.getDatabaseException(e);
				}
			}

		};

	}

}
