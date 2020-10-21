
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

import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.LoadToMemory;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
@SuppressWarnings("UnusedReturnValue")
@LoadToMemory
final class IDTable extends Table<IDTable.Record> {

	@SuppressWarnings("FieldMayBeFinal")
	static class Record extends DatabaseRecord {
		@SuppressWarnings("unused")
		@PrimaryKey
		private int id=-1;

		@Field
		protected long transactionID;
	}

	protected IDTable() throws DatabaseException {
		super();
	}

	long getAndIncrementTransactionID() throws DatabaseException {
		return getAndIncrementID(TRANSACTION_ID);
	}

	void setMinimumValidatedTransactionID(long lastID) throws DatabaseException {
		setMinimumValidatedID(TRANSACTION_ID, lastID);
	}

	@SuppressWarnings("SameParameterValue")
	private void setMinimumValidatedID(int id, long lastID) throws DatabaseException {
		getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Void>() {
			@Override
			public Void run() throws Exception {
				Record r = getRecord("id", id);
				if (r == null) {
					addRecord("id", id, "transactionID", lastID);
				} else {
					if (r.transactionID<=lastID)
						r.transactionID=lastID+1;
					updateRecord(r, "transactionID", r.transactionID);
				}
				return null;
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

	@SuppressWarnings("SameParameterValue")
	private long getAndIncrementID(int id) throws DatabaseException {

		return getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Long>() {
			@Override
			public Long run() throws Exception {
				Record r = getRecord("id", id);
				if (r == null) {
					addRecord("id", id, "transactionID", 1L);
					return 0L;
				} else {
					long res = r.transactionID++;
					updateRecord(r, "transactionID", r.transactionID);
					return res;
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

	/*private void decrementID(int id) throws DatabaseException {
		Record r = getRecord("id", id);
		if (r != null) {
			--r.transactionID;
			this.updateRecord(r);
		}
	}*/

	long setLastValidatedTransactionID(long transactionID) throws DatabaseException {
		return setID(GLOBAL_VALIDATED_TRANSACTION_ID, transactionID);
	}

	@SuppressWarnings("SameParameterValue")
	private long setID(int id, long transactionID) throws DatabaseException {
		Record r = getRecord("id", id);
		if (r == null) {
			addRecord("id", id, "transactionID", transactionID);
			return -1;
		} else {
			long prev = r.transactionID;
			r.transactionID = transactionID;
			this.updateRecord(r);
			return prev;
		}
	}

	long getLastTransactionID() throws DatabaseException {
		long id = getLastID(TRANSACTION_ID);
		if (id == -1)
			return -1;
		else
			return id - 1;
	}

	void setLastTransactionID(long id) throws DatabaseException {
		setID(TRANSACTION_ID, id);
	}

	long getLastValidatedTransactionID() throws DatabaseException {
		return getLastID(GLOBAL_VALIDATED_TRANSACTION_ID);
	}

	private long getLastID(int id) throws DatabaseException {
		Record r = getRecord("id", id);

		if (r == null) {
			return -1;
		} else {
			return r.transactionID;
		}
	}

	/*public long getLastTransactionUTC() throws DatabaseException {
		Record r = getRecord("id", LAST_LOCAL_TRANSACTION_UTC);

		if (r == null) {
			return Long.MIN_VALUE;
		} else {
			return r.transactionID;
		}
	}

	public void setTransactionUTCToBeCandidateForLastValue(final long transactionUTC) throws DatabaseException {
		getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Void>() {
			@Override
			public Void run() throws Exception {
				Record r = getRecord("id", LAST_LOCAL_TRANSACTION_UTC);

				if (r == null) {
					r=new Record();
					r.id=LAST_LOCAL_TRANSACTION_UTC;
					r.transactionID=transactionUTC;
					addRecord(r);
				} else if (r.transactionID<transactionUTC) {
					updateRecord(r, "transactionID", transactionUTC);
				}
				return null;
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
	}*/


	private static final int TRANSACTION_ID = 1;
	private static final int GLOBAL_VALIDATED_TRANSACTION_ID = 2;
	//private static final int LAST_LOCAL_TRANSACTION_UTC = 4;
}
