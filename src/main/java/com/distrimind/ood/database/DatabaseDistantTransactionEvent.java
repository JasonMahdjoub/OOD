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

import com.distrimind.ood.database.DatabaseTransactionEventsTable.AbstractRecord;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.SerializationDatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.Reference;
import com.distrimind.util.io.RandomByteArrayOutputStream;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
final class DatabaseDistantTransactionEvent extends Table<DatabaseDistantTransactionEvent.Record> {

	private static final int MAX_SIZE_IN_BYTES_FOR_INFORMED_PEERS =Short.MAX_VALUE;//DatabaseWrapper.MAX_DISTANT_PEERS*DecentralizedValue.MAX_SIZE_IN_BYTES_OF_DECENTRALIZED_VALUE;
	private volatile DatabaseDistantEventsTable databaseDistantEventsTable = null;

	DatabaseDistantTransactionEvent() throws DatabaseException {
		super();
	}

	static class Record extends AbstractRecord {
		@Field(index = true)
		protected long localID;


		@ForeignKey
		@PrimaryKey
		protected DatabaseHooksTable.Record hook;

		@Field(limit = MAX_SIZE_IN_BYTES_FOR_INFORMED_PEERS)
		protected Set<DecentralizedValue> peersInformed=null;

		@Field
		protected boolean peersInformedFull = false;

		Record() {

		}

		@Override
		public boolean equals(Object o) {

			if (o instanceof Record) {
				Record r = ((Record) o);
				return r.id == id && r.hook.equals(hook);
			} else
				return false;
		}

		@Override
		public int hashCode() {
			return (int) this.localID;
		}

		public Record(long _id, long localID, long timeUTC, com.distrimind.ood.database.DatabaseHooksTable.Record _hook,
				/* byte[] _transaction, */boolean peersInformedFull, Set<DecentralizedValue> peersInformed, boolean force) {
			super(_id, timeUTC);
			if (_hook == null)
				throw new NullPointerException("_hook");
			/*
			 * if (_transaction==null) throw new NullPointerException("_transaction");
			 */
			this.localID = localID;
			hook = _hook;
			/*
			 * if (_transaction.length>TRANSACTION_MAX_SIZE_BYTES) throw new
			 * SerializationDatabaseException("Too big transaction ! ");
			 */
			// transaction = _transaction;
			this.peersInformedFull = peersInformedFull;
			if (!peersInformedFull)
				this.peersInformed = peersInformed;
			this.setForced(force);
		}

		public DatabaseHooksTable.Record getHook() {
			return hook;
		}



		long getLocalID() {
			return localID;
		}

		void setLocalID(long _localID) {
			localID = _localID;
		}


		void setPeersInformed(Set<DecentralizedValue> peers) {
			if( peers==null)
				throw new NullPointerException();


			try(RandomByteArrayOutputStream out=new RandomByteArrayOutputStream()) {
				try {
					out.writeCollection(peers, false, MAX_SIZE_IN_BYTES_FOR_INFORMED_PEERS);
					out.flush();
				}
				catch (IOException ignored)
				{
					peersInformedFull = true;
					peersInformed=null;
					return;
				}
			}
			this.peersInformed=peers;
			peersInformedFull = false;
		}

		Set<DecentralizedValue> getPeersInformed() {
			if (peersInformed == null)
				return new HashSet<>();
			return peersInformed;
		}

		boolean addNewHostIDAndTellsIfNewPeersCanBeConcerned(DatabaseHooksTable hooks,
															 DecentralizedValue newHostID) throws DatabaseException {
			if (!hooks.getDatabaseWrapper().getSynchronizer().isSendIndirectTransactions())
				return false;
			if (peersInformedFull)
				return false;
			final Set<DecentralizedValue> l = getPeersInformed();
			if (!newHostID.equals(hook.getHostID())) {

				if (!l.contains(newHostID)) {
					l.add(newHostID);
					setPeersInformed(l);
				}
				if (peersInformedFull)
					return false;
			}

			boolean res=hooks.hasRecords("hostID!=%hid and hostID not in %l", "hid", hook.getHostID(), "l", l);
			peersInformedFull = !res;
			return res;
		}

		boolean isConcernedBy(DecentralizedValue newHostID) throws SerializationDatabaseException {
			if (peersInformedFull)
				return false;
			if (newHostID.equals(hook.getHostID()))
				return false;
			return !getPeersInformed().contains(newHostID);
		}


	}

	private volatile DatabaseHooksTable databaseHooksTable = null;

	DatabaseHooksTable getDatabaseHooksTable() throws DatabaseException {
		if (databaseHooksTable == null)
			databaseHooksTable = getDatabaseWrapper().getTableInstance(DatabaseHooksTable.class);
		return databaseHooksTable;
	}

	void cleanDistantTransactions() throws DatabaseException {
		removeRecordsWithCascade(new Filter<>() {

			@Override
			public boolean nextRecord(Record _record) throws DatabaseException {
				return !getDatabaseHooksTable().isConcernedByIndirectTransaction(_record);
			}

		});
	}

	DatabaseDistantTransactionEvent.Record unserializeDistantTransactionEvent(final RandomInputStream ois)
			throws DatabaseException {
		try {
			DecentralizedValue hookID=ois.readObject(false);
			ArrayList<DatabaseHooksTable.Record> hooks = getDatabaseHooksTable().getRecordsWithAllFields("hostID",
					hookID);
			if (hooks.size() == 0)
				throw new SerializationDatabaseException("Hook not found from this ID : " + hookID);
			else if (hooks.size() > 1)
				throw new IllegalAccessError();

			long transactionID = ois.readLong();
			long localTransactionID = ois.readLong();
			long timeUTC=ois.readLong();
			boolean force = ois.readBoolean();

			boolean peersInformedFull = ois.readBoolean();
			Set<DecentralizedValue> peersInformed = null;
			if (!peersInformedFull) {
				peersInformed=ois.readCollection(false, MAX_SIZE_IN_BYTES_FOR_INFORMED_PEERS, false, DecentralizedValue.class);
			}

			DatabaseDistantTransactionEvent.Record res = new DatabaseDistantTransactionEvent.Record(transactionID,
					localTransactionID, timeUTC, hooks.get(0), peersInformedFull, peersInformed, force);
			try {
				res.getPeersInformed();
			} catch (Exception e) {
				throw new SerializationDatabaseException("Impossible to interpret informed peers", e);
			}
			return res;
		} catch (EOFException e) {
			throw new SerializationDatabaseException("Unexpected EOF", e);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}


	DatabaseDistantEventsTable getDatabaseDistantEventsTable() throws DatabaseException {
		if (databaseDistantEventsTable == null)
			databaseDistantEventsTable = getDatabaseWrapper()
					.getTableInstance(DatabaseDistantEventsTable.class);
		return databaseDistantEventsTable;
	}


	int exportTransactions(final RandomOutputStream oos, final DatabaseHooksTable.Record hook, final int maxEventsRecords,
						   final long fromTransactionID, final AtomicLong nearNextLocalID) throws DatabaseException {
		nearNextLocalID.set(Long.MAX_VALUE);
		final Reference<Integer> number = new Reference<>(0);
		try {
			getOrderedRecords(new Filter<>() {

								  @Override
								  public boolean nextRecord(Record _record) throws DatabaseException {
									  try {
										  if (_record.getLocalID() > fromTransactionID) {
											  if (_record.getLocalID() < nearNextLocalID.get())
												  nearNextLocalID.set(_record.getLocalID());
										  } else if (_record.getLocalID() == fromTransactionID) {
											  if (_record.isConcernedBy(hook.getHostID())) {
												  oos.writeByte(DatabaseTransactionsPerHostTable.EXPORT_INDIRECT_TRANSACTION);
												  oos.writeObject(_record.getHook().getHostID(), false);
												  oos.writeLong(_record.getID());
												  oos.writeLong(_record.getLocalID());
												  oos.writeLong(_record.getTimeUTC());
												  oos.writeBoolean(_record.isForced());
												  oos.writeBoolean(_record.peersInformedFull);
												  if (!_record.peersInformedFull) {
													  oos.writeCollection(_record.peersInformed, false, MAX_SIZE_IN_BYTES_FOR_INFORMED_PEERS);
												  }
												  final Reference<Integer> n = new Reference<>(0);
												  getDatabaseDistantEventsTable()
														  .getOrderedRecords(new Filter<>() {

																				 @Override
																				 public boolean nextRecord(
																						 com.distrimind.ood.database.DatabaseDistantEventsTable.Record _record)
																						 throws DatabaseException {
																					 _record.export(oos);
																					 n.set(n.get() + 1);
																					 return false;
																				 }
																			 }, "transaction=%transaction", new Object[]{"transaction", _record}, true,
																  "position");
												  if (n.get() == 0)
													  throw new IllegalAccessError();
												  oos.writeByte(DatabaseTransactionsPerHostTable.EXPORT_INDIRECT_TRANSACTION_FINISHED);
												  number.set(number.get() + n.get());

												  if (number.get() >= maxEventsRecords)
													  stopTableParsing();
											  }
										  }
										  return false;
									  } catch (Exception e) {
										  throw DatabaseException.getDatabaseException(e);
									  }
								  }
							  }, "hook!=%hook AND localID>=%currentLocalID",
					new Object[] { "hook", hook, "currentLocalID", fromTransactionID}, true, "id");
			return number.get();
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

}