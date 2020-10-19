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

import com.distrimind.ood.database.DatabaseTransactionEventsTable.AbstractRecord;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.SerializationDatabaseException;
import com.distrimind.util.Bits;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
final class DatabaseDistantTransactionEvent extends Table<DatabaseDistantTransactionEvent.Record> {

	private volatile DatabaseDistantEventsTable databaseDistantEventsTable = null;

	protected DatabaseDistantTransactionEvent() throws DatabaseException {
		super();
	}

	static class Record extends AbstractRecord {
		@Field(index = true)
		protected long localID;


		@ForeignKey
		@PrimaryKey
		protected DatabaseHooksTable.Record hook;

		@Field(limit = 32768)
		protected byte[] peersInformed;

		@Field
		protected boolean peersInformedFull = false;

		public Record() {

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
				/* byte[] _transaction, */boolean peersInformedFull, byte[] peersInformed, boolean force) {
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

		/*public void setHook(DatabaseHooksTable.Record _hook) {
			hook = _hook;
		}*/

		/*
		 * public byte[] getTransaction() { return transaction; }
		 */

		/*
		 * public void setTransaction(byte[] _transaction) throws
		 * SerializationDatabaseException { if (_transaction==null) throw new
		 * NullPointerException("_transaction"); if
		 * (_transaction.length>TRANSACTION_MAX_SIZE_BYTES) throw new
		 * SerializationDatabaseException("Too big transaction ! "); transaction =
		 * _transaction; }
		 */

		long getLocalID() {
			return localID;
		}

		void setLocalID(long _localID) {
			localID = _localID;
		}

		void setPeersInformed(Collection<DecentralizedValue> peers) {
			byte[][] bytes = new byte[peers.size()][];
			int i = 0;
			int size = 2 + peers.size() * 2;
			for (DecentralizedValue id : peers) {
				bytes[i] = id.encode();
				size += bytes[i++].length + 2;
				if (size > 32768) {
					peersInformedFull = true;
					peersInformed = null;
					return;
				}
			}
			peersInformedFull = false;
			peersInformed = new byte[size];
			i = 2;
			Bits.putShort(peersInformed, 0, (short) peers.size());
			for (byte[] b : bytes) {
				Bits.putShort(peersInformed, i, (short) b.length);
				i += 2;
				System.arraycopy(b, 0, peersInformed, i, b.length);
				i += b.length;
			}
		}

		List<DecentralizedValue> getPeersInformed() throws SerializationDatabaseException {
			if (peersInformed == null)
				return new ArrayList<>(0);
			short nbPeers = Bits.getShort(peersInformed, 0);
			ArrayList<DecentralizedValue> res = new ArrayList<>(nbPeers);
			int off = 2;
			for (int i = 0; i < nbPeers; i++) {
				short size = Bits.getShort(peersInformed, 2);
				if (size > 1024)
					throw new SerializationDatabaseException("Invalid data (hook id size est greater to 1024)");

				off += 2;
				res.add(DecentralizedValue.decode(peersInformed, off, size));
				off += size;
			}
			return res;
		}

		boolean addNewHostIDAndTellsIfNewPeersCanBeConcerned(DatabaseHooksTable hooks,
															 DecentralizedValue newHostID) throws DatabaseException {
			if (!hooks.getDatabaseWrapper().getSynchronizer().isSendIndirectTransactions())
				return false;
			if (peersInformedFull)
				return false;
			final List<DecentralizedValue> l = getPeersInformed();
			if (!newHostID.equals(hook.getHostID())) {

				if (!l.contains(newHostID)) {
					l.add(newHostID);
					setPeersInformed(l);
				}
				if (peersInformedFull)
					return false;
			}
			final AtomicBoolean res = new AtomicBoolean(false);
			hooks.getRecords(new Filter<DatabaseHooksTable.Record>() {

				@Override
				public boolean nextRecord(com.distrimind.ood.database.DatabaseHooksTable.Record _record) {
					if (!_record.getHostID().equals(hook.getHostID()) && !l.contains(_record.getHostID())) {
						res.set(true);
						stopTableParsing();
					}

					return false;
				}
			});
			peersInformedFull = !res.get();
			return res.get();
		}

		boolean isConcernedBy(DecentralizedValue newHostID) throws SerializationDatabaseException {
			if (peersInformedFull)
				return false;
			if (newHostID.equals(hook.getHostID()))
				return false;
			List<DecentralizedValue> l = getPeersInformed();
			return !l.contains(newHostID);
		}

		/*Set<AbstractDecentralizedID> getConcernedHosts(Collection<AbstractDecentralizedID> hosts)
				throws SerializationDatabaseException {
			HashSet<AbstractDecentralizedID> res = new HashSet<>();
			if (peersInformedFull)
				return res;
			List<AbstractDecentralizedID> l = getPeersInformed();
			for (AbstractDecentralizedID id : hosts) {
				if (!id.equals(hook.getHostID()) && !l.contains(id))
					res.add(id);
			}
			return res;
		}*/

	}

	// private volatile DatabaseTransactionEventsTable
	// databaseTransactionEventsTable=null;
	private volatile DatabaseHooksTable databaseHooksTable = null;
	// private volatile DatabaseEventsTable databaseEventsTable=null;

	/*
	 * DatabaseTransactionEventsTable getDatabaseTransactionEventsTable() throws
	 * DatabaseException { if (databaseTransactionEventsTable==null)
	 * databaseTransactionEventsTable=(DatabaseTransactionEventsTable)
	 * getDatabaseWrapper().getTableInstance(DatabaseTransactionEventsTable.class);
	 * return databaseTransactionEventsTable; } DatabaseEventsTable
	 * getDatabaseEventsTable() throws DatabaseException { if
	 * (databaseEventsTable==null)
	 * databaseEventsTable=(DatabaseEventsTable)getDatabaseWrapper().
	 * getTableInstance(DatabaseEventsTable.class); return databaseEventsTable; }
	 */

	DatabaseHooksTable getDatabaseHooksTable() throws DatabaseException {
		if (databaseHooksTable == null)
			databaseHooksTable = getDatabaseWrapper().getTableInstance(DatabaseHooksTable.class);
		return databaseHooksTable;
	}

	void cleanDistantTransactions() throws DatabaseException {
		removeRecordsWithCascade(new Filter<DatabaseDistantTransactionEvent.Record>() {

			@Override
			public boolean nextRecord(Record _record) throws DatabaseException {
				return !getDatabaseHooksTable().isConcernedByIndirectTransaction(_record);
			}

		});
	}

	DatabaseDistantTransactionEvent.Record unserializeDistantTransactionEvent(final RandomInputStream ois)
			throws DatabaseException {
		try {
			int size = ois.readShort();
			if (size > 1024)
				throw new SerializationDatabaseException("Invalid data (hook id size est greater to 1024)");
			byte[] b = new byte[size];
			if (ois.read(b) != size)
				throw new SerializationDatabaseException("Impossible to read the expected bytes number : " + size);
			DecentralizedValue hookID;
			try {
				hookID = DecentralizedValue.decode(b);
			} catch (Exception e) {
				throw new SerializationDatabaseException("Impossible to get the hook identifier ! ", e);
			}
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
			byte[] peersInformed = null;
			if (!peersInformedFull) {
				size = ois.readShort();
				if (size > 0) {
					peersInformed = new byte[size];
					if (ois.read(peersInformed) != size)
						throw new SerializationDatabaseException(
								"Impossible to read the expected bytes number : " + size);
				}
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

	/*IDTable getTransactionIDTable() throws DatabaseException {
		if (transactionIDTable == null)
			transactionIDTable = (IDTable) getDatabaseWrapper().getTableInstance(IDTable.class);
		return transactionIDTable;
	}*/

	DatabaseDistantEventsTable getDatabaseDistantEventsTable() throws DatabaseException {
		if (databaseDistantEventsTable == null)
			databaseDistantEventsTable = getDatabaseWrapper()
					.getTableInstance(DatabaseDistantEventsTable.class);
		return databaseDistantEventsTable;
	}

	/*boolean hasTransactionToSend(final DatabaseHooksTable.Record hook, final long lastID) throws DatabaseException {
		final AtomicBoolean found = new AtomicBoolean(false);
		getRecords(new Filter<Record>() {

			@Override
			public boolean nextRecord(Record _record) throws DatabaseException {
				if (_record.isConcernedBy(hook.getHostID())) {
					found.set(true);
					stopTableParsing();
				}
				return false;
			}

		}, "localID>=%lastID AND hook!=%hostOrigin", "lastID", new Long(lastID), "hostOrigin", hook);
		return found.get();
	}*/

	int exportTransactions(final RandomOutputStream oos, final DatabaseHooksTable.Record hook, final int maxEventsRecords,
						   final long fromTransactionID, final AtomicLong nearNextLocalID) throws DatabaseException {
		nearNextLocalID.set(Long.MAX_VALUE);
		final AtomicInteger number = new AtomicInteger(0);
		try {
			getOrderedRecords(new Filter<DatabaseDistantTransactionEvent.Record>() {

				@Override
				public boolean nextRecord(Record _record) throws DatabaseException {
					try {
						if (_record.getLocalID() > fromTransactionID) {
							if (_record.getLocalID() < nearNextLocalID.get())
								nearNextLocalID.set(_record.getLocalID());
						} else if (_record.getLocalID() == fromTransactionID) {
							if (_record.isConcernedBy(hook.getHostID())) {
								oos.writeByte(DatabaseTransactionsPerHostTable.EXPORT_INDIRECT_TRANSACTION);
								byte[] b = _record.getHook().getHostID().encode();
								oos.writeShort((short) b.length);
								oos.write(b);
								oos.writeLong(_record.getID());
								oos.writeLong(_record.getLocalID());
								oos.writeLong(_record.getTimeUTC());
								oos.writeBoolean(_record.isForced());
								oos.writeBoolean(_record.peersInformedFull);
								if (!_record.peersInformedFull) {
									byte[] t = _record.peersInformed;
									if (t == null || t.length == 0) {
										oos.writeShort((short) 0);
									} else {
										oos.writeShort((short) t.length);
										oos.write(t);
									}
								}
								final AtomicInteger number = new AtomicInteger(0);
								getDatabaseDistantEventsTable()
										.getOrderedRecords(new Filter<DatabaseDistantEventsTable.Record>() {

											@Override
											public boolean nextRecord(
													com.distrimind.ood.database.DatabaseDistantEventsTable.Record _record)
													throws DatabaseException {
												_record.export(oos);
												number.incrementAndGet();
												return false;
											}
										}, "transaction=%transaction", new Object[] { "transaction", _record }, true,
												"position");
								if (number.get() == 0)
									throw new IllegalAccessError();
								oos.writeByte(DatabaseTransactionsPerHostTable.EXPORT_INDIRECT_TRANSACTION_FINISHED);
								number.incrementAndGet();
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