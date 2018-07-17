
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

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.SerializationDatabaseException;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.Bits;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
final class DatabaseTransactionEventsTable extends Table<DatabaseTransactionEventsTable.Record> {
	private volatile IDTable transactionIDTable = null;
	private volatile DatabaseTransactionsPerHostTable databaseTransactionsPerHostTable = null;
	private volatile DatabaseEventsTable databaseEventsTable = null;
	private volatile DatabaseHooksTable databaseHooksTable = null;
	private volatile IDTable idTable = null;

	static abstract class AbstractRecord extends DatabaseRecord {
		@PrimaryKey
		protected long id;

		@Field
		private boolean force = false;

		AbstractRecord() {

		}

		AbstractRecord(long id) {
			this.id = id;
		}

		void setID(long id) {
			this.id = id;
		}

		long getID() {
			return id;
		}

		public boolean isForce() {
			return force;
		}

		public void setForce(boolean force) {
			this.force = force;
		}

		@Override
		public boolean equals(Object record) {
			if (record == null)
				return false;
			else if (record instanceof AbstractRecord)
				return id == ((AbstractRecord) record).id;
			return false;
		}

		@Override
		public int hashCode() {
			return (int) id;
		}

	}

	private static final int concernedHostsSizeLimit = 10000;

	static class Record extends AbstractRecord {

		@NotNull
		@Field
		protected String concernedDatabasePackage;

		@Field(limit = concernedHostsSizeLimit)
		private byte[] concernedHosts;
		//private AbstractDecentralizedID newHostID;

		Record() {
			this.concernedDatabasePackage = null;
			this.concernedHosts = null;
		}

		Record(long id, String concernedDatabasePackage) {
			super(id);
			this.concernedDatabasePackage = concernedDatabasePackage;
			this.concernedHosts = null;
		}

		Record(long id, String concernedDatabasePackage, Set<AbstractDecentralizedID> concernedHosts) {
			this(id, concernedDatabasePackage);

			setConcernedHosts(concernedHosts);
		}

		/*boolean isTemporaryTransaction() {
			return id < -1;
		}*/

		@Override
		public boolean equals(Object o) {
			if (o instanceof Record) {
				return ((Record) o).getID() == this.getID();
			} else
				return false;
		}

		@Override
		public int hashCode() {
			return (int) getID();
		}

		boolean isConcernedBy(Package p) {
			return p.getName().equals(concernedDatabasePackage);
		}

		boolean isConcernedByOneOf(Package... ps) {
			if (ps == null)
				return false;
			for (Package p : ps)
				if (concernedDatabasePackage.equals(p.getName()))
					return true;
			return false;
		}

		void setConcernedHosts(Collection<AbstractDecentralizedID> peers) {
			if (peers == null || peers.isEmpty()) {
				concernedHosts = null;
				setForce(false);
				return;
			} else
				setForce(true);

			byte[][] bytes = new byte[peers.size()][];
			int i = 0;
			int size = 2 + peers.size() * 2;
			for (AbstractDecentralizedID id : peers) {
				bytes[i] = id.getBytes();
				size += bytes[i++].length + 2;
			}
			if (size > concernedHostsSizeLimit) {
				concernedHosts = null;
			} else {
				concernedHosts = new byte[size];
				i = 2;
				Bits.putShort(concernedHosts, 0, (short) peers.size());
				for (byte[] b : bytes) {
					Bits.putShort(concernedHosts, i, (short) b.length);
					i += 2;
					System.arraycopy(b, 0, concernedHosts, i, b.length);
					i += b.length;
				}
			}
		}

		List<AbstractDecentralizedID> getConcernedHosts() throws SerializationDatabaseException {
			if (concernedHosts == null)
				return new ArrayList<>(0);
			short nbPeers = Bits.getShort(concernedHosts, 0);
			ArrayList<AbstractDecentralizedID> res = new ArrayList<>(nbPeers);
			int off = 2;
			for (int i = 0; i < nbPeers; i++) {
				short size = Bits.getShort(concernedHosts, 2);
				if (size > 1024)
					throw new SerializationDatabaseException("Invalid data (hook id size est greater to 1024)");

				off += 2;
				res.add(AbstractDecentralizedID.instanceOf(concernedHosts, off, size));
				off += size;
			}
			return res;
		}

		boolean isConcernedBy(AbstractDecentralizedID newHostID) throws SerializationDatabaseException {
			//this.newHostID = newHostID;
			if (concernedHosts == null)
				return true;
			List<AbstractDecentralizedID> l = getConcernedHosts();
			return !l.contains(newHostID);
		}

	}

	protected DatabaseTransactionEventsTable() throws DatabaseException {
		super();
	}

	void removeUnusedTransactions() throws DatabaseException {

		getDatabaseWrapper().runTransaction(new Transaction() {

			@Override
			public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
				final DatabaseTransactionsPerHostTable t = getDatabaseTransactionsPerHostTable();
				DatabaseTransactionEventsTable.this
						.removeRecordsWithCascade(new Filter<DatabaseTransactionEventsTable.Record>() {

							@Override
							public boolean nextRecord(
									com.distrimind.ood.database.DatabaseTransactionEventsTable.Record _record)
									throws DatabaseException {
								return !t.hasRecordsWithAllFields("transaction", _record);
							}
						});
				return null;
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_SERIALIZABLE;
			}

			@Override
			public boolean doesWriteData() {
				return true;
			}

			@Override
			public void initOrReset() {
				
			}
		}, true);

	}

	void removeTransactionsFromLastID() throws DatabaseException {
		long globalLast = getDatabaseHooksTable().getGlobalLastValidatedTransactionID();
		long prevGlobalLast = getIDTable().getLastValidatedTransactionID();
		if (prevGlobalLast != globalLast) {
			getIDTable().setLastValidatedTransactionIDI(globalLast);
			removeTransactionUntilID(globalLast);
		}

	}

	protected long addTransactionToSynchronizeTables(final List<String> databasePackages,
			final ArrayList<AbstractDecentralizedID> hostAlreadySynchronized, final DatabaseHooksTable.Record hook,
			boolean force) throws DatabaseException {
		final Set<String> packageSynchroOneTime = new HashSet<>();

		getDatabaseHooksTable().getRecords(new Filter<DatabaseHooksTable.Record>() {

			@Override
			public boolean nextRecord(DatabaseHooksTable.Record _record) {
				if (!_record.concernsLocalDatabaseHost() && hostAlreadySynchronized.contains(_record.getHostID())) {
					for (String p : _record.getDatabasePackageNames()) {
						if (databasePackages.contains(p)) {
							packageSynchroOneTime.add(p);
						}
					}

				}
				return false;
			}
		});

		for (String databasePackage : databasePackages)
			if (!packageSynchroOneTime.contains(databasePackage))
				addTransactionToSynchronizeTables(databasePackage, hook, force);

		if (packageSynchroOneTime.size() > 0) {
			final AtomicLong lastTransactionID = new AtomicLong(Long.MAX_VALUE);
			getDatabaseHooksTable().getRecords(new Filter<DatabaseHooksTable.Record>() {

				@Override
				public boolean nextRecord(DatabaseHooksTable.Record _record) {
					if (!_record.concernsLocalDatabaseHost() && !_record.getHostID().equals(hook.getHostID())) {
						if (lastTransactionID.get() > _record.getLastValidatedTransaction()) {
							lastTransactionID.set(_record.getLastValidatedTransaction());
						}
					}
					return false;
				}
			});
			if (lastTransactionID.get() == Long.MAX_VALUE)
				lastTransactionID.set(-1);

			if (lastTransactionID.get() < getIDTable().getLastTransactionID()) {
				StringBuilder sb = new StringBuilder();
				int index = 0;
				Map<String, Object> parameters = new HashMap<>();
				for (String p : packageSynchroOneTime) {
					if (sb.length() > 0)
						sb.append(" OR ");
					else
						sb.append(" AND (");
					String var = "var" + (index++);
					sb.append("concernedDatabasePackage=%").append(var);
					parameters.put(var, p);
				}
				sb.append(")");
				parameters.put("lastID", lastTransactionID.get());

				updateRecords(new AlterRecordFilter<DatabaseTransactionEventsTable.Record>() {

					@Override
					public void nextRecord(Record _record) throws DatabaseException {
						if (!_record.isConcernedBy(hook.getHostID())) {
							List<AbstractDecentralizedID> l = _record.getConcernedHosts();
							l.add(hook.getHostID());
							_record.setConcernedHosts(l);
							update();
						}
					}
				}, "id>%lastID" + sb.toString(), parameters);
				getRecords(new Filter<DatabaseTransactionEventsTable.Record>() {

					@Override
					public boolean nextRecord(Record _record) throws DatabaseException {
						DatabaseTransactionsPerHostTable.Record r = new DatabaseTransactionsPerHostTable.Record();
						r.set(_record, hook);
						getDatabaseTransactionsPerHostTable().addRecord(r);
						return false;
					}

				}, "id>%lastID" + sb.toString(), parameters);
			}
			return lastTransactionID.get();
		} else
			return -1;

	}

	@SuppressWarnings("unchecked")
	private void addEventsForTablesToSynchronize(
			final AtomicReference<DatabaseTransactionEventsTable.Record> transaction, final String databasePackage,
			final DatabaseHooksTable.Record hook, Class<? extends Table<?>> tableClass,
			Set<Class<? extends Table<?>>> tablesDone, final AtomicInteger currentEventPos, final long maxEvents,
			final boolean force) throws DatabaseException {
		if (tablesDone.contains(tableClass))
			return;

		final Table<DatabaseRecord> table = (Table<DatabaseRecord>) getDatabaseWrapper().getTableInstance(tableClass);
		if (!table.supportSynchronizationWithOtherPeers())
			return;

		tablesDone.add(tableClass);

		for (ForeignKeyFieldAccessor fa : table.getForeignKeysFieldAccessors()) {
			addEventsForTablesToSynchronize(transaction, databasePackage, hook,
					(Class<? extends Table<?>>) (fa.getPointedTable().getClass()), tablesDone, currentEventPos,
					maxEvents, force);
		}
		table.getRecords(new Filter<DatabaseRecord>() {

			@Override
			public boolean nextRecord(DatabaseRecord _record) throws DatabaseException {
				DatabaseEventsTable.Record event = new DatabaseEventsTable.Record(transaction.get(),
						new TableEvent<>(-1, DatabaseEventType.ADD, null, _record, null),
						getDatabaseWrapper());
				event.setPosition(currentEventPos.getAndIncrement());
				getDatabaseEventsTable().addRecord(event);
				if (currentEventPos.get() > maxEvents)
					throw new IllegalAccessError(currentEventPos.get() + " ; " + maxEvents);
				if (currentEventPos.get() == maxEvents) {

					DatabaseTransactionsPerHostTable.Record trhost = new DatabaseTransactionsPerHostTable.Record();
					trhost.set(transaction.get(), hook);
					getDatabaseTransactionsPerHostTable().addRecord(trhost);

					DatabaseTransactionEventsTable.Record tr = new DatabaseTransactionEventsTable.Record();
					tr.id = getTransactionIDTable().getAndIncrementTransactionID();
					tr.concernedDatabasePackage = databasePackage;
					tr.setConcernedHosts(Collections.singletonList(hook.getHostID()));
					tr.setForce(force);
					transaction.set(addRecord(tr));
					currentEventPos.set(0);

				}
				return false;
			}
		});
	}

	protected void addTransactionToSynchronizeTables(final String databasePackage, final DatabaseHooksTable.Record hook,
			final boolean force) throws DatabaseException {
		// final long previousLastTransacionID=getIDTable().getLastTransactionID();
		DatabaseTransactionEventsTable.Record tr = new DatabaseTransactionEventsTable.Record();
		tr.id = getTransactionIDTable().getAndIncrementTransactionID();
		tr.concernedDatabasePackage = databasePackage;
		tr.setConcernedHosts(Collections.singletonList(hook.getHostID()));
		tr.setForce(force);

		AtomicReference<DatabaseTransactionEventsTable.Record> transaction = new AtomicReference<>(addRecord(tr));
		AtomicInteger currentEventPos = new AtomicInteger(0);
		Set<Class<? extends Table<?>>> tables = getDatabaseWrapper().getDatabaseConfiguration(databasePackage)
				.getTableClasses();
		Set<Class<? extends Table<?>>> tablesDone = new HashSet<>();

		for (Class<? extends Table<?>> c : tables) {
			addEventsForTablesToSynchronize(transaction, databasePackage, hook, c, tablesDone, currentEventPos,
					getDatabaseWrapper().getMaxTransactionEventsKeepedIntoMemory(), force);
		}
		if (currentEventPos.get() > 0) {
			DatabaseTransactionsPerHostTable.Record trhost = new DatabaseTransactionsPerHostTable.Record();
			trhost.set(transaction.get(), hook);
			getDatabaseTransactionsPerHostTable().addRecord(trhost);
		} else {
			removeRecord(transaction.get());
		}
		getDatabaseHooksTable().actualizeLastTransactionID(new ArrayList<AbstractDecentralizedID>(0));
	}

	DatabaseTransactionsPerHostTable getDatabaseTransactionsPerHostTable() throws DatabaseException {
		if (databaseTransactionsPerHostTable == null)
			databaseTransactionsPerHostTable = (DatabaseTransactionsPerHostTable) getDatabaseWrapper()
					.getTableInstance(DatabaseTransactionsPerHostTable.class);
		return databaseTransactionsPerHostTable;

	}

	IDTable getTransactionIDTable() throws DatabaseException {
		if (transactionIDTable == null)
			transactionIDTable = (IDTable) getDatabaseWrapper().getTableInstance(IDTable.class);
		return transactionIDTable;
	}

	DatabaseEventsTable getDatabaseEventsTable() throws DatabaseException {
		if (databaseEventsTable == null)
			databaseEventsTable = (DatabaseEventsTable) getDatabaseWrapper()
					.getTableInstance(DatabaseEventsTable.class);
		return databaseEventsTable;

	}

	DatabaseHooksTable getDatabaseHooksTable() throws DatabaseException {
		if (databaseHooksTable == null)
			databaseHooksTable = (DatabaseHooksTable) getDatabaseWrapper().getTableInstance(DatabaseHooksTable.class);
		return databaseHooksTable;

	}

	IDTable getIDTable() throws DatabaseException {
		if (idTable == null)
			idTable = (IDTable) getDatabaseWrapper().getTableInstance(IDTable.class);
		return idTable;

	}

	void removeTransactionUntilID(long lastTransactionID) throws DatabaseException {
		removeRecordsWithCascade("id<=%lastID AND id>-1", "lastID", lastTransactionID);
	}

	void cleanTmpTransactions() throws DatabaseException {
		getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

			@Override
			public Void run() throws Exception {
				removeRecordsWithCascade("id<-1");
				return null;
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_READ_COMMITTED;
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

}
