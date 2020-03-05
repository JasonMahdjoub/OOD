
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.distrimind.ood.database.annotations.AutoPrimaryKey;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.LoadToMemory;
import com.distrimind.ood.database.annotations.Unique;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.SerializationDatabaseException;
import com.distrimind.util.DecentralizedValue;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
@SuppressWarnings({"SameParameterValue", "UnusedReturnValue"})
@LoadToMemory
final class DatabaseHooksTable extends Table<DatabaseHooksTable.Record> {
	private volatile DatabaseTransactionEventsTable databaseTransactionEventsTable = null;
	private volatile DatabaseTransactionsPerHostTable databaseTransactionsPerHostTable = null;
	private volatile DatabaseDistantTransactionEvent databaseDistantTransactionEvent = null;
	private volatile IDTable idTable = null;
	protected volatile HashSet<String> supportedDatabasePackages = null;
	protected volatile AtomicReference<DatabaseHooksTable.Record> localHost = null;
	protected final HashMap<HostPair, Long> lastTransactionFieldsBetweenDistantHosts = new HashMap<>();

	static class Record extends DatabaseRecord {
		@AutoPrimaryKey
		private int id=-1;

		@Unique
		@Field(limit=3072)
		private DecentralizedValue hostID;

		@Field(limit=Short.MAX_VALUE, forceUsingBlobOrClob = true)
		private String databasePackageNames;

		@Field
		private boolean concernsDatabaseHost;

		@Field
		private long lastValidatedTransaction = -1;

		@Field
		private long lastValidatedDistantTransaction = -1;

		@Override
		public boolean equals(Object o) {
			if (o == null)
				return false;
			if (o == this)
				return true;
			if (o instanceof Record) {
				return id == ((Record) o).id;
			}
			return false;
		}

		@Override
		public int hashCode() {
			return id;
		}

		long getLastValidatedTransaction() {
			return lastValidatedTransaction;
		}

		void setLastValidatedTransaction(long _lastValidatedTransaction) {
			lastValidatedTransaction = _lastValidatedTransaction;
		}

		long getLastValidatedDistantTransaction() {
			return lastValidatedDistantTransaction;
		}

		@SuppressWarnings("SameParameterValue")
		void setLastValidatedDistantTransaction(long _lastValidatedDistantTransaction) throws DatabaseException {
			if (this.lastValidatedDistantTransaction > _lastValidatedDistantTransaction)
				throw DatabaseException.getDatabaseException(new IllegalArgumentException("The host " + this.getHostID()
						+ " can't valid a transaction (N°" + _lastValidatedDistantTransaction
						+ ") lower than the last validated transaction : " + this.lastValidatedDistantTransaction));
			this.lastValidatedDistantTransaction = _lastValidatedDistantTransaction;
		}

		int getID() {
			return id;
		}

		DecentralizedValue getHostID() {
			return hostID;
		}

		protected void setHostID(DecentralizedValue hostID) {
			this.hostID = hostID;
		}

		protected void setConcernsDatabaseHost(boolean v) {
			concernsDatabaseHost = v;
		}

		boolean concernsLocalDatabaseHost() {
			return concernsDatabaseHost;
		}

		protected List<String> setDatabasePackageNames(List<String> packages) {
			if (packages == null || packages.size() == 0) {
				databasePackageNames = null;
				return new ArrayList<>(0);
			}
			StringBuilder sb = new StringBuilder();
			ArrayList<String> packagesList = new ArrayList<>();
			for (int i = 0; i < packages.size(); i++) {
				String p = packages.get(i);
				boolean identical = false;
				for (int j = 0; j < i; j++) {
					if (packages.get(j).equals(p)) {
						identical = true;
						break;
					}
				}
				if (!identical) {
					if (sb.length() != 0)
						sb.append("\\|");
					sb.append(p);
					packagesList.add(p);
				}
			}
			databasePackageNames = sb.toString();
			return packagesList;
		}

		/*protected void addDatabasePackageName(Package p) {
			addDatabasePackageName(p.getSqlTableName());
		}

		protected void addDatabasePackageName(String p) {

			if (databasePackageNames == null || databasePackageNames.length() == 0)
				databasePackageNames = p;
			else {
				String[] packages = getDatabasePackageNames();
				for (String s : packages)
					if (s.equals(p))
						return;
				databasePackageNames += "\\|" + p;
			}
		}*/

		@SuppressWarnings("StringConcatenationInLoop")
		protected List<String> addDatabasePackageNames(List<String> ps) {
			if (ps == null || ps.size() == 0)
				return new ArrayList<>(0);
			if (databasePackageNames == null || databasePackageNames.length() == 0) {
				return setDatabasePackageNames(ps);

			} else {
				String[] packages = getDatabasePackageNames();
				ArrayList<String> packagesList = new ArrayList<>();
				for (int i = 0; i < ps.size(); i++) {
					String p = ps.get(i);

					boolean identical = false;

					for (String s : packages) {
						if (s.equals(p)) {
							identical = true;
							break;
						}
					}
					if (identical)
						continue;
					for (int j = 0; j < i; j++) {
						if (ps.get(j).equals(p)) {
							identical = true;
							break;
						}
					}
					if (identical)
						continue;

					databasePackageNames += "\\|" + p;
					packagesList.add(p);
				}
				return packagesList;
			}
		}

		String[] getDatabasePackageNames() {
			if (databasePackageNames == null || databasePackageNames.length() == 0)
				return null;
			return databasePackageNames.split("\\|");
		}

		boolean isConcernedDatabaseByPackage(String packageName) {
			return databasePackageNames != null
					&& (databasePackageNames.equals(packageName) || databasePackageNames.endsWith("|" + packageName)
							|| databasePackageNames.startsWith(packageName + "|")
							|| databasePackageNames.contains("|" + packageName + "|"));
		}

		protected boolean removePackageDatabase(Package... _packages) {
			if (databasePackageNames == null || databasePackageNames.length() == 0)
				return true;
			else if (_packages == null || _packages.length == 0)
				return false;
			else {
				String[] ps = databasePackageNames.split("\\|");
				ArrayList<String> ps2 = new ArrayList<>(ps.length);
				for (String s : ps) {
					boolean found = false;
					for (Package p : _packages) {
						if (p.getName().equals(s)) {
							found = true;
							break;
						}
					}
					if (!found)
						ps2.add(s);
				}
				StringBuilder sb = new StringBuilder();
				for (String s : ps2) {
					if (sb.length() != 0)
						sb.append("|");
					sb.append(s);
				}
				if (ps2.isEmpty()) {
					databasePackageNames = null;
					return true;
				} else {
					databasePackageNames = sb.toString();
					return false;
				}

			}
		}
	}

	static class HostPair {
		private final DecentralizedValue hostServer, hostToSynchronize;
		private final int hashCode;

		HostPair(DecentralizedValue hostServer, DecentralizedValue hostToSynchronize) {
			if (hostServer == null)
				throw new NullPointerException("hostServer");
			if (hostToSynchronize == null)
				throw new NullPointerException("hostToSynchronize");
			if (hostServer.equals(hostToSynchronize))
				throw new IllegalArgumentException("hostServer can't be equals to hostToSynchronize");
			this.hostServer = hostServer;
			this.hostToSynchronize = hostToSynchronize;
			this.hashCode = hostServer.hashCode() + hostToSynchronize.hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (o == null)
				return false;
			if (o instanceof HostPair) {
				HostPair hp = ((HostPair) o);
				return (hp.hostServer.equals(hostServer) && hp.hostToSynchronize.equals(hostToSynchronize));
			}
			return false;
		}

		@Override
		public int hashCode() {
			return hashCode;
		}

		DecentralizedValue getHostServer() {
			return hostServer;
		}

		DecentralizedValue getHostToSynchronize() {
			return hostToSynchronize;
		}

	}

	boolean isConcernedByIndirectTransaction(DatabaseDistantTransactionEvent.Record indirectTransaction)
			throws DatabaseException {
		for (Map.Entry<HostPair, Long> e : this.lastTransactionFieldsBetweenDistantHosts.entrySet()) {
			if (e.getKey().getHostServer().equals(indirectTransaction.getHook().getHostID())
					&& (getLocalDatabaseHost().getHostID() == null
							|| !e.getKey().getHostToSynchronize().equals(getLocalDatabaseHost().getHostID()))
					&& e.getValue() < indirectTransaction.getID()) {
				return true;
			}
		}
		return false;
	}

	void validateDistantTransactions(DecentralizedValue host,
			final Map<DecentralizedValue, Long> lastTransactionFieldsBetweenDistantHosts, boolean cleanNow)
			throws DatabaseException {
		synchronized (this) {
			for (Map.Entry<DecentralizedValue, Long> e : lastTransactionFieldsBetweenDistantHosts.entrySet()) {
				this.lastTransactionFieldsBetweenDistantHosts.put(new HostPair(host, e.getKey()), e.getValue());
			}
		}
		if (cleanNow) {
			getDatabaseDistantTransactionEvent().cleanDistantTransactions();
		}
	}

	void actualizeLastTransactionID(final List<DecentralizedValue> excludedHooks) throws DatabaseException {
		actualizeLastTransactionID(excludedHooks, getIDTable().getLastTransactionID());
	}

	void actualizeLastTransactionID(final List<DecentralizedValue> excludedHooks, final long lastTransactionID)
			throws DatabaseException {

		final ArrayList<DatabaseHooksTable.Record> toUpdate = new ArrayList<>();
		getRecords(new Filter<DatabaseHooksTable.Record>() {

			@Override
			public boolean nextRecord(final com.distrimind.ood.database.DatabaseHooksTable.Record h)
					throws DatabaseException {
				if (!h.concernsLocalDatabaseHost() && !excludedHooks.contains(h.getHostID())) {
					final AtomicLong actualLastID = new AtomicLong(Long.MAX_VALUE);
					getDatabaseTransactionsPerHostTable()
							.getRecords(new Filter<DatabaseTransactionsPerHostTable.Record>() {

								@Override
								public boolean nextRecord(DatabaseTransactionsPerHostTable.Record _record) {

									if (_record.getTransaction().getID() - 1 < actualLastID.get())
										actualLastID.set(_record.getTransaction().getID() - 1);
									if (actualLastID.get() == h.getLastValidatedTransaction())
										this.stopTableParsing();
									return false;
								}

							}, "hook=%hook", "hook", h);
					if (actualLastID.get() > h.getLastValidatedTransaction()) {
						getDatabaseDistantTransactionEvent()
								.getRecords(new Filter<DatabaseDistantTransactionEvent.Record>() {

									@Override
									public boolean nextRecord(
											com.distrimind.ood.database.DatabaseDistantTransactionEvent.Record _record)
											throws SerializationDatabaseException {
										if (_record.isConcernedBy(h.getHostID())) {
											if (_record.getLocalID() - 1 < actualLastID.get())
												actualLastID.set(_record.getLocalID() - 1);
											if (actualLastID.get() == h.getLastValidatedTransaction())
												this.stopTableParsing();
										}
										return false;
									}
								}, "localID<%maxLocalID AND localID>=%minLocalID and peersInformedFull=%peersInformedFull",
										"maxLocalID", actualLastID.get() - 1, "minLocalID",
										h.getLastValidatedTransaction() + 1, "peersInformedFull",
										Boolean.FALSE);
					}

					if (actualLastID.get() == Long.MAX_VALUE)
						actualLastID.set(lastTransactionID);
					else if (actualLastID.get() < h.getLastValidatedTransaction())
						throw new IllegalAccessError();

					if (h.getLastValidatedTransaction() < actualLastID.get()) {
						h.setLastValidatedTransaction(actualLastID.get());
						toUpdate.add(h);
					}

				}
				return false;
			}
		});
		for (DatabaseHooksTable.Record h : toUpdate) {
			updateRecord(h, "lastValidatedTransaction", h.getLastValidatedTransaction());
		}
	}

	Record getHook(DecentralizedValue host) throws DatabaseException {
		if (host == null)
			throw new NullPointerException("host");
		List<Record> l = getRecordsWithAllFields("hostID", host);
		if (l.size() == 0)
			throw new DatabaseException("Unkown host " + host);
		if (l.size() > 1)
			throw new IllegalAccessError();
		return l.iterator().next();
	}

	/*Long getDistantValidatedTransactionID(AbstractDecentralizedID hostSource, AbstractDecentralizedID hostDestination) {

		return this.lastTransactionFieldsBetweenDistantHosts.get(new HostPair(hostSource, hostDestination));
	}*/

	Map<DecentralizedValue, Long> getLastValidatedDistantTransactions() throws DatabaseException {
		return getDatabaseWrapper()
				.runSynchronizedTransaction(new SynchronizedTransaction<Map<DecentralizedValue, Long>>() {

					@Override
					public Map<DecentralizedValue, Long> run() throws Exception {
						final Map<DecentralizedValue, Long> res = new HashMap<>();

						getRecords(new Filter<DatabaseHooksTable.Record>() {

							@Override
							public boolean nextRecord(Record _record) {
								if (!_record.concernsLocalDatabaseHost())
									res.put(_record.getHostID(), _record.getLastValidatedTransaction());
								return false;
							}
						});
						return res;
					}

					@Override
					public TransactionIsolation getTransactionIsolation() {

						return TransactionIsolation.TRANSACTION_READ_COMMITTED;
					}

					@Override
					public boolean doesWriteData() {
						return false;
					}

					@Override
					public void initOrReset() {
						
					}
				});
	}

	protected DatabaseHooksTable() throws DatabaseException {
		super();
	}

	IDTable getIDTable() throws DatabaseException {
		if (idTable == null)
			idTable = getDatabaseWrapper().getTableInstance(IDTable.class);
		return idTable;
	}

	DatabaseTransactionEventsTable getDatabaseTransactionEventsTable() throws DatabaseException {
		if (databaseTransactionEventsTable == null)
			databaseTransactionEventsTable = getDatabaseWrapper()
					.getTableInstance(DatabaseTransactionEventsTable.class);
		return databaseTransactionEventsTable;
	}

	DatabaseTransactionsPerHostTable getDatabaseTransactionsPerHostTable() throws DatabaseException {
		if (databaseTransactionsPerHostTable == null)
			databaseTransactionsPerHostTable = getDatabaseWrapper()
					.getTableInstance(DatabaseTransactionsPerHostTable.class);
		return databaseTransactionsPerHostTable;
	}

	DatabaseDistantTransactionEvent getDatabaseDistantTransactionEvent() throws DatabaseException {
		if (databaseDistantTransactionEvent == null)
			databaseDistantTransactionEvent = getDatabaseWrapper()
					.getTableInstance(DatabaseDistantTransactionEvent.class);
		return databaseDistantTransactionEvent;
	}

	@SuppressWarnings("UnusedReturnValue")
	DatabaseHooksTable.Record addHooks(final DecentralizedValue hostID, final boolean concernsDatabaseHost,
									   final boolean replaceDistantConflictualRecords,
									   final ArrayList<DecentralizedValue> hostAlreadySynchronized, final List<String> packages)
			throws DatabaseException {

		if (hostID == null)
			throw new NullPointerException("hostID");

		return getDatabaseWrapper()
				.runSynchronizedTransaction(new SynchronizedTransaction<DatabaseHooksTable.Record>() {

					@Override
					public DatabaseHooksTable.Record run() throws Exception {
						if (concernsDatabaseHost && getLocalDatabaseHost() != null)
							throw new DatabaseException("Local database host already set !");
						ArrayList<DatabaseHooksTable.Record> l = getRecordsWithAllFields(
								"hostID", hostID);
						DatabaseHooksTable.Record r;
						if (l.size() > 1)
							throw new DatabaseException("Duplicate host id into the database unexpected !");
						if (l.size() == 0) {
							r = new DatabaseHooksTable.Record();
							r.setHostID(hostID);
							List<String> newAddedPackages = r.setDatabasePackageNames(packages);
							r.setConcernsDatabaseHost(concernsDatabaseHost);
							r.setLastValidatedDistantTransaction(-1);
							r.setLastValidatedTransaction(-1);
							r = addRecord(r);
							
							localHost = null;
							supportedDatabasePackages = null;
							if (!concernsDatabaseHost) {
								r.setLastValidatedTransaction(getDatabaseTransactionEventsTable()
										.addTransactionToSynchronizeTables(newAddedPackages, hostAlreadySynchronized, r,
												replaceDistantConflictualRecords));
								updateRecord(r);
							}
							return r;
						} else {
							r = l.get(0);
							List<String> newAddedPackages = r.addDatabasePackageNames(packages);
							updateRecord(r);
							localHost = null;
							supportedDatabasePackages = null;

							if (!concernsDatabaseHost) {
								r.setLastValidatedTransaction(getDatabaseTransactionEventsTable()
										.addTransactionToSynchronizeTables(newAddedPackages, hostAlreadySynchronized, r,
												replaceDistantConflictualRecords));
								updateRecord(r);
							}

							return r;
						}

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
				});

	}

	DatabaseHooksTable.Record removeHooks(final DecentralizedValue hostID, final Package... packages)
			throws DatabaseException {
		if (hostID == null)
			throw new NullPointerException("hostID");
		return getDatabaseWrapper()
				.runSynchronizedTransaction(new SynchronizedTransaction<DatabaseHooksTable.Record>() {

					@Override
					public Record run() throws Exception {
						ArrayList<DatabaseHooksTable.Record> l = getRecordsWithAllFields(
								"hostID", hostID);
						DatabaseHooksTable.Record r;
						if (l.size() > 1)
							throw new DatabaseException("Duplicate host id into the database unexpected !");
						if (l.size() == 0) {
							return null;
						} else {
							r = l.get(0);
							if (localHost.get().getHostID().equals(hostID))
								localHost=null;
							supportedDatabasePackages = null;
							for (Iterator<Map.Entry<HostPair, Long>> it=lastTransactionFieldsBetweenDistantHosts.entrySet().iterator();it.hasNext(); )
							{
								Map.Entry<HostPair, Long> e=it.next();
								if (e.getKey().getHostServer().equals(hostID)
										|| e.getKey().getHostToSynchronize().equals(hostID))
									it.remove();
							}

							if (r.removePackageDatabase(packages)) {
								removeRecordWithCascade(r);
								getDatabaseTransactionEventsTable().removeTransactionsFromLastID();
								return null;
							} else {
								updateRecord(r);
								getDatabaseTransactionsPerHostTable().removeTransactions(r, packages);
								return r;
							}

						}

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

				});
	}

	
	boolean supportPackage(Package p) throws DatabaseException {
		HashSet<String> hs = this.supportedDatabasePackages;
		if (hs == null) {
			hs = generateSupportedPackages();
		}
		return hs.contains(p.getName()) || (hs.size() == 1 && hs.contains(Object.class.getPackage().getName()));
	}

	private HashSet<String> generateSupportedPackages() throws DatabaseException {

		return getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<HashSet<String>>() {

			@Override
			public HashSet<String> run() throws Exception {
				final HashSet<String> databasePackages = new HashSet<>();
				getRecords(new Filter<DatabaseHooksTable.Record>() {

					@Override
					public boolean nextRecord(DatabaseHooksTable.Record _record) {

						String[] ps = _record.getDatabasePackageNames();
						if (ps == null)
							databasePackages.add(Object.class.getPackage().getName());
						else {
							databasePackages.addAll(Arrays.asList(ps));
						}

						return false;
					}
				});
				if (databasePackages.contains(Object.class.getPackage().getName())) {
					databasePackages.clear();
					databasePackages.add(Object.class.getPackage().getName());
				}
				supportedDatabasePackages = databasePackages;
				return databasePackages;
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_REPEATABLE_READ;
			}

			@Override
			public boolean doesWriteData() {
				return false;
			}

			@Override
			public void initOrReset() {
				
			}
		});
	}

	long getGlobalLastValidatedTransactionID() throws DatabaseException {
		final AtomicLong min = new AtomicLong(Long.MAX_VALUE);

		getRecords(new Filter<DatabaseHooksTable.Record>() {

			@Override
			public boolean nextRecord(DatabaseHooksTable.Record _record) {
				if (!_record.concernsLocalDatabaseHost()) {
					if (min.get() > _record.getLastValidatedTransaction())
						min.set(_record.getLastValidatedTransaction());
				}
				return false;
			}
		});
		if (min.get() == Long.MAX_VALUE)
			min.set(-1);
		return min.get();
	}

	/*
	 * Filter<DatabaseHooksTable.Record> getHooksFilter(DatabaseEventType dte,
	 * DatabaseEventType ..._databaseEventTypes) { return
	 * getHooksFilter(DatabaseEventType.getByte(dte, _databaseEventTypes)); }
	 * 
	 * Filter<DatabaseHooksTable.Record> getHooksFilter(final byte eventsType) {
	 * return new Filter<DatabaseHooksTable.Record>() {
	 * 
	 * @Override public boolean nextRecord(DatabaseHooksTable.Record _record) {
	 * return _record.isConcernedByAllTypes(eventsType); } }; }
	 */

	DatabaseHooksTable.Record getLocalDatabaseHost() throws DatabaseException {
		if (localHost == null) {
			final AtomicReference<DatabaseHooksTable.Record> res = new AtomicReference<>(null);
			getRecords(new Filter<DatabaseHooksTable.Record>() {

				@Override
				public boolean nextRecord(DatabaseHooksTable.Record _record) {
					if (_record.concernsLocalDatabaseHost()) {
						res.set(_record);
						stopTableParsing();
					}

					return false;
				}
			});
			localHost = new AtomicReference<>(res.get());
		}
		return localHost.get();
	}

	Collection<DatabaseHooksTable.Record> resetAllHosts() throws DatabaseException {
		Collection<DatabaseHooksTable.Record> res=getRecords();
		removeAllRecordsWithCascade();
		localHost=null;
		supportedDatabasePackages=null;
		lastTransactionFieldsBetweenDistantHosts.clear();
		return res;
	}

}
