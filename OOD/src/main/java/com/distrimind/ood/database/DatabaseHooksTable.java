
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

import com.distrimind.ood.database.annotations.*;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.SerializationDatabaseException;
import com.distrimind.ood.database.messages.AuthenticatedCentralDatabaseBackupMessage;
import com.distrimind.ood.database.messages.AuthenticatedMessageDestinedToCentralDatabaseBackup;
import com.distrimind.ood.database.messages.IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.Reference;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.EncryptionProfileProvider;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
@SuppressWarnings({"SameParameterValue", "UnusedReturnValue"})
@LoadToMemory
final class DatabaseHooksTable extends Table<DatabaseHooksTable.Record> {
	static final int PACKAGES_TO_SYNCHRONIZE_LENGTH=DatabaseWrapper.MAX_PACKAGE_TO_SYNCHRONIZE*70;
	private static final int SIZE_IN_BYTES_OF_AUTHENTICATED_MESSAGES_QUEUE_TO_SEND =DatabaseWrapper.MAX_DISTANT_PEERS*2*AuthenticatedP2PMessage.MAX_AUTHENTICATED_P2P_MESSAGE_SIZE_IN_BYTES +2;
	private static final int SIZE_IN_BYTES_OF_AUTHENTICATED_MESSAGES_QUEUE_DESTINED_TO_CENTRAL_DATABASE_BACKUP =10* AuthenticatedCentralDatabaseBackupMessage.MAX_AUTHENTICATED_MESSAGE_SIZE_IN_BYTES +2;

	private volatile DatabaseTransactionEventsTable databaseTransactionEventsTable = null;
	private volatile DatabaseTransactionsPerHostTable databaseTransactionsPerHostTable = null;
	private volatile DatabaseDistantTransactionEvent databaseDistantTransactionEvent = null;
	private volatile IDTable idTable = null;
	private volatile HashSet<String> supportedDatabasePackages = null;
	protected volatile DatabaseHooksTable.Record localHost = null;
	private final HashMap<HostPair, Long> lastTransactionFieldsBetweenDistantHosts = new HashMap<>();

	public enum PairingState
	{
		PAIRED,
		REMOVED
	}

	@SuppressWarnings("FieldMayBeFinal")
	static class Record extends DatabaseRecord {
		@AutoPrimaryKey
		private int id=-1;

		@Unique
		@Field(limit=3072)
		private DecentralizedValue hostID;

		@Field(limit=PACKAGES_TO_SYNCHRONIZE_LENGTH, forceUsingBlobOrClob = true)
		private Set<String> databasePackageNames;

		@Field(limit=PACKAGES_TO_SYNCHRONIZE_LENGTH, forceUsingBlobOrClob = true)
		private Set<String> databasePackageNamesThatDoNotUseExternalBackup =new HashSet<>();

		@Field
		private long lastLocalAuthenticatedP2PMessageID=0;

		@Field
		private long lastDistantAuthenticatedP2PMessageID=-1;

		@Field
		private boolean concernsDatabaseHost;

		@Field
		private long lastValidatedLocalTransactionID = -1;

		@Field
		private long lastValidatedDistantTransactionID = -1;

		@Field
		private long lastValidatedDistantTransactionUTCMs=Long.MIN_VALUE;

		@Field(limit = SIZE_IN_BYTES_OF_AUTHENTICATED_MESSAGES_QUEUE_TO_SEND, forceUsingBlobOrClob = true)
		private LinkedList<AuthenticatedP2PMessage> authenticatedMessagesQueueToSend=null;

		@Field(limit = SIZE_IN_BYTES_OF_AUTHENTICATED_MESSAGES_QUEUE_DESTINED_TO_CENTRAL_DATABASE_BACKUP, forceUsingBlobOrClob = true)
		private LinkedList<AuthenticatedMessageDestinedToCentralDatabaseBackup> authenticatedMessagesQueueDestinedToCentralDatabaseBackup=null;

		@Field
		@NotNull
		private PairingState pairingState;

		boolean willBeRemoved()
		{
			return authenticatedMessagesQueueToSend!=null && authenticatedMessagesQueueToSend.stream().anyMatch(a -> a instanceof HookRemoveRequest && ((HookRemoveRequest) a).getRemovedHookID().equals(hostID));
		}

		void setRemoved()
		{
			pairingState=PairingState.REMOVED;
			concernsDatabaseHost=false;
			lastLocalAuthenticatedP2PMessageID=0;
			lastDistantAuthenticatedP2PMessageID=-1;
			lastValidatedDistantTransactionID=-1;
			lastValidatedLocalTransactionID=-1;
			lastValidatedDistantTransactionUTCMs=Long.MIN_VALUE;
			databasePackageNames=null;
			authenticatedMessagesQueueToSend=null;

		}

		Set<String> getDatabasePackageNamesThatDoNotUseExternalBackup() {
			return databasePackageNamesThatDoNotUseExternalBackup;
		}

		boolean validateDistantAuthenticatedP2PMessage(AuthenticatedP2PMessage message, DatabaseHooksTable table) throws DatabaseException {
			if (message.getMessageID()<=lastDistantAuthenticatedP2PMessageID) {
				return false;
			}
			else
			{
				lastDistantAuthenticatedP2PMessageID=message.getMessageID();
				table.updateRecord(this, "lastDistantAuthenticatedP2PMessageID", lastDistantAuthenticatedP2PMessageID);
				table.updateLocalDatabaseHostIfNecessary(this);
				return true;
			}
		}

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



		long getLastValidatedDistantTransactionUTCMs() {
			return lastValidatedDistantTransactionUTCMs;
		}


		PairingState getPairingState() {
			return pairingState;
		}



		void setPairingState(PairingState pairingState) {
			this.pairingState = pairingState;
		}

		@Override
		public int hashCode() {
			return id;
		}

		long getLastValidatedLocalTransactionID() {
			return lastValidatedLocalTransactionID;
		}

		void setLastValidatedLocalTransactionID(long _lastValidatedTransaction) {
			lastValidatedLocalTransactionID = _lastValidatedTransaction;
		}

		long getLastValidatedDistantTransactionID() {
			return lastValidatedDistantTransactionID;
		}

		@SuppressWarnings("SameParameterValue")
		void setLastValidatedDistantTransactionID(long _lastValidatedDistantTransaction) throws DatabaseException {
			if (this.lastValidatedDistantTransactionID > _lastValidatedDistantTransaction)
				throw DatabaseException.getDatabaseException(new IllegalArgumentException("The host " + this.getHostID()
						+ " can't valid a transaction (N°" + _lastValidatedDistantTransaction
						+ ") lower than the last validated transaction : " + this.lastValidatedDistantTransactionID));
			this.lastValidatedDistantTransactionID = _lastValidatedDistantTransaction;
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

		void offerNewAuthenticatedP2PMessage(DatabaseWrapper wrapper, AuthenticatedP2PMessage message, AbstractSecureRandom random, EncryptionProfileProvider encryptionProfileProvider, AlterRecordFilter<Record> alterRecordFilter) throws DatabaseException {
			if (message==null)
				throw new NullPointerException();
			if (!(message instanceof DatabaseEvent))
				throw new IllegalAccessError();
			if (authenticatedMessagesQueueToSend==null)
				authenticatedMessagesQueueToSend=new LinkedList<>();
			message.setMessageID(lastLocalAuthenticatedP2PMessageID++);

			message.generateAndSetSignatures(random, encryptionProfileProvider);
			authenticatedMessagesQueueToSend.addLast(message);
			if (message instanceof HookRemoveRequest && ((HookRemoveRequest) message).getRemovedHookID().equals(hostID))
				alterRecordFilter.update("authenticatedMessagesQueueToSend", authenticatedMessagesQueueToSend, "lastLocalAuthenticatedP2PMessageID", lastLocalAuthenticatedP2PMessageID, "pairingState", PairingState.REMOVED);
			else
				alterRecordFilter.update("authenticatedMessagesQueueToSend", authenticatedMessagesQueueToSend, "lastLocalAuthenticatedP2PMessageID", lastLocalAuthenticatedP2PMessageID);
			wrapper.getDatabaseHooksTable().updateLocalDatabaseHostIfNecessary(this);
			message.messageReadyToSend();
			wrapper.getSynchronizer().notifyNewAuthenticatedMessage(message);
		}



		boolean hasNoAuthenticatedMessagesQueueToSend()
		{
			return authenticatedMessagesQueueToSend==null || authenticatedMessagesQueueToSend.size()==0;
		}

		List<AuthenticatedP2PMessage> getAuthenticatedMessagesQueueToSend(Map<DecentralizedValue, DatabaseWrapper.ConnectedPeers> connectedPeers) throws DatabaseException {
			if (authenticatedMessagesQueueToSend==null)
				return null;
			List<AuthenticatedP2PMessage> res=new ArrayList<>();
			for (AuthenticatedP2PMessage m : authenticatedMessagesQueueToSend)
			{
				if (m instanceof HookSynchronizeRequest)
				{
					DatabaseWrapper.ConnectedPeers cp=connectedPeers.get(m.getHostDestination());
					if (cp==null || !cp.compatibleDatabases.containsAll(((HookSynchronizeRequest) m).getPackagesToSynchronize(m.getHostSource()).keySet()))
						break;
				}
				res.add(m);
			}
			return res;
		}

		LinkedList<AuthenticatedMessageDestinedToCentralDatabaseBackup> getAuthenticatedMessagesQueueDestinedToCentralDatabaseBackup() {
			return authenticatedMessagesQueueDestinedToCentralDatabaseBackup;
		}

		List<IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup> getAuthenticatedMessagesQueueToSendToCentralDatabaseBackup(AbstractSecureRandom random, EncryptionProfileProvider encryptionProfileProvider, Map<DecentralizedValue, DatabaseWrapper.ConnectedPeersWithCentralBackup> connectedPeersWithCentralDatabaseBackup) throws DatabaseException {
			assert concernsDatabaseHost;
			List<IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup> res=new ArrayList<>();


			if (authenticatedMessagesQueueToSend==null)
				return res;
			Map<DecentralizedValue, ArrayList<AuthenticatedP2PMessage>> resTmp=new HashMap<>();
			for (AuthenticatedP2PMessage a : authenticatedMessagesQueueToSend)
			{
				if (a instanceof HookSynchronizeRequest)
				{
					HookSynchronizeRequest hsr=(HookSynchronizeRequest)a;
					DatabaseWrapper.ConnectedPeersWithCentralBackup cpcdb=connectedPeersWithCentralDatabaseBackup.get(hsr.getHostDestination());

					if (cpcdb==null)
						break;
					if (!cpcdb.compatibleDatabasesFromCentralDatabaseBackup.containsAll(hsr.getPackagesToSynchronize(hsr.getHostSource()).keySet()))
						break;
				}
				ArrayList<AuthenticatedP2PMessage> l = resTmp.computeIfAbsent(a.getHostDestination(), k -> new ArrayList<>());
				l.add(a);
			}
			for (Map.Entry<DecentralizedValue, ArrayList<AuthenticatedP2PMessage>> e : resTmp.entrySet())
			{
				res.add(new IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup(e.getValue(), random, encryptionProfileProvider));
			}
			return res;
		}


		boolean removeAuthenticatedMessage(AuthenticatedP2PMessage message)
		{
			if (authenticatedMessagesQueueToSend==null)
				return false;
			return authenticatedMessagesQueueToSend.remove(message);
		}


		protected void setDatabasePackageNames(Set<String> packages) {
			if (packages == null || packages.size() == 0) {
				databasePackageNames = null;
			}
			else
				databasePackageNames=new HashSet<>(packages);

		}

		Set<String> getDatabasePackageNames() {
			return databasePackageNames;
		}

		boolean isConcernedByDatabasePackage(String packageName) {
			return databasePackageNames!=null && databasePackageNames.contains(packageName);
		}



		protected boolean removePackageDatabase(Set<String> packages) {
			if (databasePackageNames!=null && databasePackageNames.removeAll(packages))
			{
				if (this.databasePackageNames.size()==0)
					this.databasePackageNames=null;
				return true;
			}
			else
				return false;

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



	Record authenticatedMessageSent(AuthenticatedP2PMessage message) throws DatabaseException {
		return getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Record>() {
			@Override
			public Record run() throws Exception {
				List<Record> l = getRecordsWithAllFields("hostID", message.getHostDestination());
				if (l.size() > 1)
					throw new InternalError();
				else if (l.size() == 1) {
					Record r = l.get(0);
					if (r.removeAuthenticatedMessage(message))
						updateRecord(r, "authenticatedMessagesQueueToSend", r.authenticatedMessagesQueueToSend);
					updateLocalDatabaseHostIfNecessary(r);
					return r;
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

	void actualizeLastTransactionID(final List<DecentralizedValue> excludedHooks) throws DatabaseException {
		actualizeLastTransactionID(excludedHooks, getIDTable().getLastTransactionID());
	}

	void actualizeLastTransactionID(final List<DecentralizedValue> excludedHooks, final long lastTransactionID)
			throws DatabaseException {

		final ArrayList<DatabaseHooksTable.Record> toUpdate = new ArrayList<>();
		getRecords(new Filter<Record>() {

			@Override
			public boolean nextRecord(final com.distrimind.ood.database.DatabaseHooksTable.Record h)
					throws DatabaseException {
				if (!excludedHooks.contains(h.getHostID())) {
					final AtomicLong actualLastID = new AtomicLong(Long.MAX_VALUE);
					getDatabaseTransactionsPerHostTable()
							.getRecords(new Filter<DatabaseTransactionsPerHostTable.Record>() {

								@Override
								public boolean nextRecord(DatabaseTransactionsPerHostTable.Record _record) {

									if (_record.getTransaction().getID() - 1 < actualLastID.get())
										actualLastID.set(_record.getTransaction().getID() - 1);
									if (actualLastID.get() == h.getLastValidatedLocalTransactionID())
										this.stopTableParsing();
									return false;
								}

							}, "hook=%hook", "hook", h);
					if (actualLastID.get() > h.getLastValidatedLocalTransactionID()) {
						getDatabaseDistantTransactionEvent()
								.getRecords(new Filter<com.distrimind.ood.database.DatabaseDistantTransactionEvent.Record>() {

												@Override
												public boolean nextRecord(
														com.distrimind.ood.database.DatabaseDistantTransactionEvent.Record _record)
														throws SerializationDatabaseException {
													if (_record.isConcernedBy(h.getHostID())) {
														if (_record.getLocalID() - 1 < actualLastID.get())
															actualLastID.set(_record.getLocalID() - 1);
														if (actualLastID.get() == h.getLastValidatedLocalTransactionID())
															this.stopTableParsing();
													}
													return false;
												}
											}, "localID<=%maxLocalID AND localID>%minLocalID and peersInformedFull=%peersInformedFull",
										"maxLocalID", actualLastID.get(), "minLocalID",
										h.getLastValidatedLocalTransactionID(), "peersInformedFull",
										Boolean.FALSE);
					}

					if (actualLastID.get() == Long.MAX_VALUE && h.getLastValidatedLocalTransactionID() < 0)
						actualLastID.set(lastTransactionID);
					else if (actualLastID.get() < h.getLastValidatedLocalTransactionID())
						throw new IllegalAccessError();

					if (actualLastID.get() != Long.MAX_VALUE && h.getLastValidatedLocalTransactionID() < actualLastID.get()) {
						h.setLastValidatedLocalTransactionID(actualLastID.get());
						toUpdate.add(h);
					}

				}
				return false;
			}
		}, "concernsDatabaseHost=%c", "c", false);
		for (DatabaseHooksTable.Record h : toUpdate) {
			updateRecord(h, "lastValidatedLocalTransactionID", h.getLastValidatedLocalTransactionID());
		}
	}
	Record getHook(DecentralizedValue host) throws DatabaseException {
		return getHook(host, false);
	}
	Record getHook(DecentralizedValue host, boolean nullAccepted) throws DatabaseException {
		if (host == null)
			throw new NullPointerException("host");
		List<Record> l = getRecordsWithAllFields("hostID", host);
		if (l.size() == 0) {
			if (nullAccepted)
				return null;
			throw new DatabaseException("Unknown host " + host);
		}
		if (l.size() > 1)
			throw new IllegalAccessError();
		return l.iterator().next();
	}

	Map<DecentralizedValue, Long> getLastValidatedLocalTransactionIDs() throws DatabaseException {
		return getDatabaseWrapper()
				.runSynchronizedTransaction(new SynchronizedTransaction<Map<DecentralizedValue, Long>>() {

					@Override
					public Map<DecentralizedValue, Long> run() throws Exception {
						final Map<DecentralizedValue, Long> res = new HashMap<>();

						getRecords(new Filter<Record>() {

							@Override
							public boolean nextRecord(Record _record) {
								if (!_record.concernsLocalDatabaseHost())
									res.put(_record.getHostID(), _record.getLastValidatedLocalTransactionID());
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

	Record initLocalHook(DecentralizedValue hostID) throws DatabaseException {
		if (getLocalDatabaseHost()!=null)
			throw new DatabaseException("Local database host already set !");
		DatabaseHooksTable.Record r=initHook(hostID, true);
		localHost = null;
		supportedDatabasePackages = null;
		return r;
	}
	private Record initHook(DecentralizedValue hostID, boolean local) throws DatabaseException {
		if (local && getLocalDatabaseHost()!=null)
			throw new DatabaseException("Local database host already set !");
		DatabaseHooksTable.Record r = new DatabaseHooksTable.Record();
		r.setHostID(hostID);
		r.setPairingState(PairingState.PAIRED);
		r.setConcernsDatabaseHost(local);
		r.setLastValidatedDistantTransactionID(-1);
		r.setLastValidatedLocalTransactionID(-1);
		r = addRecord(r);
		updateLocalDatabaseHostIfNecessary(r);
		return r;
	}
	Record initDistantHook(DecentralizedValue hostID) throws DatabaseException {
		return initHook(hostID, false);
	}
	void addHooks(final Map<String, Boolean> packages,
									   final Set<DecentralizedValue> peersInCloud, boolean fromDistantMessage)
			throws DatabaseException {

		if (peersInCloud == null)
			throw new NullPointerException("hostID");



		getDatabaseWrapper()
				.runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

					@Override
					public Void run() throws Exception {
						Set<DecentralizedValue> peerInCloudRemaining=new HashSet<>(peersInCloud);
						if (getLocalDatabaseHost()==null)
							throw new DatabaseException("Local database host not set");
						List<Record> records=getRecords(new Filter<Record>() {
							@Override
							public boolean nextRecord(Record r) {
								return peerInCloudRemaining.remove(r.getHostID()) || r.concernsDatabaseHost;
							}
						});
						Map<DecentralizedValue, Set<String>> synchronizedPackages=new HashMap<>();
						Map<String, Set<DecentralizedValue>> synchronizedHosts=new HashMap<>();
						for (Record r : records)
						{
							Set<String> dpn=r.getDatabasePackageNames();
							Set<String> nap=new HashSet<>(packages.keySet());
							if (dpn!=null) {
								dpn.forEach(v-> synchronizedHosts.compute(v, (s, h) -> {
									if (h==null)
										h=new HashSet<>();
									h.add(r.getHostID());
									return h;
								}));
								nap.removeAll(dpn);
							}
							if (nap.size()>0) {
								r.setDatabasePackageNames(packages.keySet());
								updateRecord(r, "databasePackageNames", r.databasePackageNames);
								if (!r.concernsDatabaseHost) {
									synchronizedPackages.put(r.getHostID(), nap);
								}
							}

							updateLocalDatabaseHostIfNecessary(r);

						}
						for (DecentralizedValue hostID : peerInCloudRemaining)
						{
							DatabaseHooksTable.Record r = new DatabaseHooksTable.Record();
							r.setHostID(hostID);
							r.setPairingState(PairingState.PAIRED);//TODO change state
							r.setDatabasePackageNames(packages.keySet());
							r.setConcernsDatabaseHost(false);
							r.setLastValidatedDistantTransactionID(-1);
							r.setLastValidatedLocalTransactionID(-1);
							r = addRecord(r);
							if (packages.keySet().size()>0)
								synchronizedPackages.put(r.getHostID(), packages.keySet());

						}
						for (Record r : getRecords())
						{
							if (!r.concernsDatabaseHost) {
								Set<String> pkgs=synchronizedPackages.get(r.getHostID());
								if (pkgs!=null && pkgs.size()>0) {
									HashMap<String, Boolean> newAddedPackages = new HashMap<>();
									pkgs.forEach(v -> newAddedPackages.put(v, packages.get(v)));
									long id=getDatabaseTransactionEventsTable()
											.addTransactionToSynchronizeTables(newAddedPackages, synchronizedHosts, r);
									if (id==-1)
										updateRecord(r, "lastValidatedLocalTransactionID", -1L);

								}
							}
						}

						supportedDatabasePackages = null;

						if (fromDistantMessage) {
							DatabaseConfigurationsBuilder builder = getDatabaseWrapper().getDatabaseConfigurationsBuilder();

							for (Map.Entry<DecentralizedValue, Set<String>> e : synchronizedPackages.entrySet()) {
								builder.synchronizeDistantPeersWithGivenAdditionalPackages(false, Collections.singletonList(e.getKey()), e.getValue().toArray(new String[0]));
							}
							if (synchronizedPackages.size()>0) {
								builder.commit();
								if (getDatabaseWrapper().getSynchronizer().isInitializedWithCentralBackup())
									getDatabaseWrapper().getSynchronizer().updateConnectionWithDistantBackupCenter(synchronizedPackages.keySet());
							}
						}



						localHost = null;
						supportedDatabasePackages = null;
						getDatabaseWrapper().getSynchronizer().notifyNewTransactionsIfNecessary();


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
				});

	}

	DatabaseHooksTable.Record removeHook(boolean propagate, final DecentralizedValue hostID) throws DatabaseException {
		DatabaseHooksTable.Record r=getLocalDatabaseHost();

		if (hostID.equals(r.getHostID()))
		{
			if (r.getDatabasePackageNames()!=null && r.getDatabasePackageNames().size()>0)
				desynchronizeDatabases(hostID, false, r.getDatabasePackageNames(), false);

			for (DatabaseHooksTable.Record r2 : getRecords())
			{
				if (!r2.concernsLocalDatabaseHost())
					desynchronizeDatabases(r2.getHostID(), true, new HashSet<>(), false);
			}
		}
		else
			r= desynchronizeDatabases(hostID, true, new HashSet<>(), false);
		if (!getDatabaseWrapper().getDatabaseConfigurationsBuilder().isCommitInProgress())
			getDatabaseWrapper().getDatabaseConfigurationsBuilder().removeDistantPeers(propagate, Collections.singleton(hostID))
				.commit();
		if (getDatabaseWrapper().getSynchronizer().isInitialized(hostID) || getDatabaseWrapper().getSynchronizer().isInitializedWithCentralBackup(hostID))
			getDatabaseWrapper().getSynchronizer().disconnectHook(hostID);
		getDatabaseWrapper().getSynchronizer().sendAvailableDatabaseToCentralDatabaseBackup();
		return r;
	}

	void desynchronizeDatabases(Set<String> packages, Set<DecentralizedValue> concernedHosts) throws DatabaseException {
		if (packages==null)
			throw new NullPointerException();
		if (concernedHosts==null)
			throw new NullPointerException();
		if (packages.size()==0)
			throw new IllegalArgumentException();

		getDatabaseWrapper()
				.runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

					@Override
					public Void run() throws Exception {
						for (DecentralizedValue host : concernedHosts) {
							desynchronizeDatabases(host, false, packages, false);
						}
						if (concernedHosts.size()>0 && packages.size()>0 && !getDatabaseWrapper().getDatabaseConfigurationsBuilder().isCommitInProgress()) {
							Set<String> ps=new HashSet<>();
							packages.forEach(p -> {
								if (getDatabaseWrapper().getDatabaseConfigurationsBuilder().getConfigurations().getDatabaseConfigurations().stream().anyMatch(dc -> dc.getDatabaseSchema().getPackage().getName().equals(p)))
									ps.add(p);
							});
							if (ps.size()>0)
								getDatabaseWrapper().getDatabaseConfigurationsBuilder().desynchronizeDistantPeersWithGivenAdditionalPackages(false, concernedHosts, ps.toArray(new String[0]))
									.commit();
						}

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

				});
	}
	void desynchronizeDatabases(final DecentralizedValue hostID) throws DatabaseException {
		desynchronizeDatabases(hostID, false, null, true);
	}
	private DatabaseHooksTable.Record desynchronizeDatabases(final DecentralizedValue hostID, final boolean removeHook, Set<String> packages, boolean removeAllDependingTransactions)
			throws DatabaseException {
		if (hostID == null)
			throw new NullPointerException("hostID");
		return getDatabaseWrapper()
				.runSynchronizedTransaction(new SynchronizedTransaction<Record>() {

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
							if (localHost != null && localHost.getHostID().equals(hostID)) {
								localHost = null;
							}
							supportedDatabasePackages = null;
							lastTransactionFieldsBetweenDistantHosts.entrySet().removeIf(e -> e.getKey().getHostServer().equals(hostID)
									|| e.getKey().getHostToSynchronize().equals(hostID));
							if (removeAllDependingTransactions) {
								r.setDatabasePackageNames(null);
							} else
								r.removePackageDatabase(packages);
							if (removeHook) {
								r.setRemoved();
								removeRecordWithCascade(r);
								getDatabaseTransactionEventsTable().removeTransactionsFromLastID();
								return null;
							} else {
								updateRecord(r);
								if (removeAllDependingTransactions) {
									getDatabaseTransactionsPerHostTable().removeTransactions(r);
								} else
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
				getRecords(new Filter<Record>() {

					@Override
					public boolean nextRecord(DatabaseHooksTable.Record _record) {

						Set<String> ps = _record.getDatabasePackageNames();
						if (ps == null)
							databasePackages.add(Object.class.getPackage().getName());
						else {
							databasePackages.addAll(ps);
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

		getRecords(new Filter<Record>() {

			@Override
			public boolean nextRecord(DatabaseHooksTable.Record _record) {
				if (!_record.concernsLocalDatabaseHost()) {
					if (min.get() > _record.getLastValidatedLocalTransactionID()) {
						min.set(_record.getLastValidatedLocalTransactionID());
					}
				}
				return false;
			}
		});
		if (min.get() == Long.MAX_VALUE)
			min.set(-1);
		return min.get();
	}



	DatabaseHooksTable.Record getLocalDatabaseHost() throws DatabaseException {
		if (localHost == null) {
			final Reference<DatabaseHooksTable.Record> res = new Reference<>();
			getRecords(new Filter<Record>() {

				@Override
				public boolean nextRecord(DatabaseHooksTable.Record _record) {
					res.set(_record);
					stopTableParsing();
					return false;
				}
			}, "concernsDatabaseHost=%b", "b", true);
			localHost = res.get();
		}
		return localHost;
	}
	private void updateLocalDatabaseHostIfNecessary(DatabaseHooksTable.Record r)
	{
		if (r.concernsDatabaseHost)
			localHost=r;
	}

	Collection<DatabaseHooksTable.Record> resetAllHosts() throws DatabaseException {
		Collection<DatabaseHooksTable.Record> res=getRecords();
		removeAllRecordsWithCascade();
		localHost=null;
		supportedDatabasePackages=null;
		lastTransactionFieldsBetweenDistantHosts.clear();
		return res;
	}

	boolean validateLastDistantTransactionIDAndLastTransactionUTC(Record hook, long distantTransactionID, Long timeUTC) throws DatabaseException {

		HashMap<String, Object> hm = new HashMap<>();
		if (hook.getLastValidatedDistantTransactionID() < distantTransactionID) {
			hm.put("lastValidatedDistantTransactionID", distantTransactionID);
		}
		if (hook.getLastValidatedDistantTransactionUTCMs()<timeUTC)
			hm.put("lastValidatedDistantTransactionUTCMs", timeUTC);
		if (hm.size()>0)
		{
			updateRecord(hook, hm);
			return true;
		}
		else
			return false;
	}

	void setDatabasePackageNamesThatUseBackup(DecentralizedValue hostSource, Set<String> databasePackageNamesThatUseBackup) throws DatabaseException {
		if (hostSource==null)
			throw new NullPointerException();
		updateRecords(new AlterRecordFilter<Record>() {
			@Override
			public void nextRecord(Record _record) throws DatabaseException {
				Set<String> hs = new HashSet<>(databasePackageNamesThatUseBackup);
				if (_record.databasePackageNames != null)
					hs.removeAll(_record.databasePackageNames);
				if (!hs.equals(_record.databasePackageNamesThatDoNotUseExternalBackup))
					update("databasePackageNamesThatDoNotUseExternalBackup", hs);
			}
		}, "hostID=%hid", "hid", hostSource);
	}
	void offerNewAuthenticatedMessageDestinedToCentralDatabaseBackup(AuthenticatedMessageDestinedToCentralDatabaseBackup message, AbstractSecureRandom random, EncryptionProfileProvider encryptionProfileProvider) throws DatabaseException {
		offerNewAuthenticatedMessageDestinedToCentralDatabaseBackup(getLocalDatabaseHost(), message, random, encryptionProfileProvider);
	}
	void offerNewAuthenticatedMessageDestinedToCentralDatabaseBackup(DatabaseHooksTable.Record record, AuthenticatedMessageDestinedToCentralDatabaseBackup message, AbstractSecureRandom random, EncryptionProfileProvider encryptionProfileProvider) throws DatabaseException {
		if (message==null)
			throw new NullPointerException();
		if (!record.concernsLocalDatabaseHost())
			throw new IllegalAccessError();
		if (record.authenticatedMessagesQueueDestinedToCentralDatabaseBackup==null)
			record.authenticatedMessagesQueueDestinedToCentralDatabaseBackup=new LinkedList<>();

		message.generateAndSetSignatures(random, encryptionProfileProvider);
		record.authenticatedMessagesQueueDestinedToCentralDatabaseBackup.addLast(message);
		updateLocalDatabaseHostIfNecessary(record);
		updateRecord(record, "authenticatedMessagesQueueDestinedToCentralDatabaseBackup", record.authenticatedMessagesQueueDestinedToCentralDatabaseBackup);
		getDatabaseWrapper().getSynchronizer().notifyNewAuthenticatedMessage(message);
	}
	void messageDestinedToCentralDatabaseBackupSent(AuthenticatedMessageDestinedToCentralDatabaseBackup message) throws DatabaseException {
		Record r=getLocalDatabaseHost();
		if (r.authenticatedMessagesQueueDestinedToCentralDatabaseBackup!=null && r.authenticatedMessagesQueueDestinedToCentralDatabaseBackup.remove(message))
		{
			updateRecord(r, "authenticatedMessagesQueueDestinedToCentralDatabaseBackup", r.authenticatedMessagesQueueDestinedToCentralDatabaseBackup);
		}
	}
}
