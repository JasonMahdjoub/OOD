
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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.distrimind.ood.database.DatabaseHooksTable.Record;
import com.distrimind.ood.database.Table.ColumnsReadQuerry;
import com.distrimind.ood.database.Table.DefaultConstructorAccessPrivilegedAction;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.fieldaccessors.ByteTabObjectConverter;
import com.distrimind.ood.database.fieldaccessors.DefaultByteTabObjectConverter;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.SecureRandomType;

/**
 * This class represent a SqlJet database.
 * 
 * @author Jason Mahdjoub
 * @version 1.6
 * @since OOD 1.4
 */
public abstract class DatabaseWrapper implements AutoCloseable {

	private static List<Class<?>> internalDatabaseClassesList = Arrays.<Class<?>>asList(DatabaseDistantTransactionEvent.class, DatabaseDistantEventsTable.class, 
			DatabaseEventsTable.class, DatabaseHooksTable.class, DatabaseTransactionEventsTable.class, DatabaseTransactionsPerHostTable.class, IDTable.class);
	
	
	// protected Connection sql_connection;
	private volatile boolean closed = false;
	protected final String database_name;
	private static final HashMap<String, Lock> lockers = new HashMap<String, Lock>();
	private static final HashMap<String, Integer> number_of_shared_lockers = new HashMap<String, Integer>();
	final static String ROW_COUNT_TABLES = "ROW_COUNT_TABLES__";
	// private final HashMap<Class<? extends Table<?>>, Table<?>>
	// tables_instances=new HashMap<>();
	private final ArrayList<ByteTabObjectConverter> converters;

	volatile HashMap<Package, Database> sql_database = new HashMap<Package, Database>();
	// private final DatabaseMetaData dmd;
	private volatile DatabaseTransactionEventsTable transactionTable = null;
	private volatile DatabaseHooksTable databaseHooksTable = null;
	private volatile DatabaseTransactionsPerHostTable databaseTransactionsPerHostTable = null;
	private volatile DatabaseEventsTable databaseEventsTable = null;
	private volatile DatabaseDistantTransactionEvent databaseDistantTransactionEvent = null;
	private volatile DatabaseTransactionEventsTable databaseTransactionEventsTable;
	private volatile IDTable transactionIDTable = null;

	private final DatabaseSynchronizer synchronizer;
	protected volatile int maxTransactionsToSynchronizeAtTheSameTime = 1000;
	volatile int maxTransactionsEventsKeepedIntoMemory = 100;
	protected Database actualDatabaseLoading = null;
	volatile int maxTransactionEventsKeepedIntoMemoryDuringImportInBytes=10000000;
	private volatile boolean hasOnePeerSyncronized=false;
	private volatile AbstractSecureRandom randomForKeys;

	/**
	 * Gets the max number of events sent to a distant peer at the same time
	 * 
	 * @return the max number of events sent to a distant peer at the same time
	 */
	public int getMaxTransactionsToSynchronizeAtTheSameTime() {
		return maxTransactionsToSynchronizeAtTheSameTime;
	}

	/**
	 * Set the max number of events sent to a distant peer at the same time
	 * 
	 * @param maxTransactionsToSynchronizeAtTheSameTime
	 *            the max number of events
	 */
	public void setMaxTransactionsToSynchronizeAtTheSameTime(int maxTransactionsToSynchronizeAtTheSameTime) {
		this.maxTransactionsToSynchronizeAtTheSameTime = maxTransactionsToSynchronizeAtTheSameTime;
	}

	// private final boolean isWindows;
	private static class Database {
		final HashMap<Class<? extends Table<?>>, Table<?>> tables_instances = new HashMap<Class<? extends Table<?>>, Table<?>>();

		private final DatabaseConfiguration configuration;

		public Database(DatabaseConfiguration configuration) {
			if (configuration == null)
				throw new NullPointerException("configuration");
			this.configuration = configuration;
		}

		DatabaseConfiguration getConfiguration() {
			return configuration;
		}

	}

	/**
	 * Set the maximum number of events keeped into memory during the current
	 * transaction. If this maximum is reached, the transaction is stored into the
	 * disk
	 * 
	 * @param v
	 *            the maximum number of events keeped into memory (minimum=1)
	 * 
	 * 
	 */
	public void setMaxTransactionEventsKeepedIntoMemory(int v) {
		if (v < 1)
			throw new IllegalArgumentException("v must be greater or equal than 1");
		this.maxTransactionsEventsKeepedIntoMemory = v;
	}

	/**
	 * Gets the maximum number of events keeped into memory during the current
	 * transaction. If this maximum is reached, the transaction is stored into the
	 * disk
	 * 
	 * @return the maximum number of events keeped into memory during the current
	 *         transaction.
	 */
	public int getMaxTransactionEventsKeepedIntoMemory() {
		return this.maxTransactionsEventsKeepedIntoMemory;
	}

	public int getMaxTransactionEventsKeepedIntoMemoryDuringImportInBytes()
	{
		return maxTransactionEventsKeepedIntoMemoryDuringImportInBytes;
	}
	
	public void setMaxTransactionEventsKeepedIntoMemoryDuringImportInBytes(int maxTransactionEventsKeepedIntoMemoryDuringImportInBytes)
	{
		this.maxTransactionEventsKeepedIntoMemoryDuringImportInBytes=maxTransactionEventsKeepedIntoMemoryDuringImportInBytes;
	}
	public void addByteTabObjectConverter(ByteTabObjectConverter converter) {
		converters.add(converter);
	}

	public ArrayList<ByteTabObjectConverter> getByteTabObjectConverters() {
		return converters;
	}

	public ByteTabObjectConverter getByteTabObjectConverter(Class<?> object_type) {
		for (int i = converters.size() - 1; i >= 0; i--) {
			ByteTabObjectConverter btoc = converters.get(i);
			if (btoc.isCompatible(object_type))
				return btoc;
		}
		return null;
	}

	/**
	 * Constructor
	 * 
	 * @param _database_name
	 *            the database name
	 * @throws DatabaseException if a problem occurs
	 * 
	 */
	protected DatabaseWrapper(String _database_name) throws DatabaseException {
		if (_database_name == null)
			throw new NullPointerException("_database_name");
		try
		{
			this.randomForKeys=SecureRandomType.BC_FIPS_APPROVED_FOR_KEYS.getSingleton(null);
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
		/*
		 * if (_sql_connection==null) throw new NullPointerException("_sql_connection");
		 */
		database_name = _database_name;
		// sql_connection=_sql_connection;

		// isWindows=OSValidator.isWindows();

		locker = getLocker();
		converters = new ArrayList<>();
		converters.add(new DefaultByteTabObjectConverter());
		synchronizer=new DatabaseSynchronizer();
	}

	public AbstractSecureRandom getSecureRandomForKeys()
	{
		return randomForKeys;
	}
	
	public void setSecureRandomForKeys(AbstractSecureRandom randomForKeys)
	{
		if (randomForKeys==null)
			throw new NullPointerException();
		this.randomForKeys=randomForKeys;
	}
	
	public boolean isReadOnly() throws DatabaseException {
		return ((Boolean) runTransaction(new Transaction() {

			@Override
			public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
				try {
					Session sql_connection = getConnectionAssociatedWithCurrentThread();
					return new Boolean(sql_connection.getConnection().isReadOnly());
				} catch (SQLException e) {
					throw DatabaseException.getDatabaseException(e);
				}

			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_READ_UNCOMMITTED;
			}

			@Override
			public boolean doesWriteData() {
				return false;
			}

			@Override
			public void initOrReset() {
				
			}
		}, true)).booleanValue();

	}

	public int getNetworkTimeout() throws DatabaseException {
		return ((Integer) runTransaction(new Transaction() {

			@Override
			public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
				try {
					Session sql_connection = getConnectionAssociatedWithCurrentThread();
					return new Integer(sql_connection.getConnection().getNetworkTimeout());
				} catch (SQLException e) {
					throw DatabaseException.getDatabaseException(e);
				}

			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_READ_UNCOMMITTED;
			}

			@Override
			public boolean doesWriteData() {
				return false;
			}

			@Override
			public void initOrReset() {
				
			}
		}, true)).intValue();

	}

	/**
	 * Gets the interface that enable database synchronization between different
	 * peers
	 * 
	 * @return the interface that enable database synchronization between different
	 *         peers
	 */
	public DatabaseSynchronizer getSynchronizer() {
		return synchronizer;

	}

	static class ConnectedPeers {
		private DatabaseHooksTable.Record hook;
		private boolean transferInProgress = false;

		ConnectedPeers(Record _hook) {
			super();
			hook = _hook;
		}

		public boolean isTransferInProgress() {
			return transferInProgress;
		}

		public void setTransferInProgress(boolean _transferInProgress) {
			transferInProgress = _transferInProgress;
		}

		public AbstractDecentralizedID getHostID() {
			return hook.getHostID();
		}

		@Override
		public boolean equals(Object o) {
			if (o == null)
				return false;
			if (o instanceof ConnectedPeers) {
				return ((ConnectedPeers) o).hook.equals(this.hook);
			} else if (o instanceof DatabaseHooksTable.Record) {
				return this.hook.equals(o);
			}
			return false;
		}

		@Override
		public int hashCode() {
			return hook.hashCode();
		}

	}

	public class DatabaseSynchronizer {
		private DatabaseNotifier notifier = null;
		private boolean canNotify = true;
		private LinkedList<DatabaseEvent> events = new LinkedList<>();
		protected HashMap<AbstractDecentralizedID, ConnectedPeers> initializedHooks = new HashMap<>();
		private Condition newEventCondition=locker.newCondition(); 
		DatabaseSynchronizer() {
		}

		public boolean isInitialized() throws DatabaseException {
			return getHooksTransactionsTable().getLocalDatabaseHost() != null;
		}

		public boolean isInitialized(AbstractDecentralizedID hostID) {
			return initializedHooks.get(hostID) != null;
		}

		private void notifyNewEvent() {
			boolean notify = false;
			
			try {
				lockWrite();
				if (canNotify && notifier != null) {
					notify = true;
					canNotify = false;
				}
				newEventCondition.signalAll();
			}
			finally
			{
				unlockWrite();
			}
			if (notify)
				notifier.newDatabaseEventDetected(DatabaseWrapper.this);
		}

		public DatabaseEvent nextEvent() {
			
			try
			{
				lockWrite();
				if (events.isEmpty())
					return null;
				else {
					DatabaseEvent e = events.removeFirst();
					if (events.isEmpty())
						canNotify = true;
					return e;
				}
			}
			finally
			{
				unlockWrite();
			}
		}

		DatabaseNotifier getNotifier() {
			
			try {
				lockWrite();
				return notifier;
			}
			finally
			{
				unlockWrite();
			}
			
		}

		public DatabaseEvent waitNextEvent() throws InterruptedException {

			try {
				lockWrite();
				DatabaseEvent de = nextEvent();
				for (; de != null; de = nextEvent()) {
					newEventCondition.await();
				}
				return de;
			}
			finally
			{
				unlockWrite();
			}
			
		}

		public void setNotifier(DatabaseNotifier notifier) {
			
			try {
				lockWrite();
				this.notifier = notifier;
				this.canNotify = true;
			}
			finally
			{
				unlockWrite();
			}
			
		}

		@SuppressWarnings("unlikely-arg-type")
		public void initLocalHostID(AbstractDecentralizedID localHostID) throws DatabaseException {
			DatabaseHooksTable.Record local = getHooksTransactionsTable().getLocalDatabaseHost();
			if (local != null && !local.getHostID().equals(localHostID))
				throw new DatabaseException("The given local host id is different from the stored local host id !");
			if (local == null) {
				addHookForLocalDatabaseHost(localHostID);
			}
			getDatabaseTransactionEventsTable().cleanTmpTransactions();
			
			try {
				lockWrite();
				if (initializedHooks.containsValue(local)) {
					throw DatabaseException.getDatabaseException(
							new IllegalAccessException("Local hostID " + localHostID + " already initialized !"));
				}
				initializedHooks.put(localHostID, new ConnectedPeers(local));
				canNotify = true;
			}
			finally
			{
				unlockWrite();
			}
			
		}

		public void addHookForLocalDatabaseHost(AbstractDecentralizedID hostID, Package... databasePackages)
				throws DatabaseException {
			ArrayList<String> packages = new ArrayList<>();
			for (Package p : databasePackages) {
				packages.add(p.getName());
			}
			getHooksTransactionsTable().addHooks(hostID, true, false, new ArrayList<AbstractDecentralizedID>(),
					packages);
		}

		public HookAddRequest askForHookAddingAndSynchronizeDatabase(AbstractDecentralizedID hostID,
				boolean replaceDistantConflitualRecords, Package... packages) throws DatabaseException {
			ArrayList<String> packagesString = new ArrayList<>();
			for (Package p : packages)
				packagesString.add(p.getName());
			return askForHookAddingAndSynchronizeDatabase(hostID, true, replaceDistantConflitualRecords,
					packagesString);
		}

		private HookAddRequest askForHookAddingAndSynchronizeDatabase(AbstractDecentralizedID hostID,
				boolean mustReturnMessage, boolean replaceDistantConflitualRecords, ArrayList<String> packages)
				throws DatabaseException {
			final ArrayList<AbstractDecentralizedID> hostAlreadySynchronized = new ArrayList<>();
			runTransaction(new Transaction() {
				
				@Override
				public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
					getHooksTransactionsTable().getRecords(new Filter<DatabaseHooksTable.Record>() {

						@Override
						public boolean nextRecord(Record _record) {
							if (!_record.concernsLocalDatabaseHost())
								hostAlreadySynchronized.add(_record.getHostID());
							return false;
						}
					});
					return null;
				}
				
				@Override
				public void initOrReset() throws DatabaseException {
					
					hostAlreadySynchronized.clear();
				}
				
				@Override
				public TransactionIsolation getTransactionIsolation() {
					return TransactionIsolation.TRANSACTION_READ_COMMITTED;
				}
				
				@Override
				public boolean doesWriteData() {
					return false;
				}
			}, true);

			return new HookAddRequest(getHooksTransactionsTable().getLocalDatabaseHost().getHostID(), hostID, packages,
					hostAlreadySynchronized, mustReturnMessage, replaceDistantConflitualRecords);
		}

		public HookAddRequest receivedHookAddRequest(HookAddRequest hookAddRequest) throws DatabaseException {
			DatabaseHooksTable.Record localDatabaseHost = getHooksTransactionsTable().getLocalDatabaseHost();
			if (localDatabaseHost == null)
				throw new DatabaseException("Function must be called before addHookForLocalDatabaseHost.");
			if (hookAddRequest == null || !hookAddRequest.getHostDestination().equals(localDatabaseHost.getHostID()))
				return null;
			HookAddRequest res = null;
			if (hookAddRequest.mustReturnsMessage()) {
				res = askForHookAddingAndSynchronizeDatabase(hookAddRequest.getHostSource(), false,
						!hookAddRequest.isReplaceDistantConflictualData(), hookAddRequest.getPackagesToSynchronize());
			}
			getHooksTransactionsTable().addHooks(hookAddRequest.getHostSource(), false,
					!hookAddRequest.isReplaceDistantConflictualData(), hookAddRequest.getHostsAlreadySynchronized(),
					hookAddRequest.getPackagesToSynchronize());
			isReliedToDistantHook();
			return res;

		}
		/*
		 * private void addHookForDistantHost(AbstractDecentralizedID hostID, boolean
		 * replaceDistantConflitualRecords, List<Package>
		 * packagesAlreadySynchronizedOneTime, Package ...databasePackages) throws
		 * DatabaseException { getHooksTransactionsTable().addHooks(hostID, false,
		 * replaceDistantConflitualRecords, packagesAlreadySynchronizedOneTime,
		 * databasePackages); }
		 */

		public void removeHook(AbstractDecentralizedID hostID, Package... databasePackages) throws DatabaseException {
			getHooksTransactionsTable().removeHooks(hostID, databasePackages);
			isReliedToDistantHook();
		}
		
		protected boolean isReliedToDistantHook() throws DatabaseException
		{
			return hasOnePeerSyncronized=getHooksTransactionsTable().hasRecordsWithAllFields("concernsDatabaseHost", new Boolean(false));
		}
		

		public long getLastValidatedSynchronization(AbstractDecentralizedID hostID) throws DatabaseException {
			/*
			 * ConnectedPeers peer=null; synchronized(this) {
			 * peer=initializedHooks.get(hostID); } DatabaseHooksTable.Record r=null; if
			 * (peer==null) {
			 */
			DatabaseHooksTable.Record r = null;
			List<DatabaseHooksTable.Record> l = getHooksTransactionsTable()
					.getRecordsWithAllFields(new Object[] { "hostID", hostID });
			if (l.size() == 1)
				r = l.iterator().next();
			else if (l.size() > 1)
				throw new IllegalAccessError();
			if (r == null)
				throw DatabaseException.getDatabaseException(
						new IllegalArgumentException("The host ID " + hostID + " has not been initialized !"));
			/*
			 * } else r=peer.getHook();
			 */

			return r.getLastValidatedDistantTransaction();

		}

		void notifyNewTransactionsIfNecessary() throws DatabaseException {
			final long lastID = getTransactionIDTable().getLastTransactionID();
			
			try {
				lockWrite();

				getHooksTransactionsTable().getRecords(new Filter<DatabaseHooksTable.Record>() {

					@Override
					public boolean nextRecord(Record _record) throws DatabaseException {
						ConnectedPeers cp = initializedHooks.get(_record.getHostID());
						if (cp != null && !_record.concernsLocalDatabaseHost() && !cp.isTransferInProgress()) {
							if (lastID > _record.getLastValidatedTransaction()) {
								cp.setTransferInProgress(true);
								addNewDatabaseEvent(new DatabaseEventsToSynchronize(
										getHooksTransactionsTable().getLocalDatabaseHost().getHostID(), _record, lastID,
										maxTransactionsToSynchronizeAtTheSameTime));
							}
						}
						return false;
					}

				});
			}
			finally
			{
				unlockWrite();
			}
			
		}

		void validateLastSynchronization(AbstractDecentralizedID hostID, long lastTransferedTransactionID)
				throws DatabaseException {
			if (hostID == null)
				throw new NullPointerException("hostID");
			if (!isInitialized())
				throw new DatabaseException("The Synchronizer must be initialized (initLocalHostID function) !");
			ConnectedPeers peer = null;
			
			try {
				lockWrite();
				peer = initializedHooks.get(hostID);
			}
			finally
			{
				unlockWrite();
			}
			
			DatabaseHooksTable.Record r = null;
			if (peer != null)
				r = getHooksTransactionsTable().getHook(hostID);

			if (r == null)
				throw DatabaseException.getDatabaseException(
						new IllegalArgumentException("The host ID " + hostID + " has not been initialized !"));
			if (r.getLastValidatedTransaction() > lastTransferedTransactionID)
				throw new DatabaseException("The given transfer ID limit " + lastTransferedTransactionID
						+ " is lower than the stored transfer ID limit " + r.getLastValidatedTransaction());
			if (r.concernsLocalDatabaseHost())
				throw new DatabaseException("The given host ID correspond to the local database host !");

			long l = getDatabaseTransactionsPerHostTable().validateTransactions(r, lastTransferedTransactionID);
			if (l < lastTransferedTransactionID)
				throw new IllegalAccessError();
			if (l != lastTransferedTransactionID)
				addNewDatabaseEvent(new LastIDCorrection(getHooksTransactionsTable().getLocalDatabaseHost().getHostID(),
						hostID, l));
			
			try {
				lockWrite();
				peer.setTransferInProgress(false);
			}
			finally
			{
				unlockWrite();
			}
			
			synchronizedDataIfNecessary(peer);
			synchronizeMetaData();
		}

		void sendLastValidatedIDIfConnected(DatabaseHooksTable.Record hook) throws DatabaseException {
			
			try {
				lockWrite();
				if (initializedHooks.containsKey(hook.getHostID()))
					addNewDatabaseEvent(new DatabaseWrapper.TransactionConfirmationEvents(
							getHooksTransactionsTable().getLocalDatabaseHost().getHostID(), hook.getHostID(),
							hook.getLastValidatedDistantTransaction()));
			}
			finally
			{
				unlockWrite();
			}
			

		}

		private void synchronizeMetaData() throws DatabaseException {
			Map<AbstractDecentralizedID, Long> lastIds = getHooksTransactionsTable()
					.getLastValidatedDistantTransactions();
			
			try {
				lockWrite();
				for (AbstractDecentralizedID host : lastIds.keySet()) {
					if (isInitialized(host)) {
						Map<AbstractDecentralizedID, Long> map = new HashMap<AbstractDecentralizedID, Long>();
						map.putAll(lastIds);
						// map.remove(hostID);
						map.remove(host);
						if (map.size() > 0) {
							addNewDatabaseEvent(new DatabaseTransactionsIdentifiersToSynchronize(
									getHooksTransactionsTable().getLocalDatabaseHost().getHostID(), host, map));
						}
					}
				}
			}
			finally
			{
				unlockWrite();
			}
			
		}

		private long synchronizedDataIfNecessary() throws DatabaseException {
			final long lastID = getTransactionIDTable().getLastTransactionID();
			
			try {
				lockWrite();
				getHooksTransactionsTable().getRecords(new Filter<DatabaseHooksTable.Record>() {

					@Override
					public boolean nextRecord(Record _record) throws DatabaseException {
						ConnectedPeers cp = initializedHooks.get(_record.getHostID());
						if (cp != null && !_record.concernsLocalDatabaseHost() && !cp.isTransferInProgress()) {
							if (lastID > _record.getLastValidatedTransaction()) {
								cp.setTransferInProgress(true);
								addNewDatabaseEvent(new DatabaseEventsToSynchronize(
										getHooksTransactionsTable().getLocalDatabaseHost().getHostID(), _record, lastID,
										maxTransactionsToSynchronizeAtTheSameTime));
							}

						}
						return false;
					}

				});

			}
			finally
			{
				unlockWrite();
			}
			
			return lastID;
		}

		private long synchronizedDataIfNecessary(ConnectedPeers peer) throws DatabaseException {
			if (peer.isTransferInProgress())
				return -1;
			long lastID = getTransactionIDTable().getLastTransactionID();
			
			try {
				lockWrite();
				DatabaseHooksTable.Record hook = getHooksTransactionsTable().getHook(peer.getHostID());
				if (lastID > hook.getLastValidatedTransaction() && !peer.isTransferInProgress()) {
					peer.setTransferInProgress(true);
					addNewDatabaseEvent(new DatabaseEventsToSynchronize(
							getHooksTransactionsTable().getLocalDatabaseHost().getHostID(), hook, lastID,
							maxTransactionsToSynchronizeAtTheSameTime));
				}
			}
			finally
			{
				unlockWrite();
			}
			
			return lastID;
		}

		void addNewDatabaseEvent(DatabaseEvent e) {
			if (e == null)
				throw new NullPointerException("e");

			
			try {
				lockWrite();
				boolean add = true;
				if (e.getClass() == DatabaseEventsToSynchronize.class) {
					DatabaseEventsToSynchronize dets = (DatabaseEventsToSynchronize) e;
					for (DatabaseEvent detmp : events) {
						if (detmp.getClass() == DatabaseEventsToSynchronize.class
								&& dets.tryToFusion((DatabaseEventsToSynchronize) detmp)) {
							add = false;
							break;
						}
					}
				}
				if (add) {
					events.add(e);
					notifyNewEvent();
				}
			}
			finally
			{
				unlockWrite();
			}
			
		}

		public void deconnectHook(final AbstractDecentralizedID hostID) throws DatabaseException {
			if (hostID == null)
				throw new NullPointerException("hostID");

			
			try {
				lockWrite();
				ConnectedPeers peer = initializedHooks.remove(hostID);
				DatabaseHooksTable.Record hook = null;
				if (peer != null)
					hook = getHooksTransactionsTable().getHook(peer.getHostID());
				if (hook == null)
					throw DatabaseException.getDatabaseException(
							new IllegalAccessException("hostID " + hostID + " has not be initialized !"));
				if (hook.concernsLocalDatabaseHost()) {
					initializedHooks.clear();
					this.events.clear();
				} else {
					for (Iterator<DatabaseEvent> it = events.iterator(); it.hasNext();) {
						DatabaseEvent de = it.next();
						if (de instanceof DatabaseEventToSend) {
							DatabaseEventToSend des = (DatabaseEventToSend) de;
							if (des.getHostDestination().equals(hostID))
								it.remove();
						}
					}
				}
			}
			finally
			{
				unlockWrite();
			}
			

		}

		@SuppressWarnings("unlikely-arg-type")
		public void initHook(final AbstractDecentralizedID hostID, final long lastValidatedTransacionID)
				throws DatabaseException {
			if (hostID == null)
				throw new NullPointerException("hostID");
			if (!isInitialized())
				throw new DatabaseException("The Synchronizer must be initialized (initLocalHostID function) !");

			
			try {
				lockWrite();
				if (initializedHooks.remove(hostID) != null)
					throw DatabaseException.getDatabaseException(
							new IllegalAccessException("hostID " + hostID + " already initialized !"));
			}
			finally
			{
				unlockWrite();
			}
			
			DatabaseHooksTable.Record r = runSynchronizedTransaction(
					new SynchronizedTransaction<DatabaseHooksTable.Record>() {

						@Override
						public DatabaseHooksTable.Record run() throws Exception {
							List<DatabaseHooksTable.Record> l = getHooksTransactionsTable()
									.getRecordsWithAllFields(new Object[] { "hostID", hostID });
							DatabaseHooksTable.Record r = null;
							if (l.size() == 1)
								r = l.iterator().next();
							else if (l.size() > 1)
								throw new IllegalAccessError();
							if (r == null)
								throw new NullPointerException("Unknow host " + hostID);

							return r;
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
			
			try {
				lockWrite();
				if (initializedHooks.containsValue(r)) {
					throw DatabaseException.getDatabaseException(
							new IllegalAccessException("hostID " + hostID + " already initialized !"));
				}
				initializedHooks.put(hostID, new ConnectedPeers(r));
				
				validateLastSynchronization(hostID,
						Math.max(r.getLastValidatedTransaction(), lastValidatedTransacionID));
				
			}
			finally
			{
				unlockWrite();
			}
			
		}

		public void received(DatabaseTransactionsIdentifiersToSynchronize d) throws DatabaseException {
			if (!isInitialized())
				throw new DatabaseException("The Synchronizer must be initialized (initLocalHostID function) !");

			getHooksTransactionsTable().validateDistantTransactions(d.getHostSource(),
					d.getLastDistantTransactionIdentifiers(), true);
		}

		public void received(BigDatabaseEventToSend data, InputStreamGetter inputStream) throws DatabaseException {
			data.inportFromInputStream(DatabaseWrapper.this, inputStream);
			synchronizedDataIfNecessary();
		}

		public void received(final LastIDCorrection idCorrection) throws DatabaseException {
			if (idCorrection == null)
				throw new NullPointerException();

			
			try {
				lockWrite();
				ConnectedPeers cp = null;
				cp = initializedHooks.get(idCorrection.getHostSource());
				if (cp == null)
					throw new DatabaseException("The host " + idCorrection.getHostSource() + " is not connected !");
			}
			finally
			{
				unlockWrite();
			}
			
			runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

				@Override
				public Void run() throws Exception {
					DatabaseHooksTable.Record hook = getHooksTransactionsTable().getHook(idCorrection.getHostSource());
					if (hook == null)
						throw new DatabaseException("Impossbile to find hook " + idCorrection.getHostSource());

					getHooksTransactionsTable().updateRecord(hook, "lastValidatedDistantTransaction",
							new Long(idCorrection.getLastValidatedTransaction()));
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

		public void received(DatabaseEventToSend data) throws DatabaseException {
			if (data instanceof DatabaseTransactionsIdentifiersToSynchronize)
				received((DatabaseTransactionsIdentifiersToSynchronize) data);
			else if (data instanceof TransactionConfirmationEvents) {
				if (isInitialized(data.getHostDestination()))
					validateLastSynchronization(data.getHostSource(),
							((TransactionConfirmationEvents) data).getLastValidatedTransaction());
				else
					initHook(data.getHostSource(),
							((TransactionConfirmationEvents) data).getLastValidatedTransaction());
			} else if (data instanceof HookAddRequest) {
				receivedHookAddRequest((HookAddRequest) data);
			} else if (data instanceof LastIDCorrection) {
				received((LastIDCorrection) data);
			}
		}

	}

	public static enum SynchonizationAnomalyType {
		RECORD_TO_REMOVE_NOT_FOUND, RECORD_TO_REMOVE_HAS_DEPENDENCIES, RECORD_TO_UPDATE_NOT_FOUND, RECORD_TO_UPDATE_HAS_INCOMPATIBLE_PRIMARY_KEYS, RECORD_TO_UPDATE_HAS_DEPENDENCIES_NOT_FOUND, RECORD_TO_ADD_ALREADY_PRESENT, RECORD_TO_ADD_HAS_INCOMPATIBLE_PRIMARY_KEYS, RECORD_TO_ADD_HAS_DEPENDENCIES_NOT_FOUND,
	}

	public static interface DatabaseNotifier {
		public void newDatabaseEventDetected(DatabaseWrapper wrapper);

		/**
		 * This function is call if an anomaly occurs the synchronization process.
		 * Anomalies can be produced when :
		 * <ul>
		 * <li>the record to remove was not found :
		 * {@link SynchonizationAnomalyType#RECORD_TO_REMOVE_NOT_FOUND}</li>
		 * <li>the record to update was not found :
		 * {@link SynchonizationAnomalyType#RECORD_TO_UPDATE_NOT_FOUND}</li>
		 * <li>the record to add already exists :
		 * {@link SynchonizationAnomalyType#RECORD_TO_ADD_ALREADY_PRESENT}</li>
		 * </ul>
		 * Anomalies differs with collision (see
		 * {@link #collisionDetected(AbstractDecentralizedID, AbstractDecentralizedID, DatabaseEventType, Table, HashMap, DatabaseRecord, DatabaseRecord)}).
		 * They should not occur and represents a synchronization failure. Whereas
		 * collisions are produced when users make modifications on the same data into
		 * several peers. These kind of conflict are considered as normal by the system.
		 * 
		 * @param distantPeerID
		 *            the concerned distant peer, that produced the data modification.
		 * @param intermediatePeerID
		 *            nearest intermediate peer that transfered the data (can be null).
		 *            This intermediate peer is not those who have generated conflict
		 *            modifications.
		 * @param type
		 *            anomaly type
		 * @param concernedTable
		 *            the concerned table
		 * @param primary_keys
		 *            the concerned field keys
		 * @param record
		 *            the transmitted record
		 */
		public void anomalyDetected(AbstractDecentralizedID distantPeerID, AbstractDecentralizedID intermediatePeerID,
				SynchonizationAnomalyType type, Table<?> concernedTable, Map<String, Object> primary_keys,
				DatabaseRecord record);

		/**
		 * This function is called when a direct collision is detected during the
		 * synchronization process, when receiving data from a distant peer.
		 * 
		 * @param distantPeerID
		 *            the concerned distant peer, that produced the data modification.
		 * @param intermediatePeerID
		 *            nearest intermediate peer that transfered the data (can be null).
		 *            This intermediate peer is not those who have generated conflict
		 *            modifications.
		 * @param type
		 *            the database event type
		 * @param concernedTable
		 *            the concerned table
		 * @param keys
		 *            the concerned field keys
		 * @param newValues
		 *            the new received values
		 * @param actualValues
		 *            the actual field values
		 * @return true if the new event can replace the actual value and false if the
		 *         actual value must replace the distant value.
		 * @throws DatabaseException
		 *             if a problem occurs
		 */
		public boolean collisionDetected(AbstractDecentralizedID distantPeerID,
				AbstractDecentralizedID intermediatePeerID, DatabaseEventType type, Table<?> concernedTable,
				HashMap<String, Object> keys, DatabaseRecord newValues, DatabaseRecord actualValues)
				throws DatabaseException;

	}

	public static class DatabaseTransactionsIdentifiersToSynchronize extends DatabaseEvent
			implements DatabaseEventToSend {
		/**
		 * 
		 */
		private static final long serialVersionUID = -925935481339233901L;

		protected final AbstractDecentralizedID hostIDSource, hostIDDestination;
		final Map<AbstractDecentralizedID, Long> lastTransactionFieldsBetweenDistantHosts;

		DatabaseTransactionsIdentifiersToSynchronize(AbstractDecentralizedID hostIDSource,
				AbstractDecentralizedID hostIDDestination,
				Map<AbstractDecentralizedID, Long> lastTransactionFieldsBetweenDistantHosts) {
			this.hostIDSource = hostIDSource;
			this.hostIDDestination = hostIDDestination;
			this.lastTransactionFieldsBetweenDistantHosts = lastTransactionFieldsBetweenDistantHosts;
		}

		@Override
		public AbstractDecentralizedID getHostDestination() {
			return hostIDDestination;
		}

		@Override
		public AbstractDecentralizedID getHostSource() {
			return hostIDSource;
		}

		public Map<AbstractDecentralizedID, Long> getLastDistantTransactionIdentifiers() {
			return lastTransactionFieldsBetweenDistantHosts;
		}

		public AbstractDecentralizedID getConcernedHost() {
			return getHostDestination();
		}

	}

	public static class TransactionConfirmationEvents extends DatabaseEvent implements DatabaseEventToSend {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2920925585457063201L;

		protected final AbstractDecentralizedID hostIDSource, hostIDDestination;
		protected final long lastValidatedTransaction;

		TransactionConfirmationEvents(AbstractDecentralizedID _hostIDSource, AbstractDecentralizedID _hostIDDestination,
				long _lastValidatedTransaction) {
			super();
			hostIDSource = _hostIDSource;
			hostIDDestination = _hostIDDestination;
			lastValidatedTransaction = _lastValidatedTransaction;
		}

		@Override
		public AbstractDecentralizedID getHostDestination() {
			return hostIDDestination;
		}

		@Override
		public AbstractDecentralizedID getHostSource() {
			return hostIDSource;
		}

		public long getLastValidatedTransaction() {
			return lastValidatedTransaction;
		}

	}

	public static class LastIDCorrection extends DatabaseEvent implements DatabaseEventToSend {
		/**
		 * 
		 */
		private static final long serialVersionUID = 240192427146753618L;
		protected final AbstractDecentralizedID hostIDSource, hostIDDestination;
		protected final long lastValidatedTransaction;

		LastIDCorrection(AbstractDecentralizedID _hostIDSource, AbstractDecentralizedID _hostIDDestination,
				long _lastValidatedTransaction) {
			super();
			hostIDSource = _hostIDSource;
			hostIDDestination = _hostIDDestination;
			lastValidatedTransaction = _lastValidatedTransaction;
		}

		@Override
		public AbstractDecentralizedID getHostDestination() {
			return hostIDDestination;
		}

		@Override
		public AbstractDecentralizedID getHostSource() {
			return hostIDSource;
		}

		public long getLastValidatedTransaction() {
			return lastValidatedTransaction;
		}
	}

	
	public static class DatabaseEventsToSynchronize extends DatabaseEvent implements BigDatabaseEventToSend {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7594047077302832978L;
		protected final transient DatabaseHooksTable.Record hook;
		protected final int hookID;
		protected final AbstractDecentralizedID hostIDSource, hostIDDestination;
		private long lastTransactionIDIncluded;
		final int maxEventsRecords;

		DatabaseEventsToSynchronize(AbstractDecentralizedID hostIDSource, DatabaseHooksTable.Record hook,
				long lastTransactionIDIncluded, int maxEventsRecords) {
			this.hook = hook;
			this.hookID = hook.getID();
			this.hostIDDestination = hook.getHostID();
			this.hostIDSource = hostIDSource;
			this.lastTransactionIDIncluded = lastTransactionIDIncluded;
			this.maxEventsRecords = maxEventsRecords;
		}

		@Override
		public AbstractDecentralizedID getHostDestination() {
			return hostIDDestination;
		}

		@Override
		public AbstractDecentralizedID getHostSource() {
			return hostIDSource;
		}

		@Override
		public void inportFromInputStream(DatabaseWrapper wrapper, final InputStreamGetter inputStream)
				throws DatabaseException {
			if (wrapper == null)
				throw new NullPointerException("wrapper");
			try
			{
				wrapper.getDatabaseTransactionsPerHostTable().alterDatabase(getHostSource(), inputStream.initOrResetInputStream());
			}
			catch(Exception e)
			{
				throw DatabaseException.getDatabaseException(e);
			}
			
		}

		@Override
		public boolean exportToOutputStream(final DatabaseWrapper wrapper, final OutputStreamGetter outputStreamGetter)
				throws DatabaseException {
			if (wrapper == null)
				throw new NullPointerException("wrapper");
			if (maxEventsRecords == 0)
				return false;
			return ((Boolean) wrapper.runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {

				@Override
				public Boolean run() throws Exception {
					int number = wrapper.getDatabaseTransactionsPerHostTable().exportTransactions(outputStreamGetter.initOrResetOutputStream(), hookID,
							maxEventsRecords);
					return new Boolean(number > 0);
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
			})).booleanValue();

		}

		public boolean tryToFusion(DatabaseEventsToSynchronize dest) {
			if (hookID == dest.hookID) {
				lastTransactionIDIncluded = Math.max(lastTransactionIDIncluded, dest.lastTransactionIDIncluded);
				return true;
			} else
				return false;
		}

	}

	private Lock getLocker() {
		synchronized (DatabaseWrapper.class) {
			String f = database_name;
			Lock rwl = lockers.get(f);
			if (rwl == null) {
				rwl = new ReentrantLock();
				lockers.put(f, rwl);
				number_of_shared_lockers.put(f, new Integer(1));
			} else
				number_of_shared_lockers.put(f, new Integer(number_of_shared_lockers.get(f).intValue() + 1));

			return rwl;
		}
	}

	DatabaseTransactionEventsTable getTransactionsTable() throws DatabaseException {
		if (transactionTable == null)
			transactionTable = (DatabaseTransactionEventsTable) getTableInstance(DatabaseTransactionEventsTable.class);
		return transactionTable;
	}

	DatabaseHooksTable getHooksTransactionsTable() throws DatabaseException {
		if (databaseHooksTable == null)
			databaseHooksTable = (DatabaseHooksTable) getTableInstance(DatabaseHooksTable.class);
		return databaseHooksTable;
	}

	DatabaseTransactionsPerHostTable getDatabaseTransactionsPerHostTable() throws DatabaseException {
		if (databaseTransactionsPerHostTable == null)
			databaseTransactionsPerHostTable = (DatabaseTransactionsPerHostTable) getTableInstance(
					DatabaseTransactionsPerHostTable.class);
		return databaseTransactionsPerHostTable;
	}

	DatabaseEventsTable getDatabaseEventsTable() throws DatabaseException {
		if (databaseEventsTable == null)
			databaseEventsTable = (DatabaseEventsTable) getTableInstance(DatabaseEventsTable.class);
		return databaseEventsTable;
	}

	DatabaseDistantTransactionEvent getDatabaseDistantTransactionEvent() throws DatabaseException {
		if (databaseDistantTransactionEvent == null)
			databaseDistantTransactionEvent = (DatabaseDistantTransactionEvent) getTableInstance(
					DatabaseDistantTransactionEvent.class);
		return databaseDistantTransactionEvent;
	}

	DatabaseTransactionEventsTable getDatabaseTransactionEventsTable() throws DatabaseException {
		if (databaseTransactionEventsTable == null)
			databaseTransactionEventsTable = (DatabaseTransactionEventsTable) getTableInstance(
					DatabaseTransactionEventsTable.class);
		return databaseTransactionEventsTable;
	}

	IDTable getTransactionIDTable() throws DatabaseException {
		if (transactionIDTable == null)
			transactionIDTable = (IDTable) getTableInstance(IDTable.class);
		return transactionIDTable;
	}

	protected abstract Connection reopenConnectionImpl() throws DatabaseLoadingException;

	final Connection reopenConnection() throws DatabaseException {
		try {
			Connection c = reopenConnectionImpl();

			disableAutoCommit(c);
			return c;
		} catch (SQLException e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	/**
	 * 
	 * @return The Sql connection.
	 * @throws SQLException
	 */
	Connection getOpenedSqlConnection(Session transaction) throws DatabaseException {
		try {
			Connection sql_connection = transaction == null ? null : transaction.getConnection();
			if (sql_connection == null || sql_connection.isClosed() || !sql_connection.isValid(5)) {
				// this.locker.lockWrite();
				/*
				 * try {
				 */
				if (sql_connection != null && !sql_connection.isValid(5))
					closeConnection(sql_connection, false);
				if (sql_connection == null || sql_connection.isClosed())
					sql_connection = reopenConnection();
				/*
				 * } finally { this.locker.unlockWrite(); }
				 */
			}
			return sql_connection;
		} catch (SQLException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	protected abstract String getCachedKeyword();

	protected abstract void closeConnection(Connection c, boolean deepClosing) throws SQLException;

	public boolean isClosed() {
		return closed;
	}

	@Override
	public final void close() {
		if (!closed) {

			// synchronized(this)
			{
				try {

					
					try {
						lockWrite();
						closed = true;
						for (Iterator<Session> it = threadPerConnection.iterator(); it.hasNext();) {
							Session s = it.next();
							/*
							 * if (threadPerConnectionInProgress.containsKey(c.getKey())) continue;
							 */
							try {
								if (!s.getConnection().isClosed())
									closeConnection(s.getConnection(), true);
							} catch (SQLException e) {
								e.printStackTrace();
							} finally {
								it.remove();
							}
						}
					}
					finally
					{
						unlockWrite();
					}
					
				} finally {
					synchronized(DatabaseWrapper.class)
					{
						DatabaseWrapper.lockers.remove(database_name);
						int v = DatabaseWrapper.number_of_shared_lockers.get(database_name).intValue() - 1;
						if (v == 0)
							DatabaseWrapper.number_of_shared_lockers.remove(database_name);
						else if (v > 0)
							DatabaseWrapper.number_of_shared_lockers.put(database_name, new Integer(v));
						else
							throw new IllegalAccessError();
						sql_database = new HashMap<>();
					}
					
					System.gc();
				}
			}
			/*
			 * try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock()) {
			 * closeConnection(); } catch(SQLException se) { throw
			 * DatabaseException.getDatabaseException(se); } finally {
			 * try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock()) {
			 * DatabaseWrapper.lockers.remove(database_name); int
			 * v=DatabaseWrapper.number_of_shared_lockers.get(database_name).intValue()-1;
			 * if (v==0) DatabaseWrapper.number_of_shared_lockers.remove(database_name);
			 * else if (v>0) DatabaseWrapper.number_of_shared_lockers.put(database_name, new
			 * Integer(v)); else throw new IllegalAccessError(); sql_database.clear(); }
			 * closed=true; }
			 */
		}
	}

	@Override
	public void finalize() {
		try {
			close();
		} catch (Exception e) {

		}
	}

	@Override
	public int hashCode() {
		return database_name.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		return o == this;
		/*
		 * if (o==null) return false; if (o==this) return true; if (o instanceof
		 * HSQLDBWrapper) { return this.database_name.equals(((HSQLDBWrapper)
		 * o).database_name); } return false;
		 */
	}

	@Override
	public String toString() {
		return database_name;
	}

	private final Lock locker;
	
	protected void lockRead()
	{
		locker.lock();
	}
	
	protected void unlockRead()
	{
		locker.unlock();
	}
	
	protected void lockWrite()
	{
		locker.lock();
	}
	
	protected void unlockWrite()
	{
		locker.unlock();
	}
	

	class Session {
		private Connection connection;
		private final Thread thread;
		private final long threadID;
		private final Set<Table<?>> memoryTablesToRefresh;
		// private boolean sessionLocked=false;

		Session(Connection connection, Thread thread) {

			this.connection = connection;
			this.thread = thread;
			this.threadID = thread.getId();
			memoryTablesToRefresh=new HashSet<>();
		}

		void addTableToRefresh(Table<?> table)
		{
			memoryTablesToRefresh.add(table);
		}
		
		void refreshAllMemoryTables()
		{
			for (Table<?> t : memoryTablesToRefresh)
				t.setToRefreshNow();
			memoryTablesToRefresh.clear();
		}
		
		void setConnection(Connection c) {
			connection = c;
		}

		Connection getConnection() {
			return connection;
		}

		boolean isConcernedBy(Thread t) {
			return threadID == t.getId();
		}

		public Thread getThread() {
			return thread;
		}

		boolean addEvent(Table<?> table, TableEvent<?> de) throws DatabaseException {
			if (table == null)
				throw new NullPointerException("table");
			if (de == null)
				throw new NullPointerException("de");
			if (!table.supportSynchronizationWithOtherPeers())
				return false;
			Package p = table.getClass().getPackage();
			if (p.equals(DatabaseWrapper.class.getPackage()))
				return false;
			if (!getHooksTransactionsTable().supportPackage(p))
				return false;
			if (!transactionToSynchronize)
				return false;
			Database d = sql_database.get(p);

			if (d != null) {
				/*
				 * if (!sessionLocked) { locker.lockWrite(); sessionLocked=true; }
				 */
				addNewTemporaryEvent(table, de);
				return true;

			} else
				return false;
		}

		void clearTransactions(boolean commit) throws DatabaseException {
			boolean transactionOK = false;
			if (commit) {
				transactionOK = validateTmpTransaction();
			} else
				cancelTmpTransaction();

			/*
			 * if (sessionLocked) { sessionLocked=false; locker.unlockWrite();
			 * 
			 * }
			 */
			if (transactionOK) {
				getSynchronizer().notifyNewTransactionsIfNecessary();
			}
		}

		protected final AtomicLong idCounterForTmpTransactions = new AtomicLong(-1);

		protected final Map<Package, TransactionPerDatabase> temporaryTransactions = new HashMap<>();
		protected volatile int actualTransactionEventsNumber = 0;
		protected volatile boolean eventsStoredIntoMemory = true;
		protected volatile int actualPosition = 0;
		protected boolean transactionToSynchronize=false;

		private void checkIfEventsMustBeStoredIntoDisk() throws DatabaseException {

			if (eventsStoredIntoMemory && actualTransactionEventsNumber >= getMaxTransactionEventsKeepedIntoMemory()) {
				runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

					@Override
					public Void run() throws Exception {
						for (TransactionPerDatabase t : temporaryTransactions.values()) {
							t.transaction = getDatabaseTransactionEventsTable().addRecord(t.transaction);
							for (DatabaseEventsTable.Record r : t.events) {
								r.setTransaction(t.transaction);
								getDatabaseEventsTable().addRecord(r);
							}
							t.events.clear();
						}
						eventsStoredIntoMemory = false;
						return null;
					}

					@Override
					public TransactionIsolation getTransactionIsolation() {
						return TransactionIsolation.TRANSACTION_READ_UNCOMMITTED;
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

		protected TransactionPerDatabase getAndCreateIfNecessaryTemporaryTransaction(Package concernedDatabase)
				throws DatabaseException {

			checkIfEventsMustBeStoredIntoDisk();

			TransactionPerDatabase res = temporaryTransactions.get(concernedDatabase);
			if (res == null) {
				DatabaseTransactionEventsTable.Record t = new DatabaseTransactionEventsTable.Record(
						idCounterForTmpTransactions.decrementAndGet(), concernedDatabase.getName());
				if (eventsStoredIntoMemory)
					res = new TransactionPerDatabase(t, getMaxTransactionEventsKeepedIntoMemory());
				else
					res = new TransactionPerDatabase(getDatabaseTransactionEventsTable().addRecord(t), 0);
				temporaryTransactions.put(concernedDatabase, res);
			}
			return res;
		}

		void addNewTemporaryEvent(final Table<?> table, final TableEvent<?> event) throws DatabaseException {
			final TransactionPerDatabase transaction = getAndCreateIfNecessaryTemporaryTransaction(
					table.getDatabaseConfiguration().getPackage());
			final AtomicInteger nb = new AtomicInteger(0);
			try {
				if (eventsStoredIntoMemory) {
					try {

						final DatabaseEventsTable.Record originalEvent = new DatabaseEventsTable.Record(
								transaction.transaction, event, DatabaseWrapper.this);
						DatabaseEventsTable.Record eventr = originalEvent;

						for (Iterator<DatabaseEventsTable.Record> it = transaction.events.iterator(); it.hasNext();) {
							DatabaseEventsTable.Record _record = it.next();
							if (_record == null)
								throw new NullPointerException();
							if (Arrays.equals(_record.getConcernedSerializedPrimaryKey(),
									originalEvent.getConcernedSerializedPrimaryKey())
									&& _record.getConcernedTable().equals(originalEvent.getConcernedTable())) {
								if (event.getType() == DatabaseEventType.UPDATE
										&& _record.getType() == DatabaseEventType.ADD.getByte()) {
									eventr = new DatabaseEventsTable.Record(transaction.transaction,
											new TableEvent<DatabaseRecord>(event.getID(), DatabaseEventType.ADD, null,
													event.getNewDatabaseRecord(), event.getHostsDestination()),
											DatabaseWrapper.this);
								} else if ((event.getType() == DatabaseEventType.REMOVE
										|| event.getType() == DatabaseEventType.REMOVE_WITH_CASCADE)
										&& _record.getType() == DatabaseEventType.ADD.getByte()) {
									eventr = null;
								}

								it.remove();
								nb.decrementAndGet();
							}
						}

						if (event.getType() == DatabaseEventType.REMOVE_WITH_CASCADE) {
							final List<Table<?>> tables = new ArrayList<>();
							for (Class<? extends Table<?>> c : table.getTablesClassesPointingToThisTable()) {
								Table<?> t = getTableInstance(c);
								tables.add(t);
							}

							for (Iterator<DatabaseEventsTable.Record> it = transaction.events.iterator(); it
									.hasNext();) {
								DatabaseEventsTable.Record _record = it.next();
								for (Table<?> t : tables) {
									if (t.getClass().getName().equals(_record.getConcernedTable())) {
										DatabaseRecord dr = t.getDefaultRecordConstructor().newInstance();
										t.unserializeFields(dr, _record.getConcernedSerializedNewForeignKey(), false,
												true, false);
										for (ForeignKeyFieldAccessor fa : t.getForeignKeysFieldAccessors()) {
											if (fa.getPointedTable().getClass().equals(table.getClass())) {
												try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
														DataOutputStream dos = new DataOutputStream(baos)) {
													fa.serialize(dos, dr);
													dos.flush();
													if (Arrays.equals(baos.toByteArray(),
															originalEvent.getConcernedSerializedPrimaryKey())) {
														nb.decrementAndGet();
														it.remove();
													}
												}
											}
										}
										break;
									}
								}
							}

						}
						if (eventr != null) {
							Set<AbstractDecentralizedID> hosts = event.getHostsDestination();
							if (hosts != null)
								transaction.concernedHosts.addAll(hosts);
							eventr.setPosition(actualPosition++);
							transaction.events.add(eventr);
							nb.incrementAndGet();
						}

					} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
							| InvocationTargetException | IOException e) {
						throw DatabaseException.getDatabaseException(e);
					}

				} else {

					runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

						@Override
						public Void run() throws Exception {
							final DatabaseEventsTable.Record originalEvent = new DatabaseEventsTable.Record(
									transaction.transaction, event, DatabaseWrapper.this);
							final AtomicReference<DatabaseEventsTable.Record> eventr = new AtomicReference<>(
									originalEvent);
							nb.addAndGet(-(int) getDatabaseEventsTable()
									.removeRecordsWithCascade(new Filter<DatabaseEventsTable.Record>() {

										@Override
										public boolean nextRecord(
												com.distrimind.ood.database.DatabaseEventsTable.Record _record)
												throws DatabaseException {
											if (event.getType() == DatabaseEventType.UPDATE
													&& _record.getType() == DatabaseEventType.ADD.getByte()) {
												eventr.set(new DatabaseEventsTable.Record(transaction.transaction,
														new TableEvent<DatabaseRecord>(event.getID(),
																DatabaseEventType.ADD, null,
																event.getNewDatabaseRecord(),
																event.getHostsDestination()),
														DatabaseWrapper.this));
											} else if ((event.getType() == DatabaseEventType.REMOVE
													|| event.getType() == DatabaseEventType.REMOVE_WITH_CASCADE)
													&& _record.getType() == DatabaseEventType.ADD.getByte()) {
												eventr.set(null);
											}
											return true;
										}

									}, "transaction=%transaction AND concernedSerializedPrimaryKey=%pks AND concernedTable=%concernedTable",
											"transaction", transaction.transaction, "pks",
											eventr.get().getConcernedSerializedPrimaryKey(), "concernedTable",
											eventr.get().getConcernedTable()));

							if (event.getType() == DatabaseEventType.REMOVE_WITH_CASCADE) {
								final List<Table<?>> tables = new ArrayList<>();
								StringBuffer sb = new StringBuffer();
								int index = 0;
								Map<String, Object> parameters = new HashMap<>();
								for (Class<? extends Table<?>> c : table.getTablesClassesPointingToThisTable()) {
									Table<?> t = getTableInstance(c);
									tables.add(t);
									if (sb.length() > 0)
										sb.append(" OR ");
									else
										sb.append(" AND (");
									String varName = "var" + (index++);
									sb.append("concernedTable=%" + varName + "");
									parameters.put(varName, t.getClass().getName());
								}
								if (sb.length() > 0)
									sb.append(")");
								parameters.put("transaction", transaction.transaction);
								nb.addAndGet(-(int) getDatabaseEventsTable()
										.removeRecordsWithCascade(new Filter<DatabaseEventsTable.Record>() {

											@Override
											public boolean nextRecord(
													com.distrimind.ood.database.DatabaseEventsTable.Record _record)
													throws DatabaseException {
												try {
													for (Table<?> t : tables) {
														if (t.getClass().getName()
																.equals(_record.getConcernedTable())) {
															DatabaseRecord dr = t.getDefaultRecordConstructor()
																	.newInstance();
															t.unserializeFields(dr,
																	_record.getConcernedSerializedNewForeignKey(),
																	false, true, false);
															for (ForeignKeyFieldAccessor fa : t
																	.getForeignKeysFieldAccessors()) {
																if (fa.getPointedTable().getClass()
																		.equals(table.getClass())) {
																	try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
																			DataOutputStream dos = new DataOutputStream(
																					baos)) {
																		fa.serialize(dos, dr);
																		dos.flush();
																		if (Arrays.equals(baos.toByteArray(),
																				originalEvent
																						.getConcernedSerializedPrimaryKey())) {
																			return true;
																		}
																	}
																}
															}
															return false;
														}
													}
													return false;
												} catch (Exception e) {
													throw DatabaseException.getDatabaseException(e);
												}

											}

										}, "transaction=%transaction" + sb.toString(), parameters));
							}
							if (eventr.get() != null) {
								Set<AbstractDecentralizedID> hosts = event.getHostsDestination();
								if (hosts != null)
									transaction.concernedHosts.addAll(hosts);
								eventr.get().setPosition(actualPosition++);
								getDatabaseEventsTable().addRecord(eventr.get());
								nb.incrementAndGet();
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
			} finally {
				transaction.eventsNumber += nb.get();
				actualTransactionEventsNumber += nb.get();
			}
		}

		void cancelTmpTransaction() throws DatabaseException {
			resetTmpTransaction(false);
			/*if (eventsStoredIntoMemory) {
				resetTmpTransaction();

			} else {
				
				runSynchronizedTransaction(new SynchronizedTransaction<Void>() {
					boolean alreadyDone=false;
					@Override
					public Void run() throws Exception {
						for (TransactionPerDatabase t : temporaryTransactions.values()) {
							getDatabaseEventsTable().removeRecords("transaction=%transaction", "transaction",
									t.transaction);
							getDatabaseTransactionEventsTable().removeRecord(t.transaction);
						}
						resetTmpTransaction();
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
						if (alreadyDone)
							throw new IllegalAccessError();
						else
							alreadyDone=false;

					}
				});
			}*/

		}

		int getActualPositionEvent() {
			return actualPosition;
		}

		void cancelTmpTransactionEvents(final int position) throws DatabaseException {
			if (position == actualPosition)
				return;
			if (eventsStoredIntoMemory) {
				for (TransactionPerDatabase t : temporaryTransactions.values()) {
					int nb = 0;
					for (Iterator<DatabaseEventsTable.Record> it = t.events.iterator(); it.hasNext();) {
						DatabaseEventsTable.Record dr = it.next();
						if (dr.getPosition() >= position) {
							it.remove();
							++nb;
						}
					}
					actualTransactionEventsNumber -= nb;
					t.eventsNumber -= nb;
				}

			} else {

				runSynchronizedTransaction(new SynchronizedTransaction<Void>() {
					boolean alreadyDone=false;
					@Override
					public Void run() throws Exception {
						for (TransactionPerDatabase t : temporaryTransactions.values()) {
							int nb = (int) getDatabaseEventsTable().removeRecords(
									"transaction=%transaction AND position>=%pos", "transaction", t.transaction, "pos",
									new Integer(position));
							actualTransactionEventsNumber -= nb;
							t.eventsNumber -= nb;
						}
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
						if (alreadyDone)
							throw new IllegalAccessError();
						else
							alreadyDone=false;

					}
				});

			}

		}

		void resetTmpTransaction(boolean transactionToSynchronize) {
			temporaryTransactions.clear();
			actualTransactionEventsNumber = 0;
			eventsStoredIntoMemory = true;
			this.transactionToSynchronize=transactionToSynchronize;
		}

		boolean validateTmpTransaction() throws DatabaseException {
			if (!transactionToSynchronize)
				return false;
			if (eventsStoredIntoMemory) {
				
				return runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {
					boolean alreadyDone=false;
					@Override
					public Boolean run() throws Exception {
						final AtomicBoolean transactionOK = new AtomicBoolean(false);
						
						for (final TransactionPerDatabase t : temporaryTransactions.values()) {

							if (t.eventsNumber > 0) {
								final AtomicReference<DatabaseTransactionEventsTable.Record> finalTR = new AtomicReference<>(
										null);
								/*
								 * final ArrayList<AbstractDecentralizedID> excludedHooks=new ArrayList<>();
								 * long
								 * previousLastTransactionID=getTransactionIDTable().getLastTransactionID();
								 * final AtomicBoolean hasIgnoredHooks=new AtomicBoolean(false);
								 */

								getHooksTransactionsTable().getRecords(new Filter<DatabaseHooksTable.Record>() {

									@Override
									public boolean nextRecord(DatabaseHooksTable.Record _record)
											throws DatabaseException {
										if (!_record.concernsLocalDatabaseHost()) {
											if (_record.isConcernedDatabaseByPackage(
													t.transaction.concernedDatabasePackage)
													&& (t.concernedHosts == null || t.concernedHosts.isEmpty()
															|| t.concernedHosts.contains(_record.getHostID()))) {

												if (finalTR.get() == null) {
													finalTR.set(t.transaction = getDatabaseTransactionEventsTable()
															.addRecord(new DatabaseTransactionEventsTable.Record(
																	getTransactionIDTable()
																			.getAndIncrementTransactionID(),
																	t.transaction.concernedDatabasePackage,
																	t.concernedHosts)));

													for (DatabaseEventsTable.Record r : t.events) {
														r.setTransaction(finalTR.get());
														getDatabaseEventsTable().addRecord(r);
													}

												}
												DatabaseTransactionsPerHostTable.Record trhost = new DatabaseTransactionsPerHostTable.Record();
												trhost.set(finalTR.get(), _record);
												getDatabaseTransactionsPerHostTable().addRecord(trhost);
												transactionOK.set(true);
											}
										}
										return false;
									}
								});
							}
						}
						resetTmpTransaction(true);
						return new Boolean(transactionOK.get());
					}

					@Override
					public TransactionIsolation getTransactionIsolation() {
						return TransactionIsolation.TRANSACTION_READ_UNCOMMITTED;
					}

					@Override
					public boolean doesWriteData() {
						return true;
					}

					@Override
					public void initOrReset() {
						if (alreadyDone)
							throw new IllegalAccessError();
						else
							alreadyDone=false;
					}

				}).booleanValue();

			} else {

				return runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {
					boolean alreadyDone=false;
					@Override
					public Boolean run() throws Exception {
						final AtomicBoolean transactionOK = new AtomicBoolean(false);
						for (final TransactionPerDatabase t : temporaryTransactions.values()) {

							if (t.eventsNumber > 0) {
								final AtomicReference<DatabaseTransactionEventsTable.Record> finalTR = new AtomicReference<>(
										null);
								/*
								 * final ArrayList<AbstractDecentralizedID> excludedHooks=new ArrayList<>();
								 * long
								 * previousLastTransactionID=getTransactionIDTable().getLastTransactionID();
								 * final AtomicBoolean hasIgnoredHooks=new AtomicBoolean(false);
								 */
								getHooksTransactionsTable().getRecords(new Filter<DatabaseHooksTable.Record>() {

									@Override
									public boolean nextRecord(DatabaseHooksTable.Record _record)
											throws DatabaseException {
										if (!_record.concernsLocalDatabaseHost()) {
											if (_record.isConcernedDatabaseByPackage(
													t.transaction.concernedDatabasePackage)
													&& (t.concernedHosts == null || t.concernedHosts.isEmpty()
															|| t.concernedHosts.contains(_record.getHostID()))) {
												// excludedHooks.add(_record.getHostID());
												if (finalTR.get() == null) {
													finalTR.set(getDatabaseTransactionEventsTable()
															.addRecord(new DatabaseTransactionEventsTable.Record(
																	getTransactionIDTable()
																			.getAndIncrementTransactionID(),
																	t.transaction.concernedDatabasePackage,
																	t.concernedHosts)));
													final HashMap<String, Object> hm = new HashMap<>();
													hm.put("transaction", finalTR.get());
													getDatabaseEventsTable().updateRecords(
															new AlterRecordFilter<DatabaseEventsTable.Record>() {

																@Override
																public void nextRecord(
																		DatabaseEventsTable.Record _record) {
																	update(hm);
																}
															}, "transaction=%transaction", "transaction",
															t.transaction);

												}
												DatabaseTransactionsPerHostTable.Record trhost = new DatabaseTransactionsPerHostTable.Record();
												trhost.set(finalTR.get(), _record);
												getDatabaseTransactionsPerHostTable().addRecord(trhost);
												transactionOK.set(true);
											}
											/*
											 * else hasIgnoredHooks.set(true);
											 */
										}
										return false;
									}
								});
								/*
								 * if (hasIgnoredHooks.get())
								 * getHooksTransactionsTable().actualizeLastTransactionID(excludedHooks,
								 * previousLastTransactionID);
								 */
							}
							getDatabaseTransactionEventsTable().removeRecordWithCascade(t.transaction);
						}
						resetTmpTransaction(true);
						return new Boolean(transactionOK.get());
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
						if (alreadyDone)
							throw new IllegalAccessError();
						else
							alreadyDone=false;
						
					}
				}).booleanValue();

			}

		}

	}

	private static class TransactionPerDatabase {
		DatabaseTransactionEventsTable.Record transaction;
		volatile int eventsNumber;
		final Set<AbstractDecentralizedID> concernedHosts;
		final ArrayList<DatabaseEventsTable.Record> events;

		TransactionPerDatabase(DatabaseTransactionEventsTable.Record transaction, int maxEventsNumberKeepedIntoMemory) {
			if (transaction == null)
				throw new NullPointerException();
			this.transaction = transaction;
			this.eventsNumber = 0;
			concernedHosts = new HashSet<>();
			events = new ArrayList<>(maxEventsNumberKeepedIntoMemory);
		}
	}

	// private final AtomicBoolean transaction_already_running=new
	// AtomicBoolean(false);
	private final List<Session> threadPerConnectionInProgress = new ArrayList<>();
	private final List<Session> threadPerConnection = new ArrayList<>();

	private static Session getSession(List<Session> sessions, Thread t) {
		for (Session s : sessions)
			if (s.isConcernedBy(t))
				return s;
		return null;
	}

	private static Session removeSession(List<Session> sessions, Thread t) {
		for (Iterator<Session> it = sessions.iterator(); it.hasNext();) {
			Session s = it.next();

			if (s.isConcernedBy(t)) {
				it.remove();
				return s;
			}
		}
		return null;
	}

	private static class ConnectionWrapper {
		final Session connection;
		final boolean newTransaction;

		ConnectionWrapper(Session connection, boolean newTransaction) {
			this.connection = connection;
			this.newTransaction = newTransaction;
		}
	}

	ConnectionWrapper isNewTransactionAndStartIt() throws DatabaseException {
		Thread t = Thread.currentThread();
		try {
			lockWrite();
			Session c = getSession(threadPerConnectionInProgress, t);

			if (c == null) {
				c = getSession(threadPerConnection, t);
				if (c == null) {
					c = new Session(getOpenedSqlConnection(null), t);
					threadPerConnection.add(c);
				}
				threadPerConnectionInProgress.add(c);

				return new ConnectionWrapper(c, true);
			} else
				return new ConnectionWrapper(c, false);
		}
		finally
		{
			unlockWrite();
		}
		
	}

	protected Session getConnectionAssociatedWithCurrentThread() throws DatabaseException {
		
		try {
			lockWrite();
			Session c = getSession(threadPerConnectionInProgress, Thread.currentThread());

			Connection c2 = getOpenedSqlConnection(c);
			if (c.getConnection() != c2) {
				c.setConnection(c2);
			}
			return c;
		}
		finally
		{
			unlockWrite();
		}
		
	}

	void releaseTransaction() throws DatabaseException {
		try {
			
			try {
				lockWrite();
				Session c = removeSession(threadPerConnectionInProgress, Thread.currentThread());
				if (c == null)
					throw new IllegalAccessError();
				if (closed) {
					if (!c.getConnection().isClosed())
						closeConnection(c.getConnection(), true);
				}

				for (Iterator<Session> it = threadPerConnection.iterator(); it.hasNext();) {
					Session s = it.next();

					if (!s.getThread().isAlive()) {
						if (!s.getConnection().isClosed() && !s.getConnection().isClosed())
							closeConnection(s.getConnection(), false);

						it.remove();
					}
				}
			} catch (Exception e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
		finally
		{
			unlockWrite();
		}
		
	}

	// private volatile boolean transaction_already_running=false;

	private boolean setTransactionIsolation(Session sql_connection, TransactionIsolation transactionIsolation,
			boolean write) throws SQLException, DatabaseException {
		// Connection sql_connection=getOpenedSqlConnection();

		transactionIsolation = getValidTransactionIsolation(transactionIsolation);
		if (transactionIsolation != null) {

			startTransaction(sql_connection, transactionIsolation, write);
			return true;
		} else
			return false;

	}

	protected TransactionIsolation getValidTransactionIsolation(TransactionIsolation transactionIsolation)
			throws SQLException, DatabaseException {

		if (supportTransactions()) {
			DatabaseMetaData dmd = getConnectionAssociatedWithCurrentThread().getConnection().getMetaData();
			if (dmd.supportsTransactionIsolationLevel(transactionIsolation.getCode())) {
				return transactionIsolation;
			} else {
				TransactionIsolation ti = transactionIsolation.getNext();
				while (ti != null) {
					if (dmd.supportsTransactionIsolationLevel(transactionIsolation.getCode())) {
						return ti;
					}
					ti = ti.getNext();
				}
				ti = transactionIsolation.getPrevious();
				while (ti != null) {
					if (dmd.supportsTransactionIsolationLevel(transactionIsolation.getCode())) {
						return ti;
					}
					ti = ti.getPrevious();
				}

			}
		}
		return null;
	}

	public boolean supportTransactions() throws DatabaseException {
		return ((Boolean) runTransaction(new Transaction() {

			@Override
			public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
				try {
					return new Boolean(getConnectionAssociatedWithCurrentThread().getConnection().getMetaData()
							.supportsTransactions());
				} catch (SQLException e) {
					throw DatabaseException.getDatabaseException(e);
				}
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_READ_UNCOMMITTED;
			}

			@Override
			public boolean doesWriteData() {
				return false;
			}

			@Override
			public void initOrReset() {
				
			}
		}, true)).booleanValue();
	}

	protected abstract void startTransaction(Session _openedConnection, TransactionIsolation transactionIsolation, boolean write)
			throws SQLException;
	/*{
		_openedConnection.getConnection().setTransactionIsolation(transactionIsolation.getCode());
	}*/

	protected void endTransaction(Session _openedConnection) {

	}

	protected abstract void rollback(Connection openedConnection) throws SQLException;

	protected abstract void commit(Connection openedConnection) throws SQLException, DatabaseException;

	protected abstract boolean supportSavePoint(Connection openedConnection) throws SQLException;

	protected abstract void rollback(Connection openedConnection, String savePointName, Savepoint savePoint)
			throws SQLException;

	protected abstract void disableAutoCommit(Connection openedConnection) throws SQLException;

	protected abstract Savepoint savePoint(Connection openedConnection, String savePoint) throws SQLException;

	protected abstract void releasePoint(Connection openedConnection, String _savePointName, Savepoint savepoint)
			throws SQLException;

	private volatile long savePoint = 0;

	protected String generateSavePointName() {
		return "Savepoint" + (++savePoint);
		/*
		 * HexadecimalEncodingAlgorithm h=new HexadecimalEncodingAlgorithm();
		 * StringBuffer sb=new StringBuffer("S"); h.convertToCharacters(new
		 * DecentralizedIDGenerator().getBytes(), sb); return sb.toString();
		 */
	}
	
	protected abstract boolean isSerializationException(SQLException e) throws DatabaseException;
	protected abstract boolean isDeconnectionException(SQLException e) throws DatabaseException;

	private boolean needsToLock()
	{
		return !isThreadSafe() || hasOnePeerSyncronized; 
	}
	
	Object runTransaction(final Transaction _transaction, boolean defaultTransaction) throws DatabaseException {

		Object res = null;
		
		
		ConnectionWrapper cw = isNewTransactionAndStartIt();
		final boolean writeData=_transaction.doesWriteData();
		boolean needsLock=needsToLock();
		if (cw.newTransaction) {
			try
			{
				if (needsLock)
				{
					if (writeData)
						lockWrite();
					else
						lockRead();
				}
				boolean retry=true;
				while(retry)
				{
					retry=false;
					String savePointName = null;
					Savepoint savePoint = null;
					try {
		
						setTransactionIsolation(cw.connection, _transaction.getTransactionIsolation(),writeData);
		
						if (writeData && supportSavePoint(cw.connection.getConnection())) {
							savePointName = generateSavePointName();
							savePoint = savePoint(cw.connection.getConnection(), savePointName);
						}
						cw.connection.resetTmpTransaction(needsLock);
						_transaction.initOrReset();
						res = _transaction.run(this);
						if (writeData)
							cw.connection.clearTransactions(true);
						try
						{
							if (writeData)
								lockWrite();
							commit(cw.connection.getConnection());
							if (writeData)
								cw.connection.refreshAllMemoryTables();
						}
						finally
						{
							if (writeData)
								unlockWrite();
						}
						if (writeData) {
							
							if (savePoint != null)
								releasePoint(cw.connection.getConnection(), savePointName, savePoint);
		
						}
							
						endTransaction(cw.connection);
					} catch (DatabaseException e) {
						Throwable t=e.getCause();
						while (t!=null)
						{
							if ((t instanceof SQLException) && (isSerializationException((SQLException)t) || isDeconnectionException((SQLException)t)))
							{
								retry=true;
								break;
							}
							t=t.getCause();
						}
						try {
							
							if (writeData)
								cw.connection.clearTransactions(false);
							rollback(cw.connection.getConnection());
							if (writeData) {
								if (savePoint != null || savePointName != null) {
									releasePoint(cw.connection.getConnection(), savePointName, savePoint);
								}
							}
						} catch (SQLException se) {
							throw new DatabaseIntegrityException("Impossible to rollback the database changments", se);
						}
						if (!retry)
							throw e;
					} catch (SQLException e) {
						
						try {
							if (writeData)
								cw.connection.clearTransactions(false);
							rollback(cw.connection.getConnection());
							if (writeData) {
								if (savePoint != null || savePointName != null) {
									releasePoint(cw.connection.getConnection(), savePointName, savePoint);
								}
		
							}
						} catch (SQLException se) {
							throw new DatabaseIntegrityException("Impossible to rollback the database changments", se);
						}
						retry=isSerializationException(e) || isDeconnectionException(e);
						if (!retry)
							throw DatabaseException.getDatabaseException(e);
					} finally {
						releaseTransaction();
					}
					if (retry)
					{
						cw = isNewTransactionAndStartIt();
						if (!cw.newTransaction)
							throw new IllegalAccessError();
					}
				}
			} finally {
				if (needsLock) {
					if (writeData)
						unlockWrite();
					else
						unlockRead();
				}
			}
			
		} else {
			String savePointName = null;
			Savepoint savePoint = null;
			int previousPosition = defaultTransaction?-1:getConnectionAssociatedWithCurrentThread().getActualPositionEvent();
			try {
				if (writeData && !defaultTransaction && supportSavePoint(cw.connection.getConnection())) {
					savePointName = generateSavePointName();
					savePoint = savePoint(cw.connection.getConnection(), savePointName);
				}
				_transaction.initOrReset();
				res = _transaction.run(this);
				if (savePoint != null)
					releasePoint(cw.connection.getConnection(), savePointName, savePoint);

			} catch (DatabaseException e) {
				try {
					if (writeData && savePoint != null) {
						rollback(cw.connection.getConnection(), savePointName, savePoint);
						getConnectionAssociatedWithCurrentThread().cancelTmpTransactionEvents(previousPosition);
						releasePoint(cw.connection.getConnection(), savePointName, savePoint);
					}
				} catch (SQLException se) {
					throw DatabaseException.getDatabaseException(e);
				}
				throw e;
			} catch (SQLException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
		return res;
		/*
		 * Object res=null; if (_transaction.doesWriteData()) { if
		 * (!transaction_already_running) { transaction_already_running=true; try {
		 * res=_transaction.run(this); sql_connection.commit(); }
		 * catch(DatabaseException e) { try { sql_connection.rollback(); }
		 * catch(SQLException se) { throw new
		 * DatabaseIntegrityException("Impossible to rollback the database changments",
		 * se); } throw e; } catch(SQLException e) { throw
		 * DatabaseException.getDatabaseException(e); } finally {
		 * transaction_already_running=false; } } else { res=_transaction.run(this); } }
		 * else { res=_transaction.run(this); } return res;
		 */
	}

	/**
	 * Run a transaction by locking this database with the current thread. During
	 * this transaction execution, no transaction will be able to be run thanks to
	 * another thread.
	 * 
	 * @param _transaction
	 *            the transaction to run
	 * @return the result of the transaction
	 * @throws DatabaseException
	 *             if an exception occurs during the transaction running
	 * @param <O>
	 *            a type
	 */
	@SuppressWarnings("unchecked")
	public <O> O runSynchronizedTransaction(final SynchronizedTransaction<O> _transaction) throws DatabaseException {
		return (O) this.runTransaction(new Transaction() {

			@Override
			public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
				try {
					return (Object) _transaction.run();

				} catch (Exception e) {
					throw DatabaseException.getDatabaseException(e);
				}
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return _transaction.getTransactionIsolation();
			}

			@Override
			public boolean doesWriteData() {
				return _transaction.doesWriteData();
			}
			
			@Override
			public void initOrReset() throws DatabaseException
			{
				try
				{
					_transaction.initOrReset();
				}
				catch(Exception e)
				{
					throw DatabaseException.getDatabaseException(e);
				}
			}
		}, false);
	}

	/**
	 * According a class name, returns the instance of a table which inherits the
	 * class <code>Table&lsaquo;T extends DatabaseRecord&rsaquo;</code>. The
	 * returned table is always the same instance.
	 * 
	 * @param _table_name
	 *            the full class name (with its package)
	 * @return the corresponding table.
	 * @throws DatabaseException
	 *             if the class have not be found or if problems occur during the
	 *             instantiation.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	public final Table<?> getTableInstance(String _table_name) throws DatabaseException {
		
		try {
			lockWrite();
			if (_table_name == null)
				throw new NullPointerException("The parameter _table_name is a null pointer !");

			try {
				Class<?> c = Class.forName(_table_name);
				if (Table.class.isAssignableFrom(c)) {
					@SuppressWarnings("unchecked")
					Class<? extends Table<?>> class_table = (Class<? extends Table<?>>) c;
					return getTableInstance(class_table);
				} else
					throw new DatabaseException(
							"The class " + _table_name + " does not extends " + Table.class.getName());
			} catch (ClassNotFoundException e) {
				throw new DatabaseException("Impossible to found the class/table " + _table_name);
			}
		}
		finally
		{
			unlockWrite();
		}
		
		/*
		 * try(ReadWriteLock.Lock lock=locker.getAutoCloseableWriteLock()) { if
		 * (_table_name==null) throw new
		 * NullPointerException("The parameter _table_name is a null pointer !");
		 * 
		 * try { Class<?> c=Class.forName(_table_name); if
		 * (Table.class.isAssignableFrom(c)) {
		 * 
		 * @SuppressWarnings("unchecked") Class<? extends Table<?>> class_table=(Class<?
		 * extends Table<?>>)c; return getTableInstance(class_table); } else throw new
		 * DatabaseException("The class "+_table_name+" does not extends "+Table.class.
		 * getName()); } catch (ClassNotFoundException e) { throw new
		 * DatabaseException("Impossible to found the class/table "+_table_name); } }
		 */
	}

	/**
	 * According a Class&lsaquo;? extends Table&lsaquo;?&rsaquo;&rsaquo;, returns
	 * the instance of a table which inherits the class
	 * <code>Table&lsaquo;T extends DatabaseRecord&rsaquo;</code>. The returned
	 * table is always the same instance.
	 * 
	 * @param _class_table
	 *            the class type
	 * @return the corresponding table.
	 * @throws DatabaseException
	 *             if problems occur during the instantiation.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @param <TT>
	 *            The table type
	 */
	public final <TT extends Table<?>> Table<? extends Object> getTableInstance(Class<TT> _class_table)
			throws DatabaseException {

		try {
			lockWrite();
			if (_class_table == null)
				throw new NullPointerException("The parameter _class_table is a null pointer !");
			Database db = this.sql_database.get(_class_table.getPackage());
			if (db == null) {
				if (_class_table.getPackage().equals(this.getClass().getPackage()) && (actualDatabaseLoading == null
						|| !actualDatabaseLoading.getConfiguration().getPackage().equals(_class_table.getPackage()))) {
					loadDatabase(new DatabaseConfiguration(_class_table.getPackage(), internalDatabaseClassesList), true);
					db = this.sql_database.get(_class_table.getPackage());
				} else {
					try {
						lockWrite();
						if (actualDatabaseLoading != null && actualDatabaseLoading.getConfiguration().getPackage()
								.equals(_class_table.getPackage()))
							db = actualDatabaseLoading;
						else
							throw new DatabaseException(
									"The given database was not loaded : " + _class_table.getPackage());
					}
					finally
					{
						unlockWrite();
					}
				}
			}
			Table<?> founded_table = db.tables_instances.get(_class_table);
			if (founded_table != null)
				return founded_table;
			else
				throw new DatabaseException("Impossible to find the instance of the table " + _class_table.getName()
						+ ". It is possible that no SqlConnection was associated to the corresponding table.");
		}
		finally
		{
			unlockWrite();
		}

	}

	/*
	 * @SuppressWarnings("unchecked") public final <TT extends Table<?>> Table<?
	 * extends DatabaseRecord> getTableInstance(Class<TT> _class_table) throws
	 * DatabaseException { try(ReadWriteLock.Lock
	 * lock=locker.getAutoCloseableWriteLock()) { if (this.closed) throw new
	 * DatabaseException("The given Database was closed : "+this); if
	 * (_class_table==null) throw new
	 * NullPointerException("The parameter _class_table is a null pointer !");
	 * 
	 * if (this.tables_instances.containsKey(_class_table)) return
	 * (TT)this.tables_instances.get(_class_table); else { checkRowCountTable();
	 * return loadTable(_class_table); } } }
	 * 
	 * @SuppressWarnings("unchecked") private <TT extends Table<?>> TT
	 * loadTable(Class<TT> _class_table) throws DatabaseException { try {
	 * ArrayList<Table<?>> list_tables=new ArrayList<>(); LinkedList<Class<? extends
	 * Table<?>>> list_classes_to_instanciate=new LinkedList<>();
	 * 
	 * TT res=newInstance(_class_table); this.tables_instances.put(_class_table,
	 * res); list_tables.add(res); list_classes_to_instanciate.push(_class_table);
	 * 
	 * while (list_classes_to_instanciate.size()>0) { Class<? extends Table<?>>
	 * c=list_classes_to_instanciate.poll(); Table<?> t=tables_instances.get(c); if
	 * (t==null) { t=newInstance(c); list_tables.add(t);
	 * this.tables_instances.put(c, t); } for (ForeignKeyFieldAccessor fkfa :
	 * t.getForeignKeysFieldAccessors()) list_classes_to_instanciate.add((Class<?
	 * extends Table<?>>)fkfa.getFieldClassType()); } for (Table<?> t : list_tables)
	 * { t.initializeStep1(); } for (Table<?> t : list_tables) {
	 * t.initializeStep2(); } for (Table<?> t : list_tables) { t.initializeStep3();
	 * } return res; } catch (InstantiationException | IllegalAccessException |
	 * IllegalArgumentException | InvocationTargetException |
	 * PrivilegedActionException e) { throw new
	 * DatabaseException("Impossible to access to a class ! ", e); } catch(Exception
	 * e) { throw DatabaseException.getDatabaseException(e); } }
	 * 
	 * private void checkRowCountTable() throws DatabaseException {
	 * runTransaction(new Transaction() {
	 * 
	 * @Override public Boolean run(DatabaseWrapper sql_connection) throws
	 * DatabaseException { try { boolean table_found=false; try (ReadQuerry rq=new
	 * ReadQuerry(sql_connection.getSqlConnection(),
	 * "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"
	 * +ROW_COUNT_TABLES+"'")) { if (rq.result_set.next()) table_found=true; } if
	 * (!table_found) { Statement
	 * st=sql_connection.getSqlConnection().createStatement();
	 * st.executeUpdate("CREATE TABLE "
	 * +ROW_COUNT_TABLES+" (TABLE_NAME VARCHAR(512), ROW_COUNT INTEGER)");
	 * st.close(); }
	 * 
	 * 
	 * return null; } catch(Exception e) { throw
	 * DatabaseException.getDatabaseException(e); } }
	 * 
	 * @Override public boolean doesWriteData() { return true; }
	 * 
	 * });
	 * 
	 * }
	 */

	/**
	 * Remove a database
	 * 
	 * @param configuration
	 *            the database configuration
	 * @throws DatabaseException
	 *             if a problem occurs
	 */
	public final void deleteDatabase(final DatabaseConfiguration configuration) throws DatabaseException {
		try  {
			lockWrite();

			runTransaction(new Transaction() {

				@Override
				public Void run(DatabaseWrapper sql_connection) throws DatabaseException {
					try {
						if (!sql_database.containsKey(configuration.getPackage()))
							loadDatabase(configuration, false);

					} catch (DatabaseException e) {
						return null;
					}

					Database db = sql_database.get(configuration.getPackage());
					if (db == null)
						throw new IllegalAccessError();

					ArrayList<Table<?>> list_tables = new ArrayList<>(configuration.getTableClasses().size());
					for (Class<? extends Table<?>> c : configuration.getTableClasses()) {
						Table<?> t = getTableInstance(c);
						list_tables.add(t);
					}
					for (Table<?> t : list_tables) {
						t.removeTableFromDatabaseStep1();
					}
					for (Table<?> t : list_tables) {
						t.removeTableFromDatabaseStep2();
					}

					@SuppressWarnings("unchecked")
					HashMap<Package, Database> sd = (HashMap<Package, Database>) sql_database.clone();
					sd.remove(configuration.getPackage());
					sql_database = sd;

					return null;
				}

				@Override
				public boolean doesWriteData() {
					return true;
				}

				@Override
				public TransactionIsolation getTransactionIsolation() {
					return TransactionIsolation.TRANSACTION_SERIALIZABLE;
				}

				@Override
				public void initOrReset() {
					
				}
			}, true);

		}
		finally
		{
			unlockWrite();
		}
	}

	/**
	 * Associate a Sql database with a given database configuration. Every
	 * table/class in the given configuration which inherits to the class
	 * <code>Table&lsaquo;T extends DatabaseRecord&rsaquo;</code> will be included
	 * into the same database. This function must be called before every any
	 * operation with the corresponding tables.
	 * 
	 * @param configuration
	 *            the database configuration
	 * @param createDatabaseIfNecessaryAndCheckIt
	 *            If set to false, and if the database does not exists, generate a
	 *            DatabaseException. If set to true, and if the database does not
	 *            exists, create it. Use
	 *            {@link DatabaseConfiguration#getDatabaseCreationCallable()} if the
	 *            database is created and if transfer from old database must done.
	 * @throws DatabaseException
	 *             if the given package is already associated to a database, or if
	 *             the database cannot be created.
	 * @throws NullPointerException
	 *             if the given parameters are null.
	 */
	public final void loadDatabase(final DatabaseConfiguration configuration,
			final boolean createDatabaseIfNecessaryAndCheckIt) throws DatabaseException {
		try  {
			lockWrite();
			if (this.closed)
				throw new DatabaseException("The given Database was closed : " + this);
			if (configuration == null)
				throw new NullPointerException("tables is a null pointer.");

			if (sql_database.containsKey(configuration.getPackage()))
				throw new DatabaseException("There is already a database associated to the given HSQLDBWrappe ");
			try {
				
				final AtomicBoolean allNotFound = new AtomicBoolean(true);
				runTransaction(new Transaction() {

					@Override
					public Void run(DatabaseWrapper sql_connection) throws DatabaseException {
						try {

							/*
							 * boolean table_found=false; try (ReadQuerry rq=new
							 * ReadQuerry(sql_connection.getSqlConnection(),
							 * "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"
							 * +ROW_COUNT_TABLES+"'")) { if (rq.result_set.next()) table_found=true; }
							 */

							if (!doesTableExists(ROW_COUNT_TABLES)) {
								Statement st = getConnectionAssociatedWithCurrentThread().getConnection()
										.createStatement();
								st.executeUpdate("CREATE TABLE " + ROW_COUNT_TABLES
										+ " (TABLE_NAME VARCHAR(512), ROW_COUNT INTEGER)" + getSqlComma());
								st.close();
							}

							ArrayList<Table<?>> list_tables = new ArrayList<>(configuration.getTableClasses().size());
							for (Class<? extends Table<?>> class_to_load : configuration.getTableClasses()) {
								Table<?> t = newInstance(class_to_load);
								list_tables.add(t);
								actualDatabaseLoading.tables_instances.put(class_to_load, t);
							}

							for (Table<?> t : list_tables) {
								t.initializeStep1(configuration);
							}
							for (Table<?> t : list_tables) {
								allNotFound.set(
										!t.initializeStep2(createDatabaseIfNecessaryAndCheckIt) && allNotFound.get());
							}
							for (Table<?> t : list_tables) {
								t.initializeStep3();
							}

							return null;

						} catch (ClassNotFoundException e) {
							throw new DatabaseException(
									"Impossible to access to t)he list of classes contained into the package "
											+ configuration.getPackage().getName(),
									e);
						} catch (IOException e) {
							throw new DatabaseException(
									"Impossible to access to the list of classes contained into the package "
											+ configuration.getPackage().getName(),
									e);
						} catch (Exception e) {
							throw DatabaseException.getDatabaseException(e);
						}
					}

					@Override
					public boolean doesWriteData() {
						return true;
					}

					@Override
					public TransactionIsolation getTransactionIsolation() {
						return TransactionIsolation.TRANSACTION_SERIALIZABLE;
					}

					@Override
					public void initOrReset() {
						actualDatabaseLoading = new Database(configuration);
						allNotFound.set(true);
					}

				}, true);
				if (allNotFound.get()) {
					try {
						DatabaseConfiguration oldConfig = configuration.getOldVersionOfDatabaseConfiguration();
						DatabaseCreationCallable callable = configuration.getDatabaseCreationCallable();
						boolean removeOldDatabase = false;
						if (oldConfig != null && callable != null) {
							try {
								loadDatabase(oldConfig, false);
								if (callable != null)
									callable.transfertDatabaseFromOldVersion(configuration);
								removeOldDatabase = callable.hasToRemoveOldDatabase();
							} catch (DatabaseException e) {
								oldConfig = null;
							}
						}
						if (callable != null) {
							callable.afterDatabaseCreation(configuration);
							if (removeOldDatabase)
								deleteDatabase(oldConfig);
						}
					} catch (Exception e) {
						throw DatabaseException.getDatabaseException(e);
					}
				}
				@SuppressWarnings("unchecked")
				HashMap<Package, Database> sd = (HashMap<Package, Database>) sql_database.clone();
				sd.put(configuration.getPackage(), actualDatabaseLoading);
				sql_database = sd;
			} finally {
				actualDatabaseLoading = null;
				if (!configuration.getPackage().equals(this.getClass().getPackage()))
					getSynchronizer().isReliedToDistantHook();
			}
		}
		finally
		{
			unlockWrite();
		}
	}

	/**
	 * Gets the database configuration corresponding to the given package
	 * 
	 * @param _package
	 *            the package that's identify the database
	 * @return the database configuration corresponding to the given package
	 */
	public DatabaseConfiguration getDatabaseConfiguration(String _package) {
		if (_package == null)
			throw new NullPointerException();
		try  {
			lockWrite();
			for (Map.Entry<Package, Database> e : sql_database.entrySet()) {
				if (e.getKey().getName().equals(_package))
					return e.getValue().getConfiguration();
			}
			return null;
		}
		finally
		{
			unlockWrite();
		}
	}

	/**
	 * Gets the database configuration corresponding to the given package
	 * 
	 * @param _package
	 *            the package that's identify the database
	 * @return the database configuration corresponding to the given package
	 */
	public DatabaseConfiguration getDatabaseConfiguration(Package _package) {
		if (_package == null)
			throw new NullPointerException();
		try  {
			lockWrite();
			Database db = sql_database.get(_package);
			if (db == null)
				return null;
			else
				return db.getConfiguration();
		}
		finally
		{
			unlockWrite();
		}
	}

	<TT extends Table<?>> TT newInstance(Class<TT> _class_table) throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, DatabaseException, PrivilegedActionException {
		DefaultConstructorAccessPrivilegedAction<TT> class_privelege = new DefaultConstructorAccessPrivilegedAction<TT>(
				_class_table);

		Constructor<TT> const_table = (Constructor<TT>) AccessController.doPrivileged(class_privelege);

		TT t = (TT) const_table.newInstance();
		t.initializeStep0(this);
		return t;
	}

	protected abstract boolean doesTableExists(String tableName) throws Exception;

	protected abstract ColumnsReadQuerry getColumnMetaData(String tableName) throws Exception;

	protected abstract void checkConstraints(Table<?> table) throws DatabaseException;

	public static abstract class TableColumnsResultSet {
		protected ResultSet resultSet;

		protected TableColumnsResultSet(ResultSet rs) {
			resultSet = rs;
		}

		public boolean next() throws SQLException {
			return resultSet.next();
		}

		public abstract String getColumnName() throws SQLException;

		public abstract String getTypeName() throws SQLException;

		public abstract int getColumnSize() throws SQLException;

		public abstract boolean isNullable() throws SQLException;

		public abstract boolean isAutoIncrement() throws SQLException;
	}

	protected abstract String getSqlComma();

	protected abstract int getVarCharLimit();

	protected abstract boolean isVarBinarySupported();

	protected abstract boolean isLongVarBinarySupported();

	protected abstract String getSqlNULL();

	protected abstract String getSqlNotNULL();

	protected abstract String getByteType();

	protected abstract String getIntType();

	protected abstract String getSerializableType();

	protected abstract String getFloatType();

	protected abstract String getDoubleType();

	protected abstract String getShortType();

	protected abstract String getLongType();

	protected abstract String getBigDecimalType();

	protected abstract String getBigIntegerType();

	//protected abstract String getSqlQuerryToGetLastGeneratedID();

	protected abstract String getDropTableIfExistsKeyWord();

	protected abstract String getDropTableCascadeKeyWord();

	Collection<Table<?>> getListTables(Package p) {
		Database db = this.sql_database.get(p);
		if (db == null) {
			try  {
				lockWrite();
				if (actualDatabaseLoading != null && actualDatabaseLoading.getConfiguration().getPackage().equals(p))
					db = actualDatabaseLoading;
			}
			finally
			{
				unlockWrite();
			}
		}
		return db.tables_instances.values();
	}

	protected abstract String getOnUpdateCascadeSqlQuerry();

	protected abstract String getOnDeleteCascadeSqlQuerry();

	protected boolean supportUpdateCascade() {
		return !getOnUpdateCascadeSqlQuerry().equals("");
	}

	protected abstract Blob getBlob(byte[] bytes) throws SQLException;

	/**
	 * Backup the database into the given path.
	 * 
	 * @param path
	 *            the path where to save the database.
	 * @throws DatabaseException
	 *             if a problem occurs
	 */
	public abstract void backup(File path) throws DatabaseException;

	protected abstract boolean isThreadSafe();

	protected abstract boolean supportFullSqlFieldName();

}
