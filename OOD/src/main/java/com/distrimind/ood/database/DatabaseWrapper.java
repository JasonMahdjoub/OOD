
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

import com.distrimind.ood.database.DatabaseHooksTable.Record;
import com.distrimind.ood.database.Table.ColumnsReadQuerry;
import com.distrimind.ood.database.Table.DefaultConstructorAccessPrivilegedAction;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseIntegrityException;
import com.distrimind.ood.database.fieldaccessors.ByteTabObjectConverter;
import com.distrimind.ood.database.fieldaccessors.DefaultByteTabObjectConverter;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.fieldaccessors.ForeignKeyFieldAccessor;
import com.distrimind.ood.database.messages.*;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.FileTools;
import com.distrimind.util.Reference;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.crypto.SecureRandomType;
import com.distrimind.util.harddrive.Disk;
import com.distrimind.util.harddrive.HardDriveDetect;
import com.distrimind.util.harddrive.Partition;
import com.distrimind.util.io.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class represent a SqlJet database.
 * 
 * @author Jason Mahdjoub
 * @version 2.0
 * @since OOD 1.4
 */
@SuppressWarnings("ThrowFromFinallyBlock")
public abstract class DatabaseWrapper implements AutoCloseable {

	private static final List<Class<?>> internalDatabaseClassesList = Arrays.<Class<?>>asList(DatabaseDistantTransactionEvent.class, DatabaseDistantEventsTable.class,
			DatabaseEventsTable.class, DatabaseHooksTable.class, DatabaseTransactionEventsTable.class, DatabaseTransactionsPerHostTable.class, IDTable.class, DatabaseTable.class);
	
	
	// protected Connection sql_connection;
	private volatile boolean closed = false;
	protected final String database_name;
	protected final boolean loadToMemory;
	protected final File databaseDirectory;
	protected final String database_identifier;
	private static final HashMap<String, Lock> lockers = new HashMap<>();
	private static final HashMap<String, Integer> number_of_shared_lockers = new HashMap<>();
	final static String ROW_PROPERTIES_OF_TABLES = "ROW_PROPERTIES_OF_TABLES__";
	final static String VERSIONS_OF_DATABASE= "VERSIONS_OF_DATABASE__";
	final static String AUTOINCREMENT_TABLE= "AUTOINCREMENT_TABLE__";
	// private final HashMap<Class<? extends Table<?>>, Table<?>>
	// tables_instances=new HashMap<>();
	private final ArrayList<ByteTabObjectConverter> converters;

	volatile HashMap<Package, Database> sql_database = new HashMap<>();
	// private final DatabaseMetaData dmd;
	private volatile DatabaseTransactionEventsTable transactionTable = null;
	private volatile DatabaseHooksTable databaseHooksTable = null;
	private volatile DatabaseTable databaseTable =null;
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
	private final boolean alwaysDeconectAfterOnTransaction;
	private static final int MAX_TRANSACTIONS_TO_SYNCHRONIZE_AT_THE_SAME_TIME=1000000;
	public static final int MAX_DISTANT_PEERS=Short.MAX_VALUE;
	public static final int MAX_PACKAGE_TO_SYNCHRONIZE=Short.MAX_VALUE;
	private static volatile int MAX_HOST_NUMBERS=5;

	public static int getMaxHostNumbers() {
		return MAX_HOST_NUMBERS;
	}

	public static void setMaxHostNumbers(int maxHostNumbers) {
		if (maxHostNumbers>=0xFFFF)
			throw new IllegalArgumentException();
		MAX_HOST_NUMBERS = maxHostNumbers;
	}

	public boolean isAlwaysDeconectAfterOnTransaction() {
        return alwaysDeconectAfterOnTransaction;
    }

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
		this.maxTransactionsToSynchronizeAtTheSameTime = Math.max(maxTransactionsToSynchronizeAtTheSameTime, MAX_TRANSACTIONS_TO_SYNCHRONIZE_AT_THE_SAME_TIME);
	}

	private static class DatabasePerVersion
	{
		HashMap<Class<? extends Table<?>>, Table<?>> tables_instances = new HashMap<>();

	}

	// private final boolean isWindows;
	private class Database {
		final HashMap<Integer, DatabasePerVersion> tables_per_versions = new HashMap<>();
		private long lastValidatedTransactionUTCForCentralBackup=Long.MIN_VALUE;
		BackupRestoreManager backupRestoreManager=null;
		private int currentVersion=-1;

		private final DatabaseConfiguration configuration;

		public Database(DatabaseConfiguration configuration) {
			if (configuration == null)
				throw new NullPointerException("configuration");
			this.configuration = configuration;
		}

		int getCurrentVersion() throws DatabaseException {
			if (currentVersion==-1)
				DatabaseWrapper.this.getCurrentDatabaseVersion(configuration.getPackage(), true);
			return currentVersion;
		}

		void updateCurrentVersion()
		{
			this.currentVersion=-1;
		}

		DatabaseConfiguration getConfiguration() {
			return configuration;
		}

		void initBackupRestoreManager(DatabaseWrapper wrapper, File databaseDirectory, DatabaseConfiguration configuration) throws DatabaseException {
			if (this.backupRestoreManager!=null)
				return;
			if (configuration.getBackupConfiguration()!=null)
			{
				this.backupRestoreManager =new BackupRestoreManager(wrapper, new File(new File(databaseDirectory, "nativeBackups"), DatabaseWrapper.getLongPackageName(configuration.getPackage())), configuration, false);
				while((configuration=configuration.getOldVersionOfDatabaseConfiguration())!=null)
				{
					if (this.configuration.getPackage().equals(configuration.getPackage()))
						continue;

					File f=new File(new File(databaseDirectory, "nativeBackups"),DatabaseWrapper.getLongPackageName(configuration.getPackage()));
					if (!f.exists())
						continue;
					BackupRestoreManager m=new BackupRestoreManager(wrapper, f,  configuration, false);
					m.cleanOldBackups();
					if (m.isEmpty())
					{
						FileTools.deleteDirectory(f);
					}
				}
			}
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
	 * @param databaseDirectory the database file or directory. Can be null for distant database.
     * @param alwaysDisconnectAfterOnTransaction true if the database must always be connected and detected during one transaction
	 * @throws DatabaseException if a problem occurs
	 * 
	 */
	protected DatabaseWrapper(String _database_name, File databaseDirectory, boolean alwaysDisconnectAfterOnTransaction, boolean loadToMemory) throws DatabaseException {

		if (loadToMemory)
		{
			this.alwaysDeconectAfterOnTransaction=false;
			this.database_name=_database_name;
			this.loadToMemory=true;
			this.database_identifier=this.database_name;
			this.databaseDirectory=null;
		}
		else {
			if (_database_name == null)
				throw new NullPointerException("_database_name");
			if (databaseDirectory==null)
				throw new NullPointerException();
			if (databaseDirectory.exists() && !databaseDirectory.isDirectory())
				throw new IllegalArgumentException();

			this.loadToMemory=false;
			this.alwaysDeconectAfterOnTransaction = alwaysDisconnectAfterOnTransaction;
			/*
			 * if (_sql_connection==null) throw new NullPointerException("_sql_connection");
			 */

			database_name = _database_name;
			this.databaseDirectory = databaseDirectory;
			Disk disk = null;
			try {
				Partition p = HardDriveDetect.getInstance().getConcernedPartition(databaseDirectory);
				disk = p.getDisk();
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (disk != null)
				database_identifier = disk.toString();
			else
				database_identifier = _database_name;
		}
		try {
			this.randomForKeys = SecureRandomType.BC_FIPS_APPROVED_FOR_KEYS.getSingleton(null);
		} catch (Exception e) {
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}
		// sql_connection=_sql_connection;

		// isWindows=OSValidator.isWindows();

		locker = getLocker();
		converters = new ArrayList<>();
		converters.add(new DefaultByteTabObjectConverter());
		synchronizer=new DatabaseSynchronizer();
	}

	public boolean isLoadedToMemory()
	{
		return loadToMemory;
	}

	public File getDatabaseDirectory() {
		return databaseDirectory;
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
		return (Boolean) runTransaction(new Transaction() {

            @Override
            public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
                try {
                    Session sql_connection = getConnectionAssociatedWithCurrentThread();
                    return sql_connection.getConnection().isReadOnly();
                } catch (SQLException e) {
                    throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
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
        }, true);

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
		private final DatabaseHooksTable.Record hook;
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

		public DecentralizedValue getHostID() {
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
	/*private long getLastTransactionUTC() throws DatabaseException {

		long res=Long.MIN_VALUE;
		for (Database d : sql_database.values())
		{
			if (d.backupRestoreManager!=null)
				res=Math.max(d.backupRestoreManager.getLastTransactionUTCInMS(), res);
		}
		return res;
	}*/
	private static class ValidatedIDPerDistantHook
	{
		final HashMap<String, TreeSet<DatabaseBackupMetaDataPerFile>> metaData=new HashMap<>();
		long getFirstTransactionID(String packageString)
		{
			TreeSet<DatabaseBackupMetaDataPerFile> ts=metaData.get(packageString);
			if (ts==null || ts.size()==0)
				return Long.MAX_VALUE;
			else
				return ts.first().getFirstTransactionID();
		}
		void addMetaData(String packageString, DatabaseBackupMetaDataPerFile m)  {
			if (packageString==null)
				throw new NullPointerException();
			if (packageString.trim().length()==0)
				throw new IllegalArgumentException();
			TreeSet<DatabaseBackupMetaDataPerFile> ts=metaData.get(packageString);
			if (ts==null)
				metaData.put(packageString, ts=new TreeSet<>());
			for (DatabaseBackupMetaDataPerFile me : ts) {
				if (me.timeStampUTC == m.timeStampUTC)
					return;
				if (me.timeStampUTC > m.timeStampUTC)
					break;
			}

			ts.add(m);
		}
		long getLastTransactionID(String packageString)
		{
			TreeSet<DatabaseBackupMetaDataPerFile> ts=metaData.get(packageString);

			if (ts==null || ts.size()==0)
				return Long.MIN_VALUE;
			else
				return ts.last().getLastTransactionID();
		}

		long getFileUTCToTransfer(String packageString, long lastValidatedDistantTransactionID) {
			TreeSet<DatabaseBackupMetaDataPerFile> ts=metaData.get(packageString);
			if (ts==null)
				return Long.MIN_VALUE;
			++lastValidatedDistantTransactionID;
			for (DatabaseBackupMetaDataPerFile m : ts)
			{
				if (lastValidatedDistantTransactionID>=m.getFirstTransactionID() && lastValidatedDistantTransactionID<=m.getLastTransactionID())
					return m.getFileTimestampUTC();
			}
			return Long.MIN_VALUE;
		}

		void validateLastTransaction(String packageString, long lastValidatedTransaction) {
			TreeSet<DatabaseBackupMetaDataPerFile> ts=metaData.get(packageString);
			if (ts==null)
				return;
			for (Iterator<DatabaseBackupMetaDataPerFile> it = ts.iterator(); it.hasNext(); ) {
				if (it.next().getLastTransactionID() <= lastValidatedTransaction) {
					it.remove();
				} else
					break;
			}
		}
	}




	public class DatabaseSynchronizer {
		private DatabaseNotifier notifier = null;
		private boolean canNotify = true;
		private final LinkedList<DatabaseEvent> events = new LinkedList<>();
		protected final HashMap<DecentralizedValue, ConnectedPeers> initializedHooks = new HashMap<>();
		protected final HashMap<DecentralizedValue, ConnectedPeers> initializedHooksWithCentralBackup = new HashMap<>();
		protected final HashMap<DecentralizedValue, Long> suspendedHooksWithCentralBackup = new HashMap<>();
		protected final HashMap<DecentralizedValue, Long> lastValidatedTransactionIDFromCentralBackup = new HashMap<>();
		private final HashMap<DecentralizedValue, ValidatedIDPerDistantHook> validatedIDPerDistantHook =new HashMap<>();
		protected boolean centralBackupInitialized=false;
		private final Condition newEventCondition=locker.newCondition();
		private boolean extendedTransactionInProgress=false;
		private long lastTransactionID=Long.MIN_VALUE;
		/*private final TreeSet<DatabaseBackupToIncorporateFromCentralDatabaseBackup> differedDatabaseBackupToIncorporate=new TreeSet<>();
		private final HashMap<DatabaseBackupToIncorporateFromCentralDatabaseBackup, InputStreamGetter> differedDatabaseBackupToIncorporateInputStreams=new HashMap<>();*/
		private final Set<String> backupDatabasePartsSynchronizingWithCentralDatabaseBackup=new HashSet<>();
		private final Set<DecentralizedValue> otherBackupDatabasePartsSynchronizingWithCentralDatabaseBackup=new HashSet<>();
		private boolean sendIndirectTransactions;
		private AbstractSecureRandom random=null;
		private EncryptionProfileProvider encryptionProfileProvider=null;

		DatabaseSynchronizer() {
		}

		public DatabaseNotifier getNotifier() {
			return notifier;
		}

		void startExtendedTransaction() throws DatabaseException {
			extendedTransactionInProgress=true;
			lastTransactionID=getTransactionIDTable().getLastTransactionID();
		}

		public boolean isSendIndirectTransactions() {
			return sendIndirectTransactions;
		}

		void validateExtendedTransaction() throws DatabaseException {
			lastTransactionID=Long.MIN_VALUE;
			extendedTransactionInProgress=false;
			notifyNewEvent();
		}

		void cancelExtendedTransaction() throws DatabaseException {
			getDatabaseTransactionEventsTable().removeRecordsWithCascade("id>=%id", "id", lastTransactionID);
			getTransactionIDTable().setLastTransactionID(lastTransactionID);
			lastTransactionID=Long.MIN_VALUE;
			extendedTransactionInProgress=false;
		}

		public boolean isInitialized() throws DatabaseException {
			return getHooksTransactionsTable().getLocalDatabaseHost() != null;
		}

		public boolean isInitialized(DecentralizedValue hostID) {
			lockRead();
			try {
				return initializedHooks.get(hostID) != null;
			}
			finally {
				unlockRead();
			}
		}

		private void notifyNewEvent() throws DatabaseException {
			boolean notify = false;
			
			try {
				lockWrite();
				if (!extendedTransactionInProgress && canNotify && notifier != null) {
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
				if (this.extendedTransactionInProgress)
					return null;
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

		/*DatabaseNotifier getNotifier() {
			
			try {
				lockWrite();
				return notifier;
			}
			finally
			{
				unlockWrite();
			}
			
		}*/

		public DatabaseEvent waitNextEvent() throws InterruptedException {

			try {
				lockWrite();
				DatabaseEvent de = nextEvent();
				for (; de != null; de = nextEvent()) {
					newEventCondition.await();
				}
				return null;
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
		public void initLocalHostID(DecentralizedValue localHostID, boolean sendIndirectTransactions) throws DatabaseException {
			DatabaseHooksTable.Record local = getHooksTransactionsTable().getLocalDatabaseHost();
			if (local != null && !local.getHostID().equals(localHostID))
				throw new DatabaseException("The given local host id is different from the stored local host id !");
			if (local == null) {
				addHookForLocalDatabaseHost(localHostID);
			}
			getDatabaseTransactionEventsTable().cleanTmpTransactions();
			
			try {
				lockWrite();
                assert local != null;
				this.sendIndirectTransactions=sendIndirectTransactions;
                if (initializedHooks.containsKey(local.getHostID())) {
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



		public void addHookForLocalDatabaseHost(DecentralizedValue hostID, Package... databasePackages)
				throws DatabaseException {
			ArrayList<String> packages = new ArrayList<>();
			for (Package p : databasePackages) {
				packages.add(p.getName());
			}
			getHooksTransactionsTable().addHooks(hostID, true, false, new ArrayList<DecentralizedValue>(),
					packages);
		}

		public HookAddRequest askForHookAddingAndSynchronizeDatabase(DecentralizedValue hostID,
				boolean replaceDistantConflitualRecords, Package... packages) throws DatabaseException {
			ArrayList<String> packagesString = new ArrayList<>();
			for (Package p : packages)
				packagesString.add(p.getName());
			return askForHookAddingAndSynchronizeDatabase(hostID, true, replaceDistantConflitualRecords,
					packagesString);
		}

		private HookAddRequest askForHookAddingAndSynchronizeDatabase(DecentralizedValue hostID,
				boolean mustReturnMessage, boolean replaceDistantConflitualRecords, ArrayList<String> packages)
				throws DatabaseException {
			if (packages.size()>MAX_PACKAGE_TO_SYNCHRONIZE)
				throw new DatabaseException("The number of packages to synchronize cannot be greater than "+MAX_PACKAGE_TO_SYNCHRONIZE);
			final ArrayList<DecentralizedValue> hostAlreadySynchronized = new ArrayList<>();
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
				public void initOrReset() {
					
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
			if (hostAlreadySynchronized.size()>=MAX_DISTANT_PEERS)
				throw new DatabaseException("The maximum number of distant peers ("+MAX_DISTANT_PEERS+") is reached");
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

		public void removeHook(DecentralizedValue hostID, Package... databasePackages) throws DatabaseException {
			getHooksTransactionsTable().removeHooks(hostID, databasePackages);
			isReliedToDistantHook();
		}
		
		@SuppressWarnings("UnusedReturnValue")
        protected boolean isReliedToDistantHook() throws DatabaseException
		{
			return hasOnePeerSyncronized=getHooksTransactionsTable().hasRecordsWithAllFields("concernsDatabaseHost", Boolean.FALSE);
		}
		

		public long getLastValidatedSynchronization(DecentralizedValue hostID) throws DatabaseException {
			/*
			 * ConnectedPeers peer=null; synchronized(this) {
			 * peer=initializedHooks.get(hostID); } DatabaseHooksTable.Record r=null; if
			 * (peer==null) {
			 */
			DatabaseHooksTable.Record r = null;
			List<DatabaseHooksTable.Record> l = getHooksTransactionsTable()
					.getRecordsWithAllFields("hostID", hostID);
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

			return r.getLastValidatedDistantTransactionID();

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
							if (lastID > _record.getLastValidatedLocalTransactionID()) {
								cp.setTransferInProgress(true);
								addNewDatabaseEvent(new DatabaseEventsToSynchronizeP2P(
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

		void validateLastSynchronization(DecentralizedValue hostID, long lastTransferredTransactionID)
				throws DatabaseException {
			if (hostID == null)
				throw new NullPointerException("hostID");
			if (!isInitialized())
				throw new DatabaseException("The Synchronizer must be initialized (initLocalHostID function) !");
			ConnectedPeers peer;

			boolean central=false;
			try {
				lockWrite();
				peer = initializedHooks.get(hostID);

				if (peer==null) {
					central=true;
					peer= initializedHooksWithCentralBackup.get(hostID);
				}
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
			if (r.getLastValidatedLocalTransactionID() > lastTransferredTransactionID)
				throw new DatabaseException("The given transfer ID limit " + lastTransferredTransactionID
						+ " is lower than the stored transfer ID limit " + r.getLastValidatedLocalTransactionID());
			if (r.concernsLocalDatabaseHost())
				throw new DatabaseException("The given host ID correspond to the local database host !");

			long l = getDatabaseTransactionsPerHostTable().validateTransactions(r, lastTransferredTransactionID);
			if (l < lastTransferredTransactionID)
				throw new IllegalAccessError("l="+l+"; lastTransferredTransactionID="+lastTransferredTransactionID);

			if (!central)
			{
				if (l != lastTransferredTransactionID)
					addNewDatabaseEvent(new LastIDCorrection(getHooksTransactionsTable().getLocalDatabaseHost().getHostID(),
							hostID, l));

				synchronizedDataIfNecessary(peer);
			}
			synchronizeMetaData();
		}

		void sendLastValidatedIDIfConnected(DatabaseHooksTable.Record hook) throws DatabaseException {
			
			try {
				lockWrite();
				if (initializedHooks.containsKey(hook.getHostID()))
					addNewDatabaseEvent(new DatabaseWrapper.TransactionConfirmationEvents(
							getHooksTransactionsTable().getLocalDatabaseHost().getHostID(), hook.getHostID(),
							hook.getLastValidatedDistantTransactionID()));
			}
			finally
			{
				unlockWrite();
			}
			

		}



		private void synchronizeMetaData() throws DatabaseException {
			Map<DecentralizedValue, Long> lastIds = getHooksTransactionsTable()
					.getLastValidatedLocalTransactionIDs();

			try {
				lockWrite();
				for (DecentralizedValue host : lastIds.keySet()) {
					if (isInitialized(host)) {
                        Map<DecentralizedValue, Long> map = new HashMap<>(lastIds);
						// map.remove(hostID);
						map.remove(host);
						if (map.size() > 0) {
							addNewDatabaseEvent(new DatabaseTransactionsIdentifiersToSynchronize(
									getHooksTransactionsTable().getLocalDatabaseHost().getHostID(), host, map));
						}
					}
				}

				if (centralBackupInitialized && initializedHooksWithCentralBackup.size()>0) {
					for (ConnectedPeers cp : initializedHooksWithCentralBackup.values())
						checkMetaDataUpdate(cp.hook.getHostID());
				}
			} finally
			{
				unlockWrite();
			}
			
		}

		@SuppressWarnings("UnusedReturnValue")
        private long synchronizedDataIfNecessary() throws DatabaseException {
			final long lastID = getTransactionIDTable().getLastTransactionID();
			
			try {
				lockWrite();
				getHooksTransactionsTable().getRecords(new Filter<DatabaseHooksTable.Record>() {

					@Override
					public boolean nextRecord(Record _record) throws DatabaseException {
						ConnectedPeers cp = initializedHooks.get(_record.getHostID());
						if (cp != null && !_record.concernsLocalDatabaseHost() && !cp.isTransferInProgress()) {
							if (lastID > _record.getLastValidatedLocalTransactionID()) {
								cp.setTransferInProgress(true);
								addNewDatabaseEvent(new DatabaseEventsToSynchronizeP2P(
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

		@SuppressWarnings("UnusedReturnValue")
        private long synchronizedDataIfNecessary(ConnectedPeers peer) throws DatabaseException {
			if (peer.isTransferInProgress())
				return -1;
			long lastID = getTransactionIDTable().getLastTransactionID();
			
			try {
				lockWrite();
				DatabaseHooksTable.Record hook = getHooksTransactionsTable().getHook(peer.getHostID());
				if (lastID > hook.getLastValidatedLocalTransactionID() && !peer.isTransferInProgress()) {
					peer.setTransferInProgress(true);
					addNewDatabaseEvent(new DatabaseEventsToSynchronizeP2P(
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
		void addNewTransactionConfirmationEvents(DecentralizedValue hostDestination, String packageString, long lastValidatedTransaction) throws DatabaseException {
			try
			{
				lockWrite();
				boolean notify=false;
				if (isInitialized(hostDestination)) {
					notify=true;
					events.add(new DatabaseWrapper.TransactionConfirmationEvents(
							getLocalHostID(), hostDestination,
							lastValidatedTransaction));
				}
				ValidatedIDPerDistantHook v=validatedIDPerDistantHook.get(hostDestination);
				if (v!=null)
				{
					v.validateLastTransaction(packageString, lastValidatedTransaction);
				}
				if (notify)
					notifyNewEvent();

			}
			finally {
				unlockWrite();
			}

		}


		void addNewDatabaseEvent(DatabaseEvent e) throws DatabaseException {
			if (e == null)
				throw new NullPointerException("e");

			try {
				lockWrite();
				boolean add = true;
				if (e.getClass() == DatabaseEventsToSynchronizeP2P.class) {
					DatabaseEventsToSynchronizeP2P dets = (DatabaseEventsToSynchronizeP2P) e;
					for (DatabaseEvent detmp : events) {
						if (detmp.getClass() == DatabaseEventsToSynchronizeP2P.class
								&& dets.tryToMerge((DatabaseEventsToSynchronizeP2P) detmp)) {
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
		public DecentralizedValue getLocalHostID() throws DatabaseException {
			DatabaseHooksTable.Record r=getHooksTransactionsTable().getLocalDatabaseHost();
			if (r==null)
				return null;
			else
				return r.getHostID();
		}

		public void resetSynchronizerAndRemoveAllHosts() throws DatabaseException {
			resetSynchronizerAndGetAllHosts();
		}
		private Collection<DatabaseHooksTable.Record> resetSynchronizerAndGetAllHosts() throws DatabaseException {
			if (getLocalHostID()==null)
				return null;
			disconnectAll();
			Collection<DatabaseHooksTable.Record> r=getHooksTransactionsTable().resetAllHosts();
			getDatabaseTransactionEventsTable().resetAllTransactions();
			return r;
		}
		private void restoreHosts(Collection<DatabaseHooksTable.Record> hosts, boolean replaceDistantConflictualRecords) throws DatabaseException {
			if (hosts==null)
				return ;
			DatabaseHooksTable.Record local=null;
			for (Iterator<DatabaseHooksTable.Record> it=hosts.iterator();it.hasNext();)
			{
				DatabaseHooksTable.Record r=it.next();
				if (r.concernsLocalDatabaseHost()) {
					if (local!=null)
						throw new IllegalAccessError();
					local = r;
					it.remove();
				}
			}
			if (local==null)
				throw new IllegalAccessError();
			getHooksTransactionsTable().addHooks(local.getHostID(), true, false, new ArrayList<DecentralizedValue>(),
					Arrays.asList(local.getDatabasePackageNames()));

			for (DatabaseHooksTable.Record r : hosts)
				getHooksTransactionsTable().addHooks(r.getHostID(), false,
						replaceDistantConflictualRecords, new ArrayList<DecentralizedValue>(), Arrays.asList(r.getDatabasePackageNames()));
			isReliedToDistantHook();

		}
		public void disconnectAll() throws DatabaseException {
			DecentralizedValue hostID=getLocalHostID();
			if (isInitialized(hostID))
				disconnectHook(hostID);
			disconnectAllHooksFromThereBackups();
		}

		public void disconnectHook(final DecentralizedValue hostID) throws DatabaseException {
			if (hostID == null)
				throw new NullPointerException("hostID");

			Long backupChannelInitializationMessageFromCentralDatabaseBackup=null;
			DatabaseHooksTable.Record hook = null;
			try {
				lockWrite();
				ConnectedPeers peer = initializedHooks.remove(hostID);

				if (peer != null)
					hook = getHooksTransactionsTable().getHook(peer.getHostID());
				if (hook == null)
					throw DatabaseException.getDatabaseException(
							new IllegalAccessException("hostID " + hostID + " has not been initialized !"));
				if (hook.concernsLocalDatabaseHost()) {
					ArrayList<DecentralizedValue> hs=new ArrayList<>(initializedHooks.keySet());
					initializedHooks.clear();
					this.events.clear();
					if (notifier!=null) {
						for (DecentralizedValue h : hs)
							notifier.hostDisconnected(h);
					}
				} else {

					cancelEventToSend(hostID, false);
					notifier.hostDisconnected(hostID);
					backupChannelInitializationMessageFromCentralDatabaseBackup=suspendedHooksWithCentralBackup.remove(hostID);
				}
			}
			finally
			{
				unlockWrite();
			}
			if (backupChannelInitializationMessageFromCentralDatabaseBackup!=null)
			{
				initDistantBackupCenter(hook, backupChannelInitializationMessageFromCentralDatabaseBackup);
			}


		}


		public boolean isPairedWith(final DecentralizedValue hostID) throws DatabaseException {
			DatabaseHooksTable.Record r = runSynchronizedTransaction(
					new SynchronizedTransaction<DatabaseHooksTable.Record>() {

						@Override
						public DatabaseHooksTable.Record run() throws Exception {
							List<DatabaseHooksTable.Record> l = getHooksTransactionsTable()
									.getRecordsWithAllFields("hostID", hostID);
							DatabaseHooksTable.Record r = null;
							if (l.size() == 1)
								r = l.iterator().next();
							else if (l.size() > 1)
								throw new IllegalAccessError();

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
			return r!=null;
		}
		private void checkMetaDataUpdate(DecentralizedValue hostChannel) throws DatabaseException {
			ValidatedIDPerDistantHook v= validatedIDPerDistantHook.get(hostChannel);
			Long lastValidatedDistantID=lastValidatedTransactionIDFromCentralBackup.get(hostChannel);
			DatabaseHooksTable.Record r=null;
			for (String packageString : getDatabasePackagesToSynchronizeWithCentralBackup()) {

				long from=v.getLastTransactionID(packageString);
				if (from<lastValidatedDistantID)
					addNewDatabaseEvent(new AskForMetaDataPerFileToCentralDatabaseBackup(getLocalHostID(), hostChannel, from, packageString));
				else {
					long firstTransactionID = v.getFirstTransactionID(packageString);
					if (r==null) {
						r = getHooksTransactionsTable().getRecord("hostID", hostChannel);
						assert r != null;
					}
					if (firstTransactionID - 1 > r.getLastValidatedDistantTransactionID())
						addNewDatabaseEvent(new AskForMetaDataPerFileToCentralDatabaseBackup(getLocalHostID(), hostChannel, firstTransactionID, packageString));
					else {
						checkAskForEncryptedBackupFilePart(hostChannel, packageString);
					}
				}
			}
		}
		private void checkAskForEncryptedBackupFilePart(DecentralizedValue hostChannel, String packageString) throws DatabaseException {
			if (otherBackupDatabasePartsSynchronizingWithCentralDatabaseBackup.contains(hostChannel))
				return;
			ConnectedPeers cp=initializedHooksWithCentralBackup.get(hostChannel);
			if (cp!=null) {
				ValidatedIDPerDistantHook v= validatedIDPerDistantHook.get(hostChannel);
				DatabaseHooksTable.Record r=getHooksTransactionsTable().getRecord("hostID", hostChannel);
				assert r!=null;
				long fileUTC=v.getFileUTCToTransfer(packageString, r.getLastValidatedDistantTransactionID());
				if (fileUTC!=Long.MIN_VALUE) {
					otherBackupDatabasePartsSynchronizingWithCentralDatabaseBackup.add(hostChannel);
					addNewDatabaseEvent(new AskForDatabaseBackupPartDestinedToCentralDatabaseBackup(packageString, getLocalHostID(), hostChannel, fileUTC-1));
				}
			}
		}

		private void received(EncryptedMetaDataFromCentralDatabaseBackup metaData) throws DatabaseException {
			ValidatedIDPerDistantHook v= validatedIDPerDistantHook.get(metaData.getHostSource());
			if (v==null)
				return;
			if (!metaData.getHostDestination().equals(getLocalHostID()))
				throw new DatabaseException("Invalid host destination");
			if (metaData.getHostDestination().equals(metaData.getHostSource()))
				return;
			try {

				v.addMetaData(metaData.getMetaData().getPackageString(), metaData.getMetaData().decodeMetaData(encryptionProfileProvider));
				Long m=lastValidatedTransactionIDFromCentralBackup.get(metaData.getHostSource());
				Long mn=v.getLastTransactionID(metaData.getMetaData().getPackageString());
				if (m==null || m<mn)
					lastValidatedTransactionIDFromCentralBackup.put(metaData.getHostSource(), mn);
				checkMetaDataUpdate(metaData.getHostSource());
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}

		private void initDistantBackupCenter(final DatabaseHooksTable.Record r, final long lastValidatedDistantTransactionID) throws DatabaseException {
			if (r==null)
				return;
			try {
				lockWrite();
				if (!validatedIDPerDistantHook.containsKey(r.getHostID()))
					validatedIDPerDistantHook.put(r.getHostID(), new ValidatedIDPerDistantHook());
				DecentralizedValue hostID=r.getHostID();
				if (initializedHooks.containsKey(hostID))
				{
					initDistantBackupCenter(hostID, lastValidatedDistantTransactionID);
				}
				else
				{
					initializedHooksWithCentralBackup.put(hostID, new ConnectedPeers(r));
					lastValidatedTransactionIDFromCentralBackup.put(r.getHostID(), lastValidatedDistantTransactionID);
					if (lastValidatedDistantTransactionID!=Long.MIN_VALUE) {
						validateLastSynchronization(hostID,
								Math.max(r.getLastValidatedLocalTransactionID(), lastValidatedDistantTransactionID));
					}
					else {
						synchronizeMetaData();
					}
				}
			} finally {
				unlockWrite();
			}
		}
		private void updateDistantBackupCenter(final DatabaseHooksTable.Record r, final long lastValidatedDistantTransactionID) throws DatabaseException {
			if (r==null)
				return;
			try {
				lockWrite();
				if (!validatedIDPerDistantHook.containsKey(r.getHostID()))
					validatedIDPerDistantHook.put(r.getHostID(), new ValidatedIDPerDistantHook());
				DecentralizedValue hostID=r.getHostID();
				if (initializedHooks.containsKey(hostID))
				{
					initDistantBackupCenter(hostID, lastValidatedDistantTransactionID);
				}
				else
				{
					lastValidatedTransactionIDFromCentralBackup.put(r.getHostID(), lastValidatedDistantTransactionID);
					if (lastValidatedDistantTransactionID!=Long.MIN_VALUE) {
						validateLastSynchronization(hostID,
								Math.max(r.getLastValidatedLocalTransactionID(), lastValidatedDistantTransactionID));
					}
					else {
						synchronizeMetaData();
					}
				}
			} finally {
				unlockWrite();
			}
		}
		public void checkForNewCentralBackupDatabaseEvent() throws DatabaseException {
			lockWrite();
			try {
				if (centralBackupInitialized) {
					for (Map.Entry<Package, Database> e : sql_database.entrySet()) {
						if (e.getValue().backupRestoreManager != null) {
							checkForNewBackupFilePartToSendToCentralDatabaseBackup(e.getKey());
						}
					}
				}

			}
			finally {
				unlockWrite();
			}


		}

		public boolean isInitializedWithCentralBackup(){
			lockRead();
			try {
				return centralBackupInitialized;
			}
			finally {
				unlockRead();
			}
		}

		public boolean isInitializedWithCentralBackup(DecentralizedValue hostID)
		{
			lockRead();
			try {
				return centralBackupInitialized && (initializedHooksWithCentralBackup.containsKey(hostID) || suspendedHooksWithCentralBackup.containsKey(hostID));
			}
			finally {
				unlockRead();
			}
		}

		@SuppressWarnings("UnusedReturnValue")
		public boolean synchronizeDatabasePackageWithCentralBackup(final Package _package) throws DatabaseException {
			lockWrite();
			try {
				if (_package.equals(DatabaseWrapper.class.getPackage()))
					throw new IllegalArgumentException();
				if (runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {
					@Override
					public Boolean run() throws Exception {
						DatabaseTable.Record r=getDatabaseTable().getRecord("databasePackageName", _package.getName());
						if (r==null)
							throw new DatabaseException("You must load first database package "+_package);
						if (r.isSynchronizedWithCentralDatabaseBackup())
							return false;
						else
						{
							getDatabaseTable().updateRecord(r, "synchronizedWithCentralDatabaseBackup", true);
							return true;
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
				}))
				{
					validateLastSynchronizationWithCentralDatabaseBackup(_package, Long.MIN_VALUE);
					return true;
				}
				else
					return false;

			}
			finally {
				unlockWrite();
			}
		}
		@SuppressWarnings("UnusedReturnValue")
		public boolean unsynchronizeDatabasePackageWithCentralBackup(final Package _package) throws DatabaseException {
			lockWrite();
			try {
				if (_package.equals(DatabaseWrapper.class.getPackage()))
					throw new IllegalArgumentException();
				if (runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {
					@Override
					public Boolean run() throws Exception {
						DatabaseTable.Record r=getDatabaseTable().getRecord("databasePackageName", _package.getName());
						if (r==null)
							throw new DatabaseException("You must load first database package "+_package);
						if (!r.isSynchronizedWithCentralDatabaseBackup())
							return false;
						else
						{
							getDatabaseTable().updateRecord(r, "synchronizedWithCentralDatabaseBackup", false);
							return true;
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
				}))
				{
					if (centralBackupInitialized)
						addNewDatabaseEvent(new DatabaseBackupToRemoveDestinedToCentralDatabaseBackup(getLocalHostID(), _package.getName()));
					return true;
				}
				else
					return false;

			}
			finally {
				unlockWrite();
			}
		}

		public Set<String> getDatabasePackagesToSynchronizeWithCentralBackup() throws DatabaseException {
			lockRead();
			try {
				List<DatabaseTable.Record> l= getDatabaseTable().getRecords();
				Set<String> res=new HashSet<>(l.size());
				for (DatabaseTable.Record r : l) {
					if (r.isSynchronizedWithCentralDatabaseBackup())
						res.add(r.getDatabasePackageName());
				}
				return res;
			}
			finally {
				unlockRead();
			}
		}

		private void initDistantBackupCenterForThisHostWithStringPackages(Map<String, Long> lastValidatedTransactionsUTC) throws DatabaseException {
			lockWrite();
			try {
				if (lastValidatedTransactionsUTC==null)
					throw new NullPointerException();
				centralBackupInitialized = true;
				Set<String> authorizedPackagesToBeSynchronizedWithCentralDatabaseBackup=getDatabasePackagesToSynchronizeWithCentralBackup();
				for (Map.Entry<String, Long> e : lastValidatedTransactionsUTC.entrySet()) {
					if (authorizedPackagesToBeSynchronizedWithCentralDatabaseBackup.contains(e.getKey())) {
						validateLastSynchronizationWithCentralDatabaseBackup(e.getKey(), e.getValue());
					}
					else
						addNewDatabaseEvent(new DatabaseBackupToRemoveDestinedToCentralDatabaseBackup(getLocalHostID(), e.getKey()));
				}
				for (String p : authorizedPackagesToBeSynchronizedWithCentralDatabaseBackup)
				{
					if (!lastValidatedTransactionsUTC.containsKey(p))
						validateLastSynchronizationWithCentralDatabaseBackup(p, Long.MIN_VALUE);
				}
			}
			finally {
				unlockWrite();
			}

		}
		private void received(InitialMessageComingFromCentralBackup initialMessageComingFromCentralBackup) throws DatabaseException {
			if (!initialMessageComingFromCentralBackup.getHostDestination().equals(getLocalHostID()))
				throw new IllegalArgumentException();
			initDistantBackupCenterForThisHostWithStringPackages(initialMessageComingFromCentralBackup.getLastValidatedTransactionsUTCForDestinationHost());
			try {
				for (Map.Entry<DecentralizedValue, Long> e : initialMessageComingFromCentralBackup.getLastValidatedIDPerHost(encryptionProfileProvider).entrySet()) {
					initDistantBackupCenter(e.getKey(), e.getValue());
				}
			}
			catch (IOException e)
			{
				throw DatabaseException.getDatabaseException(e);
			}
		}
		/*private void initDistantBackupCenterForThisHost(Map<Package, Long> lastValidatedTransactionsUTC) throws DatabaseException {
			Map<String, Long> m=new HashMap<>();
			for (Map.Entry<Package, Long> e : lastValidatedTransactionsUTC.entrySet())
				m.put(e.getKey().getName(), e.getValue());
			initDistantBackupCenterForThisHostWithStringPackages(m);
		}*/

		public void initConnexionWithDistantBackupCenter(AbstractSecureRandom random, EncryptionProfileProvider encryptionProfileProvider) throws DatabaseException {
			if (random==null)
				throw new NullPointerException();
			if (encryptionProfileProvider==null)
				throw new NullPointerException();
			lockWrite();
			try
			{
				if (!centralBackupInitialized)
				{
					this.random=random;
					this.encryptionProfileProvider=encryptionProfileProvider;
					addNewDatabaseEvent(new DistantBackupCenterConnexionInitialisation(getLocalHostID()));
				}
			}
			finally {
				unlockWrite();
			}
		}

		private void received(final BackupChannelInitializationMessageFromCentralDatabaseBackup message) throws DatabaseException {
			if (message == null)
				throw new NullPointerException();
			if (!message.getHostDestination().equals(getLocalHostID()))
				throw new IllegalArgumentException();
			try {
				initDistantBackupCenter(message.getHostChannel(), message.getLastValidatedID(encryptionProfileProvider));
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
		private void received(final BackupChannelUpdateMessageFromCentralDatabaseBackup message) throws DatabaseException {
			if (message == null)
				throw new NullPointerException();
			if (!message.getHostDestination().equals(getLocalHostID()))
				throw new IllegalArgumentException();
			try {
				updateDistantBackupCenter(message.getHostChannel(), message.getLastValidatedID(encryptionProfileProvider));
			} catch (IOException e) {
				throw DatabaseException.getDatabaseException(e);
			}
		}
		private void initDistantBackupCenter(final DecentralizedValue hostChannel, final long lastValidatedDistantTransactionID) throws DatabaseException {


			lockWrite();
			if (!centralBackupInitialized)
				throw new DatabaseException("Distant database backup must be initialized first with function initDistantBackupCenterForThisHost");

			try
			{

				if (initializedHooks.containsKey(hostChannel)) {
					if (suspendedHooksWithCentralBackup.containsKey(hostChannel))
						throw DatabaseException.getDatabaseException(
								new IllegalAccessException("hostID " + hostChannel + " already initialized !"));
					suspendedHooksWithCentralBackup.put(hostChannel, lastValidatedDistantTransactionID);
					return;
				}
			}
			finally {
				unlockWrite();
			}
			initDistantBackupCenter(getDatabaseHookRecord(hostChannel), lastValidatedDistantTransactionID);
		}
		DatabaseHooksTable.Record getDatabaseHookRecord(final DecentralizedValue hostChannel) throws DatabaseException {
			return runSynchronizedTransaction(
					new SynchronizedTransaction<DatabaseHooksTable.Record>() {

						@Override
						public DatabaseHooksTable.Record run() throws Exception {
							List<DatabaseHooksTable.Record> l = getHooksTransactionsTable()
									.getRecordsWithAllFields("hostID", hostChannel);
							DatabaseHooksTable.Record r = null;
							if (l.size() == 1)
								r = l.iterator().next();
							else if (l.size() > 1)
								throw new IllegalAccessError();
							if (r == null)
								throw new NullPointerException("Unknow host " + hostChannel);

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
		}
		private void updateDistantBackupCenter(final DecentralizedValue hostChannel, final long lastValidatedDistantTransactionID) throws DatabaseException {


			lockWrite();
			if (!centralBackupInitialized)
				throw new DatabaseException("Distant database backup must be initialized first with function initDistantBackupCenterForThisHost");

			try
			{
				if (initializedHooks.containsKey(hostChannel)) {
					return;
				}
			}
			finally {
				unlockWrite();
			}

			updateDistantBackupCenter(getDatabaseHookRecord(hostChannel), lastValidatedDistantTransactionID);
		}

		private void cancelEventToSend(DecentralizedValue peerDestination, boolean centralDatabaseBackupEvent) throws DatabaseException {

			for (Iterator<DatabaseEvent> it = events.iterator(); it.hasNext(); ) {
				DatabaseEvent de = it.next();
				if (de instanceof DatabaseEventToSend) {
					if (centralDatabaseBackupEvent) {
						if (!(de instanceof ChannelMessageDestinedToCentralDatabaseBackup))
							continue;

						ChannelMessageDestinedToCentralDatabaseBackup des = (ChannelMessageDestinedToCentralDatabaseBackup) de;
						if (des.getChannelHost().equals(peerDestination))
							it.remove();
					}
					else {
						if (!(de instanceof P2PDatabaseEventToSend))
							continue;
						P2PDatabaseEventToSend des = (P2PDatabaseEventToSend) de;
						if (des.getHostDestination().equals(peerDestination))
							it.remove();
					}

				}
			}
		}

		private void disconnectLocalHookFromItsCentralBackup() throws DatabaseException {
			lockWrite();
			try {
				addNewDatabaseEvent(new DisconnectCentralDatabaseBackup(getLocalHostID()));
				random=null;
				encryptionProfileProvider=null;
				centralBackupInitialized=false;
				backupDatabasePartsSynchronizingWithCentralDatabaseBackup.clear();
			}
			finally {
				unlockWrite();
			}
		}

		public void disconnectHookFromItsBackup(final DecentralizedValue hostID) throws DatabaseException {
			if (hostID == null)
				throw new NullPointerException("hostID");
			if (hostID.equals(getLocalHostID())) {
				disconnectAllHooksFromThereBackups();
				return;
			}

			try {
				lockWrite();
				validatedIDPerDistantHook.remove(hostID);
				ConnectedPeers peer = initializedHooksWithCentralBackup.remove(hostID);

				if (peer == null) {
					if (suspendedHooksWithCentralBackup.remove(hostID)==null)
						throw DatabaseException.getDatabaseException(
								new IllegalAccessException("hostID " + hostID + " has not been initialized !"));
				}
				else {
					cancelEventToSend(hostID, true);
				}
			}
			finally
			{
				unlockWrite();
			}
		}

		public void disconnectAllHooksFromThereBackups() throws DatabaseException {
			try {
				lockWrite();
				disconnectLocalHookFromItsCentralBackup();
				suspendedHooksWithCentralBackup.clear();


				for (ConnectedPeers peer : initializedHooksWithCentralBackup.values()) {
					cancelEventToSend(peer.hook.getHostID(), true);
				}
				initializedHooksWithCentralBackup.clear();
				validatedIDPerDistantHook.clear();
			}
			finally
			{
				unlockWrite();
			}
		}



		@SuppressWarnings("unlikely-arg-type")
		public void initHook(final DecentralizedValue hostID, final long lastValidatedTransactionID)
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
									.getRecordsWithAllFields("hostID", hostID);
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
				if (initializedHooks.containsKey(r.getHostID())) {
					throw DatabaseException.getDatabaseException(
							new IllegalAccessException("hostID " + hostID + " already initialized !"));
				}

				ConnectedPeers centralPeer=initializedHooksWithCentralBackup.remove(hostID);
				if (centralPeer!=null) {
					cancelEventToSend(hostID, true);
					Long l=lastValidatedTransactionIDFromCentralBackup.get(hostID);
					if (l!=null)
						suspendedHooksWithCentralBackup.put(hostID, l);
				}
				initializedHooks.put(hostID, new ConnectedPeers(r));

				validateLastSynchronization(hostID,
						Math.max(r.getLastValidatedLocalTransactionID(), lastValidatedTransactionID));
				if (notifier!=null && !r.concernsLocalDatabaseHost())
					notifier.hostConnected(hostID);
			}
			finally
			{
				unlockWrite();
			}
			
		}

		public Collection<DecentralizedValue> getDistantHostsIDs() throws DatabaseException {
			HashSet<DecentralizedValue> res=new HashSet<>();
			for (DatabaseHooksTable.Record hook : getHooksTransactionsTable().getRecords())
			{
				if (hook.concernsLocalDatabaseHost())
					continue;
				res.add(hook.getHostID());
			}
			return res;
		}

		public void received(DatabaseTransactionsIdentifiersToSynchronize d) throws DatabaseException {
			if (!isInitialized())
				throw new DatabaseException("The Synchronizer must be initialized (initLocalHostID function) !");

			getHooksTransactionsTable().validateDistantTransactions(d.getHostSource(),
					d.getLastDistantTransactionIdentifiers(), true);
		}

		public void received(P2PBigDatabaseEventToSend data, InputStreamGetter inputStream) throws DatabaseException {
			data.importFromInputStream(DatabaseWrapper.this, inputStream);
			synchronizedDataIfNecessary();
		}


		public void received(final LastIDCorrection idCorrection) throws DatabaseException {
			if (idCorrection == null)
				throw new NullPointerException();

			
			try {
				lockWrite();
				ConnectedPeers cp;
				cp = initializedHooks.get(idCorrection.getHostSource());
				if (cp == null)
					throw new DatabaseException("The host " + idCorrection.getHostSource() + " is not connected !");
			}
			finally
			{
				unlockWrite();
			}
			
			lastIDCorrection(idCorrection.getHostSource(), idCorrection.getLastValidatedTransaction() );
		}

		private void lastIDCorrection(final DecentralizedValue hostSource, final long lastValidatedTransaction) throws DatabaseException {
			if (hostSource.equals(getLocalHostID()))
				throw new IllegalArgumentException();
			runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

				@Override
				public Void run() throws Exception {
					DatabaseHooksTable.Record hook = getHooksTransactionsTable().getHook(hostSource);
					if (hook == null)
						throw new DatabaseException("Impossbile to find hook " + hostSource);

					getHooksTransactionsTable().updateRecord(hook, "lastValidatedDistantTransactionID",
							lastValidatedTransaction);
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


		private void validateLastSynchronizationWithCentralDatabaseBackup(Package _package, @SuppressWarnings("SameParameterValue") long lastValidatedTransactionUTC) throws DatabaseException {

			lockWrite();
			try {
				if(centralBackupInitialized)
				{
					Database d=sql_database.get(_package);
					if(d==null)
						return;
					validateLastSynchronizationWithCentralDatabaseBackup(_package, d, lastValidatedTransactionUTC);
				}
			}
			finally {
				unlockWrite();
			}
		}
		private void validateLastSynchronizationWithCentralDatabaseBackup(String _package, long lastValidatedTransactionUTC) throws DatabaseException {
			lockWrite();
			try {
				if(centralBackupInitialized)
				{
					for (Map.Entry<Package, Database> e : sql_database.entrySet())
					{
						if (e.getKey().getName().equals(_package))
						{
							validateLastSynchronizationWithCentralDatabaseBackup(e.getKey(), e.getValue(), lastValidatedTransactionUTC);
							return;
						}
					}
				}

			}
			finally {
				unlockWrite();
			}
		}
		private void validateLastSynchronizationWithCentralDatabaseBackup(Package _package, Database d, long lastValidatedTransactionUTC) throws DatabaseException {

	  		if (d.backupRestoreManager==null) {
				return;
			}
			long timeStamp;
			d.lastValidatedTransactionUTCForCentralBackup = lastValidatedTransactionUTC;
			if (lastValidatedTransactionUTC == Long.MIN_VALUE) {
				timeStamp = d.backupRestoreManager.getLastFileReferenceTimestampUTC();
			} else {
				timeStamp = d.backupRestoreManager.getNearestFileUTCFromGivenTimeNotIncluded(lastValidatedTransactionUTC);
			}

			if (timeStamp != Long.MIN_VALUE) {
				if (!backupDatabasePartsSynchronizingWithCentralDatabaseBackup.contains(_package.getName())) {
					//File f = d.backupRestoreManager.getFile(timeStamp);
					backupDatabasePartsSynchronizingWithCentralDatabaseBackup.add(_package.getName());
					addNewDatabaseEvent(d.backupRestoreManager.getEncryptedFilePartWithMetaData(getLocalHostID(), timeStamp, d.backupRestoreManager.isReference(timeStamp), random, encryptionProfileProvider));
					//addNewDatabaseEvent(new DatabaseBackupToIncorporateFromCentralDatabaseBackup(getLocalHostID(), getHooksTransactionsTable().getLocalDatabaseHost(), _package,timeStamp, b.isReference(timeStamp), b.extractTransactionInterval(f), f ));
				}
			}
			else
				checkAskForDatabaseBackupPartDestinedToCentralDatabaseBackup(_package.getName(), d);
		}

		void checkForNewBackupFilePartToSendToCentralDatabaseBackup(Package p) throws DatabaseException {
			lockWrite();
			try {
				if (this.centralBackupInitialized)
				{
					if (backupDatabasePartsSynchronizingWithCentralDatabaseBackup.contains(p.getName())) {
						return;
					}
					Database db=sql_database.get(p);
					if (db!=null){
						validateLastSynchronizationWithCentralDatabaseBackup(p,db, db.lastValidatedTransactionUTCForCentralBackup );
					}
				}
			}
			finally {
				unlockWrite();
			}
		}

		private void checkAskForDatabaseBackupPartDestinedToCentralDatabaseBackup(String packageString, Database d) throws DatabaseException {
			if (d.lastValidatedTransactionUTCForCentralBackup>Long.MIN_VALUE)
			{
				long lts=d.backupRestoreManager.getLastTransactionUTCInMS();
				if (lts<d.lastValidatedTransactionUTCForCentralBackup) {

					addNewDatabaseEvent(new AskForDatabaseBackupPartDestinedToCentralDatabaseBackup(packageString, getLocalHostID(), lts));
				}
			}
		}

		private void received(EncryptedBackupPartComingFromCentralDatabaseBackup backupPart) throws DatabaseException {
			if (!backupPart.getHostDestination().equals(getLocalHostID()))
			{
				throw new DatabaseException("Invalid host destination");
			}
			if (backupPart.getHostDestination()==backupPart.getHostSource())
			{
				String pname = backupPart.getMetaData().getPackageString();
				if (getDatabasePackagesToSynchronizeWithCentralBackup().contains(pname)) {
					for (Map.Entry<Package, Database> e : sql_database.entrySet()) {
						if (e.getKey().getName().equals(pname)) {
							Database d = e.getValue();
							d.backupRestoreManager.importEncryptedBackupPartComingFromCentralDatabaseBackup(backupPart, encryptionProfileProvider, false);
							checkAskForDatabaseBackupPartDestinedToCentralDatabaseBackup(e.getKey().getName(), d);
							break;
						}
					}

				} else {
					try {
						backupPart.getPartInputStream().close();
					} catch (IOException e) {
						throw DatabaseException.getDatabaseException(e);
					}
				}
			}
			else {
				try (RandomCacheFileOutputStream out=RandomCacheFileCenter.getSingleton().getNewBufferedRandomCacheFileOutputStream(true, RandomFileOutputStream.AccessMode.READ_AND_WRITE)){
					EncryptionTools.decode(encryptionProfileProvider, backupPart.getPartInputStream(), out);
					out.flush();
					getDatabaseTransactionsPerHostTable().alterDatabase(backupPart.getMetaData().getPackageString(), backupPart.getHostSource(), out.getRandomInputStream());
					otherBackupDatabasePartsSynchronizingWithCentralDatabaseBackup.remove(backupPart.getHostSource());
					checkAskForEncryptedBackupFilePart(backupPart.getHostSource(), backupPart.getMetaData().getPackageString());
				} catch (IOException e) {
					throw DatabaseException.getDatabaseException(e);
				}

			}
		}

		private void received(EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup confirmation) throws DatabaseException {
			if (!confirmation.getHostDestination().equals(getLocalHostID()))
				throw DatabaseException.getDatabaseException(new MessageExternalizationException(Integrity.FAIL));
			this.backupDatabasePartsSynchronizingWithCentralDatabaseBackup.remove(confirmation.getPackageString());
			validateLastSynchronizationWithCentralDatabaseBackup(confirmation.getPackageString(), confirmation.getLastTransactionUTC());
		}

		public void received(DatabaseEventToSend data) throws DatabaseException {
			if (data instanceof P2PDatabaseEventToSend)
				received((P2PDatabaseEventToSend)data);
			else if (data instanceof MessageComingFromCentralDatabaseBackup)
				received((MessageComingFromCentralDatabaseBackup)data);
		}
		private void received(P2PDatabaseEventToSend data) throws DatabaseException {
			if (data instanceof DatabaseTransactionsIdentifiersToSynchronize)
				received((DatabaseTransactionsIdentifiersToSynchronize) data);

			else if (data instanceof TransactionConfirmationEvents) {
				TransactionConfirmationEvents tce=(TransactionConfirmationEvents)data;
				if (isInitialized(tce.getHostSource()))
					validateLastSynchronization(tce.getHostSource(),
							tce.getLastValidatedTransaction());
				else
					initHook(tce.getHostSource(),
							tce.getLastValidatedTransaction());
			} else if (data instanceof HookAddRequest) {
				receivedHookAddRequest((HookAddRequest) data);
			} else if (data instanceof LastIDCorrection) {
				received((LastIDCorrection) data);
			}
		}
		private void received(MessageComingFromCentralDatabaseBackup data) throws DatabaseException {
			if (data instanceof EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup)
			{
				received((EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup)data);
			} else if (data instanceof EncryptedBackupPartComingFromCentralDatabaseBackup)
			{
				received((EncryptedBackupPartComingFromCentralDatabaseBackup)data);
			}
			else if (data instanceof EncryptedMetaDataFromCentralDatabaseBackup)
			{
				received((EncryptedMetaDataFromCentralDatabaseBackup)data);
			}
			else if (data instanceof BackupChannelUpdateMessageFromCentralDatabaseBackup)
			{
				if (data instanceof BackupChannelInitializationMessageFromCentralDatabaseBackup)
				{
					received((BackupChannelInitializationMessageFromCentralDatabaseBackup)data);
				}
				else
					received((BackupChannelUpdateMessageFromCentralDatabaseBackup)data);
			}
			else if (data instanceof InitialMessageComingFromCentralBackup)
			{
				received((InitialMessageComingFromCentralBackup)data);
			}
		}





	}

	public enum SynchronizationAnomalyType {
		RECORD_TO_REMOVE_NOT_FOUND, RECORD_TO_REMOVE_HAS_DEPENDENCIES, RECORD_TO_UPDATE_NOT_FOUND, RECORD_TO_UPDATE_HAS_INCOMPATIBLE_PRIMARY_KEYS, RECORD_TO_UPDATE_HAS_DEPENDENCIES_NOT_FOUND, RECORD_TO_ADD_ALREADY_PRESENT, RECORD_TO_ADD_HAS_INCOMPATIBLE_PRIMARY_KEYS, RECORD_TO_ADD_HAS_DEPENDENCIES_NOT_FOUND,
	}


	/*public static class TransactionsInterval implements SecureExternalizable, Comparable<TransactionsInterval>
	{
		private long startIncludedTransactionID;
		private long endIncludedTransactionID;

		TransactionsInterval(long startIncludedTransactionID, long endIncludedTransactionID) {
			if (endIncludedTransactionID<startIncludedTransactionID)
				throw new IllegalArgumentException();
			this.startIncludedTransactionID = startIncludedTransactionID;
			this.endIncludedTransactionID = endIncludedTransactionID;
		}

		public void setStartIncludedTransactionID(long startIncludedTransactionID) {
			this.startIncludedTransactionID = startIncludedTransactionID;
		}

		public void setEndIncludedTransactionID(long endIncludedTransactionID) {
			this.endIncludedTransactionID = endIncludedTransactionID;
		}

		public long getStartIncludedTransactionID() {
			return startIncludedTransactionID;
		}

		public long getEndIncludedTransactionID() {
			return endIncludedTransactionID;
		}

		@Override
		public int getInternalSerializedSize() {
			return 16;
		}

		@Override
		public void writeExternal(SecuredObjectOutputStream out) throws IOException {
			out.writeLong(startIncludedTransactionID);
			out.writeLong(endIncludedTransactionID);
		}

		@Override
		public void readExternal(SecuredObjectInputStream in) throws IOException {
			startIncludedTransactionID=in.readLong();
			endIncludedTransactionID=in.readLong();
			if (endIncludedTransactionID<startIncludedTransactionID)
				throw new MessageExternalizationException(Integrity.FAIL);
		}

		@Override
		public int compareTo(TransactionsInterval o) {
			if (startIncludedTransactionID<o.startIncludedTransactionID)
				return -1;
			else if (startIncludedTransactionID>o.startIncludedTransactionID)
				return 1;
			else return Long.compare(endIncludedTransactionID, o.endIncludedTransactionID);
		}
	}*/




	public static abstract class AbstractTransactionConfirmationEvents extends DatabaseEvent implements SecureExternalizable{

		protected DecentralizedValue hostIDSource;
		protected DecentralizedValue hostIDDestination;
		protected long lastValidatedTransaction;

		@Override
		public int getInternalSerializedSize() {
			return SerializationTools.getInternalSize(hostIDSource,0)+SerializationTools.getInternalSize(hostIDDestination,0);
		}

		@Override
		public void writeExternal(SecuredObjectOutputStream out) throws IOException {
			out.writeObject(hostIDSource, false);
			out.writeObject(hostIDDestination, false);
			out.writeLong(lastValidatedTransaction);
		}

		@Override
		public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
			hostIDSource=in.readObject(false, DecentralizedValue.class);
			hostIDDestination=in.readObject(false, DecentralizedValue.class);
			lastValidatedTransaction=in.readLong();
		}
		@SuppressWarnings("unused")
		AbstractTransactionConfirmationEvents()
		{

		}
		AbstractTransactionConfirmationEvents(DecentralizedValue _hostIDSource,DecentralizedValue _hostDestination,
									  long _lastValidatedTransaction) {
			super();
			hostIDSource = _hostIDSource;
			hostIDDestination = _hostDestination;
			lastValidatedTransaction = _lastValidatedTransaction;
		}

		public DecentralizedValue getHostDestination() {
			return hostIDDestination;
		}


		public DecentralizedValue getHostSource() {
			return hostIDSource;
		}

		public long getLastValidatedTransaction() {
			return lastValidatedTransaction;
		}

	}

	public static class TransactionConfirmationEvents extends AbstractTransactionConfirmationEvents implements P2PDatabaseEventToSend, SecureExternalizable {





		@SuppressWarnings("unused")
		TransactionConfirmationEvents()
		{

		}
		TransactionConfirmationEvents(DecentralizedValue _hostIDSource, DecentralizedValue _hostIDDestination,
				long _lastValidatedTransaction) {
			super(_hostIDSource,_hostIDDestination, _lastValidatedTransaction);
			hostIDDestination = _hostIDDestination;
		}




	}

	public static class DatabaseEventsToSynchronizeP2P extends AbstractDatabaseEventsToSynchronizeP2P {
		private long lastTransactionIDIncluded;
		int maxEventsRecords;
		@SuppressWarnings("unused")
		DatabaseEventsToSynchronizeP2P() {
		}

		DatabaseEventsToSynchronizeP2P(DecentralizedValue hostIDSource, Record hook, long lastTransactionIDIncluded, int maxEventsRecords) {
			super(hostIDSource, hook);
			this.lastTransactionIDIncluded = lastTransactionIDIncluded;
			this.maxEventsRecords = maxEventsRecords;
		}
		@Override
		public void writeExternal(SecuredObjectOutputStream out) throws IOException {
			super.writeExternal(out);
			out.writeLong(lastTransactionIDIncluded);
			out.writeInt(maxEventsRecords);
		}
		@Override
		public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
			super.readExternal(in);
			lastTransactionIDIncluded=in.readLong();
			maxEventsRecords=in.readInt();
			if (maxEventsRecords<1 || maxEventsRecords>MAX_TRANSACTIONS_TO_SYNCHRONIZE_AT_THE_SAME_TIME)
				throw new MessageExternalizationException(Integrity.FAIL_AND_CANDIDATE_TO_BAN);
		}
		@Override
		public void importFromInputStream(DatabaseWrapper wrapper, final InputStreamGetter inputStream)
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
			return wrapper.runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {

				@Override
				public Boolean run() throws Exception {
					int number = wrapper.getDatabaseTransactionsPerHostTable().exportTransactions(outputStreamGetter.initOrResetOutputStream(), hookID,
							maxEventsRecords);
					return number > 0;
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

		public boolean tryToMerge(DatabaseEventsToSynchronizeP2P dest) {
			if (hookID == dest.hookID) {
				lastTransactionIDIncluded = Math.max(lastTransactionIDIncluded, dest.lastTransactionIDIncluded);
				return true;
			} else
				return false;
		}
	}
	public static abstract class AbstractDatabaseEventsToSynchronizeP2P extends DatabaseEvent implements P2PBigDatabaseEventToSend, SecureExternalizable {
		protected transient DatabaseHooksTable.Record hook;
		protected int hookID;
		protected DecentralizedValue hostIDSource, hostIDDestination;


		@SuppressWarnings("unused")
		AbstractDatabaseEventsToSynchronizeP2P() {

		}

		@Override
		public int getInternalSerializedSize() {
			return 16+SerializationTools.getInternalSize(hostIDSource, 0)+SerializationTools.getInternalSize(hostIDDestination, 0);
		}

		@Override
		public void writeExternal(SecuredObjectOutputStream out) throws IOException {
			out.writeInt(hookID);
			out.writeObject(hostIDSource, false);
			out.writeObject(hostIDDestination, false);
		}

		@Override
		public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
			hookID=in.readInt();
			hostIDSource=in.readObject(false, DecentralizedValue.class);
			hostIDDestination=in.readObject(false, DecentralizedValue.class);
		}


		AbstractDatabaseEventsToSynchronizeP2P(DecentralizedValue hostIDSource, DatabaseHooksTable.Record hookDestination) {
			this.hook = hookDestination;
			this.hookID = hookDestination.getID();
			this.hostIDDestination = hookDestination.getHostID();
			this.hostIDSource = hostIDSource;
		}

		@Override
		public DecentralizedValue getHostDestination() {
			return hostIDDestination;
		}

		@Override
		public DecentralizedValue getHostSource() {
			return hostIDSource;
		}




	}

	private Lock getLocker() {
		synchronized (DatabaseWrapper.class) {
			String f = database_identifier;

			Lock rwl = lockers.get(f);
			if (rwl == null) {

				rwl = new ReentrantLock();
				lockers.put(f, rwl);
				number_of_shared_lockers.put(f, 1);
			} else
				number_of_shared_lockers.put(f, number_of_shared_lockers.get(f) + 1);

			return rwl;
		}
	}

	DatabaseTransactionEventsTable getTransactionsTable() throws DatabaseException {
		if (transactionTable == null)
			transactionTable = getTableInstance(DatabaseTransactionEventsTable.class);
		return transactionTable;
	}

	DatabaseHooksTable getHooksTransactionsTable() throws DatabaseException {
		if (databaseHooksTable == null)
			databaseHooksTable = getTableInstance(DatabaseHooksTable.class);
		return databaseHooksTable;
	}

	DatabaseTable getDatabaseTable() throws DatabaseException {
		if (databaseTable == null)
			databaseTable = getTableInstance(DatabaseTable.class);
		return databaseTable;
	}

	DatabaseTransactionsPerHostTable getDatabaseTransactionsPerHostTable() throws DatabaseException {
		if (databaseTransactionsPerHostTable == null)
			databaseTransactionsPerHostTable = getTableInstance(
					DatabaseTransactionsPerHostTable.class);
		return databaseTransactionsPerHostTable;
	}

	DatabaseEventsTable getDatabaseEventsTable() throws DatabaseException {
		if (databaseEventsTable == null)
			databaseEventsTable = getTableInstance(DatabaseEventsTable.class);
		return databaseEventsTable;
	}

	DatabaseDistantTransactionEvent getDatabaseDistantTransactionEvent() throws DatabaseException {
		if (databaseDistantTransactionEvent == null)
			databaseDistantTransactionEvent = getTableInstance(
					DatabaseDistantTransactionEvent.class);
		return databaseDistantTransactionEvent;
	}

	DatabaseTransactionEventsTable getDatabaseTransactionEventsTable() throws DatabaseException {
		if (databaseTransactionEventsTable == null)
			databaseTransactionEventsTable = getTableInstance(
					DatabaseTransactionEventsTable.class);
		return databaseTransactionEventsTable;
	}

	IDTable getTransactionIDTable() throws DatabaseException {
		if (transactionIDTable == null)
			transactionIDTable = getTableInstance(IDTable.class);
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
	protected abstract String getNotCachedKeyword();

	public abstract boolean supportCache();

	public abstract boolean supportNoCacheParam();

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
						DatabaseWrapper.lockers.remove(database_identifier);
						Integer i=DatabaseWrapper.number_of_shared_lockers.get(database_identifier);
						if (i!=null) {
							int v = i - 1;
							if (v == 0)
								DatabaseWrapper.number_of_shared_lockers.remove(database_identifier);
							else if (v > 0)
								DatabaseWrapper.number_of_shared_lockers.put(database_identifier, v);
							else
								throw new IllegalAccessError();
						}
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

	@SuppressWarnings("deprecation")
    @Override
	public void finalize() {
		try {
			close();
		} catch (Exception ignored) {

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

	protected int getMaxKeySize()
	{
		return Table.maxPrimaryKeysSizeBytes;
	}



	class Session {
		private Connection connection;
		private final Thread thread;
		private final long threadID;
		private final Set<Table<?>> memoryTablesToRefresh;
		private final HashMap<Package, BackupRestoreManager.Transaction> backupManager=new HashMap<>();
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

		private BackupRestoreManager.Transaction getBackupManagerAndStartTransactionIfNecessary(Package p, boolean transactionToSynchronizeFromCentralDatabaseBackup) throws DatabaseException {
			if (!backupManager.containsKey(p)) {
				BackupRestoreManager brm = getBackupRestoreManager(p);

				if (brm!=null)
				{
					BackupRestoreManager.Transaction res=brm.startTransaction(transactionToSynchronizeFromCentralDatabaseBackup);
					backupManager.put(p, res);
					return res;
				}
				else {
					backupManager.put(p, null);
					return null;
				}
			}
			else
				return backupManager.get(p);
		}

		@SuppressWarnings("UnusedReturnValue")
        boolean addEvent(Table<?> table, TableEvent<?> de, boolean applySynchro) throws DatabaseException {
			if (table == null)
				throw new NullPointerException("table");
			if (de == null)
				throw new NullPointerException("de");
			Package p = table.getClass().getPackage();

			BackupRestoreManager.Transaction backupTransaction=getBackupManagerAndStartTransactionIfNecessary(p, applySynchro);
			if (backupTransaction!=null)
				backupTransaction.backupRecordEvent(table, de);
			if (!applySynchro)
				return false;

			if (p.equals(DatabaseWrapper.class.getPackage()))
				return false;
			if (!table.supportSynchronizationWithOtherPeers())
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
		protected final AtomicInteger actualTransactionEventsNumber = new AtomicInteger(0);
		protected volatile boolean eventsStoredIntoMemory = true;
		protected final AtomicInteger actualPosition = new AtomicInteger(0);
		protected boolean transactionToSynchronize=false;

		private void checkIfEventsMustBeStoredIntoDisk() throws DatabaseException {

			if (eventsStoredIntoMemory && actualTransactionEventsNumber.get() >= getMaxTransactionEventsKeepedIntoMemory()) {
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
						idCounterForTmpTransactions.decrementAndGet(), getTransactionUTC(concernedDatabase.getName()), concernedDatabase.getName());
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
							if (_record.getConcernedTable().equals(originalEvent.getConcernedTable())
							&& Arrays.equals(_record.getConcernedSerializedPrimaryKey(),
									originalEvent.getConcernedSerializedPrimaryKey())
									) {
								if (event.getType() == DatabaseEventType.UPDATE
										&& _record.getType() == DatabaseEventType.ADD.getByte()) {
									eventr = new DatabaseEventsTable.Record(transaction.transaction,
                                            new TableEvent<>(event.getID(), DatabaseEventType.ADD, null,
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
										t.deserializeFields(dr, _record.getConcernedSerializedNewForeignKey(), false,
												true, false);
										for (ForeignKeyFieldAccessor fa : t.getForeignKeysFieldAccessors()) {
											if (fa.getPointedTable().getClass().equals(table.getClass())) {
												try (RandomByteArrayOutputStream baos = new RandomByteArrayOutputStream()) {
													fa.serialize(baos, dr);
													baos.flush();
													if (Arrays.equals(baos.getBytes(),
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
							Set<DecentralizedValue> hosts = event.getHostsDestination();
							if (hosts != null)
								transaction.concernedHosts.addAll(hosts);
							eventr.setPosition(actualPosition.getAndIncrement());
							transaction.events.add(eventr);
							nb.incrementAndGet();
						}

					} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
							| InvocationTargetException e) {
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
                                                        new TableEvent<>(event.getID(),
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
								StringBuilder sb = new StringBuilder();
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
									sb.append("concernedTable=%").append(varName);
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
															t.deserializeFields(dr,
																	_record.getConcernedSerializedNewForeignKey(),
																	false, true, false);
															for (ForeignKeyFieldAccessor fa : t
																	.getForeignKeysFieldAccessors()) {
																if (fa.getPointedTable().getClass()
																		.equals(table.getClass())) {
																	try (RandomByteArrayOutputStream baos = new RandomByteArrayOutputStream()) {
																		fa.serialize(baos, dr);
																		baos.flush();
																		if (Arrays.equals(baos.getBytes(),
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
								Set<DecentralizedValue> hosts = event.getHostsDestination();
								if (hosts != null)
									transaction.concernedHosts.addAll(hosts);
                            eventr.get().setPosition(actualPosition.getAndIncrement());
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
				transaction.eventsNumber.addAndGet(nb.get());
				actualTransactionEventsNumber.addAndGet(nb.get());
			}
		}

		void cancelTmpTransaction() throws DatabaseException {
			resetTmpTransaction(true, false);
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
			return actualPosition.get();
		}



		public Map<Package, Long> getBackupPositions() throws DatabaseException {
			HashMap<Package, Long> hashMap=new HashMap<>();
			for (Map.Entry<Package, BackupRestoreManager.Transaction> e : this.backupManager.entrySet())
			{
				if (e.getValue()!=null)
					hashMap.put(e.getKey(), e.getValue().getBackupPosition());
			}
			return hashMap;
		}

		void cancelTmpTransactionEvents(final int position, Map<Package, Long> backupPositions) throws DatabaseException {
			if (position == actualPosition.get())
				return;
			for (Iterator<Map.Entry<Package, BackupRestoreManager.Transaction>> it = this.backupManager.entrySet().iterator();it.hasNext();)
			{
				Map.Entry<Package, BackupRestoreManager.Transaction> e=it.next();
				if (e.getValue()!=null) {
					if (backupPositions.containsKey(e.getKey())) {
						e.getValue().cancelTransaction(backupPositions.get(e.getKey()));
					} else {
						e.getValue().cancelTransaction();
						it.remove();
					}
				}
			}
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
					actualTransactionEventsNumber.addAndGet(nb);
					t.eventsNumber.addAndGet(-nb);
				}

			} else {

				runSynchronizedTransaction(new SynchronizedTransaction<Void>() {
					boolean alreadyDone=false;
					@Override
					public Void run() throws Exception {
						alreadyDone=true;
						for (TransactionPerDatabase t : temporaryTransactions.values()) {
							int nb = (int) getDatabaseEventsTable().removeRecords(
									"transaction=%transaction AND position>=%pos", "transaction", t.transaction, "pos",
                                    position);
							actualTransactionEventsNumber.addAndGet(nb);
							t.eventsNumber.addAndGet(-nb);
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

					}
				});

			}

		}

		private void cancelBackupTransaction() throws DatabaseException {
			for (BackupRestoreManager.Transaction t : backupManager.values())
			{
				if (t!=null)
					t.cancelTransaction();
			}
			backupManager.clear();

		}

		void resetTmpTransaction(boolean resetBackup, boolean transactionToSynchronize) throws DatabaseException {
			if (resetBackup)
				cancelBackupTransaction();
			temporaryTransactions.clear();
			actualTransactionEventsNumber.set(0);
			eventsStoredIntoMemory = true;
			this.transactionToSynchronize=transactionToSynchronize;
		}

		long getTransactionUTC(String concernedDatabasePackage)
		{
			for (Map.Entry<Package, BackupRestoreManager.Transaction> e: backupManager.entrySet()) {
				if (e.getKey().toString().equals(concernedDatabasePackage))
					return e.getValue().getTransactionUTC();
			}
			return System.currentTimeMillis();
		}

		boolean validateTmpTransaction() throws DatabaseException {
			final HashMap<Package, Long> transactionsID=new HashMap<>();
			try {
				boolean res=transactionToSynchronize;
				if (res) {
					if (eventsStoredIntoMemory) {

						res = runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {
							final boolean alreadyDone = false;

							@Override
							public Boolean run() throws Exception {
								final AtomicBoolean transactionOK = new AtomicBoolean(false);

								for (final Map.Entry<Package, TransactionPerDatabase> e : temporaryTransactions.entrySet()) {
									final TransactionPerDatabase t = e.getValue();
									if (t.eventsNumber.get() > 0) {
										final AtomicReference<DatabaseTransactionEventsTable.Record> finalTR = new AtomicReference<>(
												null);
										/*
										 * final ArrayList<AbstractDecentralizedID> excludedHooks=new ArrayList<>();
										 * long
										 * previousLastTransactionID=getTransactionIDTable().getLastTransactionID();
										 * final AtomicBoolean hasIgnoredHooks=new AtomicBoolean(false);
										 */

										getHooksTransactionsTable().getRecords(new Filter<Record>() {

											@Override
											public boolean nextRecord(Record _record)
													throws DatabaseException {
												if (!_record.concernsLocalDatabaseHost()) {
													if (_record.isConcernedByDatabasePackage(
															t.transaction.concernedDatabasePackage)
															&& (t.concernedHosts == null || t.concernedHosts.isEmpty()
															|| t.concernedHosts.contains(_record.getHostID()))) {

														if (finalTR.get() == null) {
															//long timeUTC=getTransactionUTC(t.transaction.concernedDatabasePackage);
															finalTR.set(t.transaction = getDatabaseTransactionEventsTable()
																	.addRecord(new DatabaseTransactionEventsTable.Record(
																			getTransactionIDTable()
																					.getAndIncrementTransactionID(),t.transaction.timeUTC,
																			t.transaction.concernedDatabasePackage,
																			t.concernedHosts)));
															transactionsID.put(e.getKey(), t.transaction.id);
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
								resetTmpTransaction(false, true);
								return transactionOK.get();
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
							}

						});

					} else {

						res = runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {
							boolean alreadyDone = false;

							@Override
							public Boolean run() throws Exception {
								alreadyDone=true;
								final AtomicBoolean transactionOK = new AtomicBoolean(false);
								for (final Map.Entry<Package, TransactionPerDatabase> e : temporaryTransactions.entrySet()) {
									final TransactionPerDatabase t = e.getValue();
									if (t.eventsNumber.get() > 0) {
										final Reference<DatabaseTransactionEventsTable.Record> finalTR = new Reference<>(
												);
										/*
										 * final ArrayList<AbstractDecentralizedID> excludedHooks=new ArrayList<>();
										 * long
										 * previousLastTransactionID=getTransactionIDTable().getLastTransactionID();
										 * final AtomicBoolean hasIgnoredHooks=new AtomicBoolean(false);
										 */
										getHooksTransactionsTable().getRecords(new Filter<Record>() {

											@Override
											public boolean nextRecord(Record _record)
													throws DatabaseException {
												if (!_record.concernsLocalDatabaseHost()) {
													if (_record.isConcernedByDatabasePackage(
															t.transaction.concernedDatabasePackage)
															&& (t.concernedHosts == null || t.concernedHosts.isEmpty()
															|| t.concernedHosts.contains(_record.getHostID()))) {
														// excludedHooks.add(_record.getHostID());
														if (finalTR.get() == null) {
															//long timeUTC=getTransactionUTC(t.transaction.concernedDatabasePackage);
															DatabaseTransactionEventsTable.Record te;
															finalTR.set(te = getDatabaseTransactionEventsTable()
																	.addRecord(new DatabaseTransactionEventsTable.Record(
																			getTransactionIDTable()
																					.getAndIncrementTransactionID(),t.transaction.timeUTC,
																			t.transaction.concernedDatabasePackage,
																			t.concernedHosts)));
															transactionsID.put(e.getKey(), te.id);
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
								resetTmpTransaction(false, true);
								return transactionOK.get();
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

							}
						});

					}
				}
				if (res) {
					for (Map.Entry<Package, BackupRestoreManager.Transaction> e : backupManager.entrySet()) {

						BackupRestoreManager.Transaction t = e.getValue();
						if (t != null) {
							long tid=transactionsID.get(e.getKey());
							t.validateTransaction(tid);
						}
					}

				}
				else
				{
					for (Map.Entry<Package, BackupRestoreManager.Transaction> e : backupManager.entrySet()) {
						BackupRestoreManager.Transaction t = e.getValue();
						if (t != null)
							t.cancelTransaction();
					}
				}
				/*if (min==Long.MIN_VALUE)
					getTransactionIDTable().setTransactionUTCToBeCandidateForLastValue(System.currentTimeMillis());
				else
					getTransactionIDTable().setTransactionUTCToBeCandidateForLastValue(min);*/
				backupManager.clear();
				return res;
			}
			catch (DatabaseException e)
			{
				for (Map.Entry<Package, BackupRestoreManager.Transaction> element: backupManager.entrySet())
				{
					BackupRestoreManager.Transaction t=element.getValue();
					if (t!=null)
						t.cancelTransaction();
				}
				throw e;
			}
		}

	}

	private static class TransactionPerDatabase {
		DatabaseTransactionEventsTable.Record transaction;
		AtomicInteger eventsNumber;
		final Set<DecentralizedValue> concernedHosts;
		final ArrayList<DatabaseEventsTable.Record> events;

		TransactionPerDatabase(DatabaseTransactionEventsTable.Record transaction, int maxEventsNumberKeepedIntoMemory) {
			if (transaction == null)
				throw new NullPointerException();
			this.transaction = transaction;
			this.eventsNumber = new AtomicInteger(0);
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
            if (c==null)
            	throw new DatabaseException("Invalid connection", new NullPointerException());
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
				if (closed || alwaysDeconectAfterOnTransaction) {
					if (!c.getConnection().isClosed())
						closeConnection(c.getConnection(), true);
				}

				for (Iterator<Session> it = threadPerConnection.iterator(); it.hasNext();) {
					Session s = it.next();

					if (!s.getThread().isAlive()) {
						if (!s.getConnection().isClosed() && !s.getConnection().isClosed())
							closeConnection(s.getConnection(), alwaysDeconectAfterOnTransaction);

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

	@SuppressWarnings("UnusedReturnValue")
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
		return (Boolean) runTransaction(new Transaction() {

            @Override
            public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
                try {
                    return getConnectionAssociatedWithCurrentThread().getConnection().getMetaData()
                            .supportsTransactions();
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
        }, true);
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

	private final AtomicLong savePoint = new AtomicLong(0);

	protected String generateSavePointName() {
		return "Savepoint" + (savePoint.incrementAndGet());
		/*
		 * HexadecimalEncodingAlgorithm h=new HexadecimalEncodingAlgorithm();
		 * StringBuffer sb=new StringBuffer("S"); h.convertToCharacters(new
		 * DecentralizedIDGenerator().getBytes(), sb); return sb.toString();
		 */
	}
	
	protected abstract boolean isSerializationException(SQLException e) throws DatabaseException;
	protected abstract boolean isTransactionDeadLockException(SQLException e) throws DatabaseException;
	protected abstract boolean isDisconnectionException(SQLException e) throws DatabaseException;

	private boolean needsToLock()
	{
		return !isThreadSafe() || hasOnePeerSyncronized; 
	}

	private void checkAutoIncrementTable() throws DatabaseException {
		try {
			if ((!supportMultipleAutoPrimaryKeys() || !supportSingleAutoPrimaryKeys()) && !doesTableExists(DatabaseWrapper.AUTOINCREMENT_TABLE)) {
				try (Statement pst = getConnectionAssociatedWithCurrentThread().getConnection()
						.createStatement()) {
					pst.execute("CREATE TABLE `" + DatabaseWrapper.AUTOINCREMENT_TABLE +
							"` (TABLE_ID INT NOT NULL, TABLE_VERSION INT NOT NULL, AI BIGINT NOT NULL, CONSTRAINT TABLE_ID_PK PRIMARY KEY(TABLE_ID, TABLE_VERSION))"
							+ getPostCreateTable(null) + getSqlComma());
				}
			}
		}
		catch (Exception e) {
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}

	}
	protected long getNextAutoIncrement(Table<?> table, FieldAccessor fa) throws DatabaseException {
		return getNextAutoIncrement(table.getTableID(), table.getDatabaseVersion(), fa.getStartValue());
	}
	protected long getNextAutoIncrement(int tableID, int databaseVersion, long startValue) throws DatabaseException {
		try {
			boolean insert=false;
			long res;
			try(PreparedStatement pst = getConnectionAssociatedWithCurrentThread().getConnection()
					.prepareStatement("SELECT AI FROM `" + DatabaseWrapper.AUTOINCREMENT_TABLE + "` WHERE TABLE_ID='"
							+ tableID + "' AND TABLE_VERSION='"+databaseVersion+"'"+ getSqlComma())) {
				ResultSet rs = pst.executeQuery();

				if (rs.next()) {
					res = rs.getLong(1);
				} else {
					insert=true;
					res = startValue;
				}
			}
			if (insert) {
				try (PreparedStatement pst = getConnectionAssociatedWithCurrentThread().getConnection()
						.prepareStatement("INSERT INTO `" + DatabaseWrapper.AUTOINCREMENT_TABLE + "`(TABLE_ID, TABLE_VERSION, AI) VALUES('"
								+ tableID+"', '" +databaseVersion+"', '"+ (res + 1) + "')" + getSqlComma())) {
					pst.executeUpdate();
				}
			}
			else
			{
				try (PreparedStatement pst = getConnectionAssociatedWithCurrentThread().getConnection()
						.prepareStatement("UPDATE `" + DatabaseWrapper.AUTOINCREMENT_TABLE + "` SET AI='"+(res+1)+"' WHERE TABLE_ID='"
								+ tableID + "' AND TABLE_VERSION='"+databaseVersion+"'" + getSqlComma())) {
					pst.executeUpdate();
				}
			}
			return res;

		} catch (SQLException e) {
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}
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
				boolean deconnexionException=false;
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
						cw.connection.resetTmpTransaction(true, true);
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
							if (t instanceof SQLException)
							{
								SQLException sqlE=(SQLException)t;
								if (isTransactionDeadLockException(sqlE))
								{
									retry=true;
								}
                                else if (isSerializationException(sqlE)) {
                                    retry = true;
                                }
								else if (isDisconnectionException(sqlE))
								{
									if (!deconnexionException) {
										retry = true;
										deconnexionException = true;
									}
								}


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

							if (!retry) {
								t = se.getCause();

								while (t != null) {
									if (t instanceof SQLException) {
										SQLException sqlE=(SQLException)t;
										if (isTransactionDeadLockException(sqlE))
										{
											retry=true;
										}
										else if (isSerializationException(sqlE)) {
											retry = true;
										} else if (isDisconnectionException(sqlE)) {
											if (!deconnexionException) {
												retry = true;
												deconnexionException = true;
											}
										}

										break;
									}
									t = t.getCause();
								}
								if (!retry)
									throw new DatabaseIntegrityException("Impossible to rollback the database changments", se);
							}
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
						retry=isSerializationException(e) || isDisconnectionException(e);
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
			int previousPosition=-1;
			Map<Package, Long> backupPositions=null;

			try {
				if (writeData && !defaultTransaction && supportSavePoint(cw.connection.getConnection()))
				{
					Session s=getConnectionAssociatedWithCurrentThread();
					previousPosition = s.getActualPositionEvent();
					backupPositions=s.getBackupPositions();

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
						getConnectionAssociatedWithCurrentThread().cancelTmpTransactionEvents(previousPosition, backupPositions);
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

	protected abstract boolean isDuplicateKeyException(SQLException e);
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
					return _transaction.run();

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
		return getTableInstance(_table_name, -1);
	}
	/**
	 * According a class name, returns the instance of a table which inherits the
	 * class <code>Table&lsaquo;T extends DatabaseRecord&rsaquo;</code>. The
	 * returned table is always the same instance.
	 * 
	 * @param _table_name
	 *            the full class name (with its package)
	 * @param databaseVersion the database version (if negative, takes the last version)
	 * @return the corresponding table.
	 * @throws DatabaseException
	 *             if the class have not be found or if problems occur during the
	 *             instantiation.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 */
	final Table<?> getTableInstance(String _table_name, @SuppressWarnings("SameParameterValue") int databaseVersion) throws DatabaseException {
		
		try {
			lockWrite();
			if (_table_name == null)
				throw new NullPointerException("The parameter _table_name is a null pointer !");

			try {
				Class<?> c = Class.forName(_table_name);
				if (Table.class.isAssignableFrom(c)) {
					@SuppressWarnings("unchecked")
					Class<? extends Table<?>> class_table = (Class<? extends Table<?>>) c;
					return getTableInstance(class_table, databaseVersion);
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
		 * getSqlTableName()); } catch (ClassNotFoundException e) { throw new
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
	 *             if the class have not be found or if problems occur during the
	 * 	 *             instantiation.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @param <TT>
	 *            The table type
	 */
	public final <TT extends Table<?>> TT getTableInstance(Class<TT> _class_table)
			throws DatabaseException {
		return getTableInstance(_class_table, -1);
	}





	/**
	 * According a Class&lsaquo;? extends Table&lsaquo;?&rsaquo;&rsaquo;, returns
	 * the instance of a table which inherits the class
	 * <code>Table&lsaquo;T extends DatabaseRecord&rsaquo;</code>. The returned
	 * table is always the same instance.
	 * 
	 * @param _class_table
	 *            the class type
	 * @param databaseVersion the database version (if negative, takes the last version)
	 * @return the corresponding table.
	 * @throws DatabaseException
	 *             if the class have not be found or if problems occur during the
	 * 	 *             instantiation.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @param <TT>
	 *            The table type
	 */
	final <TT extends Table<?>> TT getTableInstance(Class<TT> _class_table, int databaseVersion)
			throws DatabaseException {

		try {
			lockWrite();
			if (_class_table == null)
				throw new NullPointerException("The parameter _class_table is a null pointer !");
			//Database db = this.sql_database.get(_class_table.getPackage());
			if (_class_table.getPackage().equals(getClass().getPackage()))
				databaseVersion=0;
			else if (databaseVersion<0)
				databaseVersion=getCurrentDatabaseVersion(_class_table.getPackage());
			Database db=sql_database.get(_class_table.getPackage());
			DatabasePerVersion dpv=db==null?null:db.tables_per_versions.get(databaseVersion);

			if (dpv == null) {
				if (_class_table.getPackage().equals(this.getClass().getPackage()) && (actualDatabaseLoading == null
						|| !actualDatabaseLoading.getConfiguration().getPackage().equals(_class_table.getPackage()) )) {
					loadDatabase(new DatabaseConfiguration(_class_table.getPackage(), internalDatabaseClassesList), true, 0);
					db=sql_database.get(_class_table.getPackage());
					dpv=db.tables_per_versions.get(databaseVersion);

				} else {
					try {
						lockWrite();
						if (databaseVersion<0)
							databaseVersion=getCurrentDatabaseVersion(_class_table.getPackage());
						if (actualDatabaseLoading != null && actualDatabaseLoading.getConfiguration().getPackage()
								.equals(_class_table.getPackage()) && actualDatabaseLoading.tables_per_versions.containsKey(databaseVersion)) {
							db = actualDatabaseLoading;
							dpv = db.tables_per_versions.get(databaseVersion);
						}
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

			Table<?> founded_table = dpv.tables_instances.get(_class_table);
			if (founded_table != null)
				//noinspection unchecked
				return (TT)founded_table;
			else
				throw new DatabaseException("Impossible to find the instance of the table " + _class_table.getName()
						+ ". It is possible that no SqlConnection was associated to the corresponding table.");
		}
		finally
		{
			unlockWrite();
		}

	}

	public final <R extends DatabaseRecord> Class<Table<R>> getTableClassFromRecord(R record) throws DatabaseException {
		if (record==null)
			throw new NullPointerException();
		Class<?> c=record.getClass();
		String name=c.getName();
		int i=name.lastIndexOf("$");
		if (i>0) {
			name = name.substring(0, i);
			try {
				c=Class.forName(name);
				if (Table.class.isAssignableFrom(c))
				{
					//noinspection unchecked
					return (Class<Table<R>>)c;
				}

			} catch (ClassNotFoundException | ClassCastException e) {
				throw DatabaseException.getDatabaseException(e);
			}

		}
		throw new DatabaseException("Class not found !");
	}

	public final <R extends DatabaseRecord> Table<R> getTableInstanceFromRecord(R record) throws DatabaseException {
		Class<Table<R>> c=getTableClassFromRecord(record);
		return getTableInstance(c, getCurrentDatabaseVersion(c.getPackage()));
	}
	/*final <R extends DatabaseRecord> Table<R> getTableInstanceFromRecord(R record, int databaseVersion) throws DatabaseException {
		Class<Table<R>> c=getTableClassFromRecord(record);
		if (databaseVersion<0)
			databaseVersion=getCurrentDatabaseVersion(c.getPackage());
		return getTableInstance(c, databaseVersion);
	}*/



	/**
	 * Remove a database
	 * 
	 * @param configuration
	 *            the database configuration
	 * @throws DatabaseException
	 *             if a problem occurs
	 */
	public final void deleteDatabase(final DatabaseConfiguration configuration) throws DatabaseException {
		deleteDatabase(configuration, getCurrentDatabaseVersion(configuration.getPackage()));
	}
	final void deleteDatabase(final DatabaseConfiguration configuration, final int databaseVersion) throws DatabaseException {
		try  {
			lockWrite();

			runTransaction(new Transaction() {

				@Override
				public Void run(DatabaseWrapper sql_connection) throws DatabaseException {
					getSynchronizer().unsynchronizeDatabasePackageWithCentralBackup(configuration.getPackage());

					try {
						if (!sql_database.containsKey(configuration.getPackage()))
							loadDatabase(configuration, false, databaseVersion);

					} catch (DatabaseException e) {
						return null;
					}

					Database db = sql_database.get(configuration.getPackage());
					if (db == null)
						throw new IllegalAccessError();

					ArrayList<Table<?>> list_tables = new ArrayList<>(configuration.getTableClasses().size());
					for (Class<? extends Table<?>> c : configuration.getTableClasses()) {
						Table<?> t = getTableInstance(c, databaseVersion);
						list_tables.add(t);
					}
					for (int i=list_tables.size()-1;i>=0;i--) {
						Table<?> t = list_tables.get(i);

						t.removeTableFromDatabaseStep1();
					}
					for (int i=list_tables.size()-1;i>=0;i--) {
						Table<?> t = list_tables.get(i);
						t.removeTableFromDatabaseStep2();
					}





					HashMap<Package, Database> sd = new HashMap<>(sql_database);
					db=sd.get(configuration.getPackage());
					db.tables_per_versions.remove(databaseVersion);
					if (db.tables_per_versions.size()==0)
						sd.remove(configuration.getPackage());
					else
						db.updateCurrentVersion();
					sql_database = sd;

					getDatabaseTable().removeRecord("databasePackageName", configuration.getPackage().getName());
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

	public String getInternalTableName(Class<?> tTableClass) throws DatabaseException {
		return getInternalTableName(tTableClass, getCurrentDatabaseVersion(tTableClass.getPackage()));
	}
	public String getInternalTableName(Class<?> tTableClass, int databaseVersion) throws DatabaseException {
		return getInternalTableNameFromTableID(getTableID(tTableClass, databaseVersion));
	}

	String getInternalTableNameFromTableID(int tableID) {
		return Table.TABLE_NAME_PREFIX+tableID+"__";
	}

	public int getTableID(Table<?> tTable) throws DatabaseException {
		return getTableID(tTable, tTable.getDatabaseVersion());
	}

	public int getTableID(Table<?> tTable, int databaseVersion) throws DatabaseException {
		return getTableID(tTable.getClass(), databaseVersion);
	}

	static String getLongTableName(Class<?> tTableClass)
	{
		return (tTableClass.getCanonicalName().replace(".", "_")).toUpperCase();
	}
	static String getLongPackageName(Package p)
	{
		return p.getName().replace(".", "_").toLowerCase();
	}
	int getCurrentDatabaseVersion(Package p) throws DatabaseException {
		return getCurrentDatabaseVersion(p, false);
	}
	int getCurrentDatabaseVersion(final Package p, final boolean forceUpdate) throws DatabaseException {
		return getCurrentDatabaseVersion(p, forceUpdate, -1);
	}
	int getCurrentDatabaseVersion(final Package p, final boolean forceUpdate, final int defaultValue) throws DatabaseException {
		lockRead();
		try {
			Database db = null;
			if (!forceUpdate)
				db = sql_database.get(p);
			if (db == null || db.getCurrentVersion()==-1) {
				final Database fdb=db;
				return (int) runTransaction(new Transaction() {
					@Override
					public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
						try {

							PreparedStatement pst = getConnectionAssociatedWithCurrentThread().getConnection()
									.prepareStatement("SELECT CURRENT_DATABASE_VERSION FROM `" + DatabaseWrapper.VERSIONS_OF_DATABASE + "` WHERE PACKAGE_NAME='"
											+ getLongPackageName(p) + "'" + getSqlComma());
							ResultSet rs = pst.executeQuery();
							int res=defaultValue==-1?0:defaultValue;
							if (rs.next())
								res=rs.getInt(1);
							else
							{
								Statement st = getConnectionAssociatedWithCurrentThread().getConnection()
										.createStatement();
								st.executeUpdate("INSERT INTO `" + DatabaseWrapper.VERSIONS_OF_DATABASE + "`(PACKAGE_NAME, CURRENT_DATABASE_VERSION) VALUES('"
										+ getLongPackageName(p) + "', '" + res + "')" + getSqlComma());
								st.close();
							}
							if (fdb!=null)
								fdb.currentVersion=res;
							return res;

						} catch (SQLException e) {
							throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
						}
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
				}, true);
			} else
				return db.getCurrentVersion();
		}
		finally {
			unlockRead();
		}
	}
	final void validateNewDatabaseVersionAndDeleteOldVersion(final DatabaseConfiguration configuration, final int oldDatabasaseVersion, final int newDatabaseVersion) throws DatabaseException {
		try {

			lockWrite();
			runTransaction(new Transaction() {
				@Override
				public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
					try {
						assert oldDatabasaseVersion != newDatabaseVersion;
						int r = getConnectionAssociatedWithCurrentThread().getConnection().createStatement()
								.executeUpdate("UPDATE `" + VERSIONS_OF_DATABASE + "` SET CURRENT_DATABASE_VERSION='" + newDatabaseVersion
										+ "' WHERE PACKAGE_NAME='" + getLongPackageName(configuration.getPackage()) + "'" + getSqlComma());
						if (r != 1)
							throw new DatabaseException("no record found (r="+r+")");
						if (oldDatabasaseVersion >= 0) {
							Database db = sql_database.get(configuration.getPackage());
							HashMap<Class<? extends Table<?>>, Table<?>> hm = new HashMap<>(db.tables_per_versions.get(oldDatabasaseVersion).tables_instances);
							deleteDatabase(configuration, oldDatabasaseVersion);
							for (Table<?> t : hm.values()) {
								t.changeVersion(newDatabaseVersion, getTableID(t.getClass(), newDatabaseVersion), DatabaseWrapper.this);
							}
							for (Table<?> t : hm.values()) {
								for (ForeignKeyFieldAccessor fa : t.foreign_keys_fields)
									fa.initialize();
							}
							db.tables_per_versions.get(newDatabaseVersion).tables_instances = hm;
						}
					}
					catch (SQLException e) {
						throw DatabaseException.getDatabaseException(e);
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
			}, false);
		}
		finally {
			unlockWrite();
		}

	}


	boolean doesVersionExists(final Package p, final int databaseVersion) throws DatabaseException {

			return (boolean)runTransaction(new Transaction() {
				@Override
				public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
					try {
						PreparedStatement pst = getConnectionAssociatedWithCurrentThread().getConnection()
								.prepareStatement("SELECT count(TABLE_VERSION) FROM " + ROW_PROPERTIES_OF_TABLES + " WHERE PACKAGE_NAME='" + getLongPackageName(p) + "' AND TABLE_VERSION="+databaseVersion+getSqlComma());
						ResultSet rs = pst.executeQuery();

						if (!rs.next())
							return false;
						int r=rs.getInt(1);
						assert r<2;
						return r==1;
					}
					catch (SQLException e) {
						throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
					}
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
			}, true);


	}



	public int getTableID(Class<?> tTableClass) throws DatabaseException {
		return getTableID(tTableClass, getCurrentDatabaseVersion(tTableClass.getPackage()));
	}
	int getTableID(Class<?> tTableClass, int databaseVersion) throws DatabaseException {
		String longTableName=getLongTableName(tTableClass);

		try {
			PreparedStatement pst = getConnectionAssociatedWithCurrentThread().getConnection()
					.prepareStatement("SELECT TABLE_ID FROM " + ROW_PROPERTIES_OF_TABLES +" WHERE TABLE_NAME='" + longTableName + "' AND TABLE_VERSION="+databaseVersion+getSqlComma());
			ResultSet rs = pst.executeQuery();
			Integer res=null;
			if (rs.next())
				res=rs.getInt(1);
			rs.close();
			pst.close();
			if (res!=null)
				return res;


			if (supportSingleAutoPrimaryKeys()) {
				Statement st = getConnectionAssociatedWithCurrentThread().getConnection()
						.createStatement();
				st.executeUpdate("INSERT INTO " + DatabaseWrapper.ROW_PROPERTIES_OF_TABLES + "(TABLE_NAME, TABLE_VERSION, PACKAGE_NAME) VALUES('"
						+ longTableName + "', " + databaseVersion + ", '" + getLongPackageName(tTableClass.getPackage()) + "')" + getSqlComma());
				st.close();
			}
			else
			{
				long id=getNextAutoIncrement(-1, databaseVersion, 1L);
				Statement st = getConnectionAssociatedWithCurrentThread().getConnection()
						.createStatement();
				st.executeUpdate("INSERT INTO " + DatabaseWrapper.ROW_PROPERTIES_OF_TABLES + "(TABLE_ID, TABLE_NAME, TABLE_VERSION, PACKAGE_NAME) VALUES("
						+id+", '"+ longTableName +"', "+databaseVersion+", '"+getLongPackageName(tTableClass.getPackage())+ "')" + getSqlComma());
				st.close();
			}

			/*if (!doesVersionExists(tTableClass.getPackage(), databaseVersion)) {
				st = getConnectionAssociatedWithCurrentThread().getConnection()
						.createStatement();
				rs=st.executeQuery("SELECT * FROM "+DatabaseWrapper.VERSIONS_OF_DATABASE+" WHERE PACKAGE_NAME='"
						+getLongPackageName(tTableClass.getPackage())+"'");
				boolean insert=!rs.next();
				rs.close();
				if (insert) {
					st.executeUpdate("INSERT INTO " + DatabaseWrapper.VERSIONS_OF_DATABASE + "(PACKAGE_NAME, CURRENT_DATABASE_VERSION) VALUES('"
							+ getLongPackageName(tTableClass.getPackage()) + "', " + databaseVersion + ")" + getSqlComma());

					st.close();
				}
			}*/

			pst = getConnectionAssociatedWithCurrentThread().getConnection()
					.prepareStatement("SELECT TABLE_ID FROM " + ROW_PROPERTIES_OF_TABLES +" WHERE TABLE_NAME='" + longTableName + "' AND TABLE_VERSION="+databaseVersion+getSqlComma());

			rs = pst.executeQuery();
			rs.next();
			res=rs.getInt(1);

			rs.close();
			pst.close();




			return res;

		} catch (SQLException e) {
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}

	}

	protected abstract String getSequenceQueryCreation(String sqlTableName, String sqlFieldName, long startWith);

	protected abstract String getAutoIncrementPart(String sqlTableName, String sqlFieldName, long startWith);

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
	 *            {@link DatabaseConfiguration#getDatabaseLifeCycles()} if the
	 *            database is created and if transfer from old database must done.
	 * @throws DatabaseException
	 *             if the given package is already associated to a database, or if
	 *             the database cannot be created.
	 * @throws NullPointerException
	 *             if the given parameters are null.
	 */
	public final void loadDatabase(final DatabaseConfiguration configuration,
								   final boolean createDatabaseIfNecessaryAndCheckIt) throws DatabaseException {
		loadDatabase(configuration, createDatabaseIfNecessaryAndCheckIt, -1);
	}

	final void loadDatabase(final DatabaseConfiguration configuration,
			final boolean createDatabaseIfNecessaryAndCheckIt, int databaseVersion) throws DatabaseException {
		if (configuration == null)
			throw new NullPointerException("tables is a null pointer.");
		boolean internalPackage=configuration.getPackage().equals(this.getClass().getPackage());
		if (!internalPackage)
			getTableInstance(DatabaseTable.class, databaseVersion);

		try  {
			lockWrite();

			boolean allNotFound=true;
			//final AtomicBoolean allNotFound = new AtomicBoolean(true);
			if (this.closed)
				throw new DatabaseException("The given Database was closed : " + this);
			{
				Database db = sql_database.get(configuration.getPackage());
				if (db != null) {
					if ((databaseVersion == -1 && db.tables_per_versions.containsKey(db.getCurrentVersion()))
							|| (databaseVersion != -1 && db.tables_per_versions.containsKey(databaseVersion))) {
						throw new DatabaseException("There is already a database associated to the given wrapper ");
					}
				}
			}
			try {

				allNotFound=loadDatabaseTables(configuration, createDatabaseIfNecessaryAndCheckIt, databaseVersion);
				Database actualDatabaseLoading=this.actualDatabaseLoading;
				assert actualDatabaseLoading!=null;
				if (databaseVersion==-1)
					databaseVersion=getCurrentDatabaseVersion(configuration.getPackage());
				if (!internalPackage && allNotFound) {

					try {
						DatabaseLifeCycles callable = configuration.getDatabaseLifeCycles();
						this.actualDatabaseLoading=null;

						Collection<DatabaseHooksTable.Record> hosts=getSynchronizer().resetSynchronizerAndGetAllHosts();
						this.actualDatabaseLoading=actualDatabaseLoading;
						int currentVersion=getCurrentDatabaseVersion(configuration.getPackage());
						if (currentVersion==databaseVersion) {
							DatabaseConfiguration oldConfig = configuration.getOldVersionOfDatabaseConfiguration();

							boolean removeOldDatabase = false;
							if (oldConfig != null && callable != null) {
								try {
									this.actualDatabaseLoading=null;
									loadDatabase(oldConfig, false);
									this.actualDatabaseLoading=actualDatabaseLoading;
									callable.transferDatabaseFromOldVersion(this, oldConfig, configuration);

									removeOldDatabase = callable.hasToRemoveOldDatabase();

								} catch (DatabaseException e) {
									oldConfig = null;
								}
							}
							if (callable != null) {
								callable.afterDatabaseCreation(this, configuration);
								if (removeOldDatabase)
									deleteDatabase(oldConfig, databaseVersion);
							}
							actualDatabaseLoading.initBackupRestoreManager(this, getDatabaseDirectory(), configuration);
						}
						getSynchronizer().restoreHosts(hosts, callable != null && callable.replaceDistantConflictualRecordsWhenDistributedDatabaseIsResynchronized());



					} catch (Exception e) {
						throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
					}
				}


				@SuppressWarnings("unchecked")
				HashMap<Package, Database> sd = (HashMap<Package, Database>) sql_database.clone();
				Database db=sd.get(configuration.getPackage());

				if (db==null) {
					sd.put(configuration.getPackage(), actualDatabaseLoading);
				}
				else {
					db.tables_per_versions.put(databaseVersion, actualDatabaseLoading.tables_per_versions.get(databaseVersion));
					//db.updateCurrentVersion();
				}
				sql_database = sd;
			} finally {
				actualDatabaseLoading = null;
				if (!allNotFound && !configuration.getPackage().equals(this.getClass().getPackage()))
					getSynchronizer().isReliedToDistantHook();
			}
		}
		finally
		{
			unlockWrite();
		}
	}
	protected String getPostCreateTable(Long autoIncrementStart)
	{
		return "";
	}

	protected abstract boolean supportForeignKeys();

	/*
	 * result found into actualDatabaseLoading
	 */
	final boolean loadDatabaseTables(final DatabaseConfiguration configuration, final boolean createDatabaseIfNecessaryAndCheckIt, final int _version) throws DatabaseException {

		return (boolean)runTransaction(new Transaction() {

			@Override
			public Boolean run(DatabaseWrapper sql_connection) throws DatabaseException {
				try {

					boolean allNotFound=true;
					/*
					 * boolean table_found=false; try (ReadQuerry rq=new
					 * ReadQuerry(sql_connection.getSqlConnection(),
					 * "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.SYSTEM_COLUMNS WHERE TABLE_NAME='"
					 * +ROW_PROPERTIES_OF_TABLES+"'")) { if (rq.result_set.next()) table_found=true; }
					 */
					checkAutoIncrementTable();
					if (!doesTableExists(ROW_PROPERTIES_OF_TABLES)) {
						String seqQuery=getSequenceQueryCreation(ROW_PROPERTIES_OF_TABLES,"PACKAGE_NAME", 1);
						if (seqQuery!=null && seqQuery.length()>0) {
							Statement st = getConnectionAssociatedWithCurrentThread().getConnection()
									.createStatement();
							st.executeUpdate(seqQuery);
							st.close();
						}
						Statement st = getConnectionAssociatedWithCurrentThread().getConnection()
								.createStatement();
						st.executeUpdate("CREATE TABLE " + ROW_PROPERTIES_OF_TABLES
								+ " (TABLE_NAME VARCHAR(512), TABLE_VERSION INTEGER, PACKAGE_NAME VARCHAR(512), TABLE_ID INTEGER "+getAutoIncrementPart(ROW_PROPERTIES_OF_TABLES,"PACKAGE_NAME", 1)
								//+", CONSTRAINT TABLE_NAME_PK PRIMARY KEY(TABLE_NAME)"
								+", CONSTRAINT TABLE_ID_PK PRIMARY KEY(TABLE_ID, TABLE_VERSION))"+getPostCreateTable(1L)
								+ getSqlComma());
						st.close();
					}
					int version=_version;
					if (!doesTableExists(VERSIONS_OF_DATABASE)) {
						Statement st = getConnectionAssociatedWithCurrentThread().getConnection()
								.createStatement();
						st.executeUpdate("CREATE TABLE `" + VERSIONS_OF_DATABASE
								+ "`(PACKAGE_NAME VARCHAR(512), CURRENT_DATABASE_VERSION INTEGER "
								+", CONSTRAINT VERSION_DB_ID_PK PRIMARY KEY(PACKAGE_NAME))"+getPostCreateTable(null)
								+ getSqlComma());
						st.close();
						if (version==-1)
							version=0;
						st = getConnectionAssociatedWithCurrentThread().getConnection()
								.createStatement();
						st.executeUpdate("INSERT INTO `" + DatabaseWrapper.VERSIONS_OF_DATABASE + "`(PACKAGE_NAME, CURRENT_DATABASE_VERSION) VALUES('"
								+ getLongPackageName(configuration.getPackage()) + "', '" + version + "')" + getSqlComma());
						st.close();
					}
					else
						version=getCurrentDatabaseVersion(configuration.getPackage(), false, version);



					ArrayList<Table<?>> list_tables = new ArrayList<>(configuration.getTableClasses().size());
					DatabasePerVersion dpv=new DatabasePerVersion();
					actualDatabaseLoading.tables_per_versions.put(version, dpv);
					//actualDatabaseLoading.updateCurrentVersion();
					for (Class<? extends Table<?>> class_to_load : configuration.getTableClasses()) {
						Table<?> t = newInstance(class_to_load, version);
						list_tables.add(t);
						dpv.tables_instances.put(class_to_load, t);
					}

					for (Table<?> t : list_tables) {
						t.initializeStep1(configuration);
					}
					for (Table<?> t : list_tables) {
						allNotFound=
								!t.initializeStep2(createDatabaseIfNecessaryAndCheckIt) && allNotFound;
					}
					for (Table<?> t : list_tables) {
						t.initializeStep3();
					}
					Database actualDatabaseLoading=DatabaseWrapper.this.actualDatabaseLoading;
					DatabaseWrapper.this.actualDatabaseLoading=null;
					DatabaseTable.Record dbt=getDatabaseTable().getRecord("databasePackageName", configuration.getPackage().getName());
					if (dbt==null)
					{
						getDatabaseTable().addRecord(new DatabaseTable.Record(configuration.getPackage().getName()));
					}
					DatabaseWrapper.this.actualDatabaseLoading=actualDatabaseLoading;
					return allNotFound;

				} /*catch (ClassNotFoundException e) {
							throw new DatabaseException(
									"Impossible to access to t)he list of classes contained into the package "
											+ configuration.getPackage().getSqlTableName(),
									e);
						} catch (IOException e) {
							throw new DatabaseException(
									"Impossible to access to the list of classes contained into the package "
											+ configuration.getPackage().getSqlTableName(),
									e);
						}*/ catch (Exception e) {
					throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
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
			}

		}, true);
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

	<TT extends Table<?>> TT newInstance(Class<TT> _class_table, int databaseVersion) throws InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, DatabaseException, PrivilegedActionException {
		DefaultConstructorAccessPrivilegedAction<TT> class_privelege = new DefaultConstructorAccessPrivilegedAction<>(
                _class_table);

		Constructor<TT> const_table = AccessController.doPrivileged(class_privelege);


		TT t = const_table.newInstance();
		t.initializeStep0(this, databaseVersion);
		return t;
	}

	protected abstract boolean doesTableExists(String tableName) throws Exception;

	protected final ColumnsReadQuerry getColumnMetaData(String tableName) throws Exception
	{
		return getColumnMetaData(tableName, null);
	}


	protected abstract ColumnsReadQuerry getColumnMetaData(String tableName, String columnName) throws Exception;

	protected abstract void checkConstraints(Table<?> table) throws DatabaseException;

	protected abstract boolean autoPrimaryKeyIndexStartFromOne();

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

		public abstract int getOrdinalPosition() throws SQLException;
	}

	protected abstract String getSqlComma();

	protected abstract int getVarCharLimit();

	protected abstract String getBinaryBaseWord();
	protected abstract String getBlobBaseWord();

	protected abstract String getVarBinaryType(long limit);

	protected abstract String getLongVarBinaryType(long limit);

	protected abstract boolean isVarBinarySupported();

	protected abstract boolean isLongVarBinarySupported();

	protected abstract String getSqlNULL();

	protected abstract String getSqlNotNULL();

	protected abstract String getByteType();

	protected abstract String getIntType();

	@SuppressWarnings("SameParameterValue")
	protected abstract String getBlobType(long limit);

	protected abstract String getTextType(long limit);

	protected abstract String getFloatType();

	protected abstract String getDoubleType();

	protected abstract String getShortType();

	protected abstract String getLongType();

	protected abstract String getBigDecimalType(@SuppressWarnings("SameParameterValue") long limit);

	protected abstract String getBigIntegerType(long limit);


	protected abstract String getDateTimeType();
	//protected abstract String getSqlQuerryToGetLastGeneratedID();


	protected abstract String getDropTableCascadeQuery(Table<?> table);


	protected abstract boolean supportSingleAutoPrimaryKeys();

	protected abstract boolean supportMultipleAutoPrimaryKeys();

	Collection<Table<?>> getListTables(Package p, int databaseVersion) {
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
        assert db != null;
        return db.tables_per_versions.get(databaseVersion).tables_instances.values();
	}


	protected abstract String getOnUpdateCascadeSqlQuery();

	protected abstract String getOnDeleteCascadeSqlQuery();

	protected boolean supportUpdateCascade() {
		return !getOnUpdateCascadeSqlQuery().equals("");
	}

	protected abstract Blob getBlob(byte[] bytes) throws SQLException;

	/**
	 * Backup the database into the given path using the native database backup system
	 * 
	 * @param path
	 *            the path where to save the database.
	 * @throws DatabaseException
	 *             if a problem occurs
	 */
	public abstract void nativeBackup(File path) throws DatabaseException;


	/**
	 * Returns the OOD (and non native) backup manager.
	 *
	 * Backups into this manager are done in real time.
	 *
	 * Backups and restores into this backup manager can be done manually.
	 *
	 * Enables to make incremental database backups, and database restore.
	 * @param _package the concerned database
	 * @return the backup manager or null if no backup manager was configured.
	 * @see DatabaseConfiguration
	 */
	public BackupRestoreManager getBackupRestoreManager(Package _package)
	{
		Database d=this.sql_database.get(_package);
		if (d==null)
			return null;
		else
			return d.backupRestoreManager;
	}



	/**
	 * Returns the OOD (and non native) backup manager.
	 *
	 * Backups into this manager are NOT done in real time.
	 *
	 * Backups and restores into this backup manager can be done manually.
	 *
	 * Enables to make incremental database backups, and database restore.
	 * @param backupDirectory the backup directory
	 * @param _package the concerned database
	 * @param backupConfiguration the backup configuration
	 * @return the backup manager or null if no backup manager was configured.
	 * @see DatabaseConfiguration
	 * @throws DatabaseException if a problem occurs
	 */
	public BackupRestoreManager getExternalBackupRestoreManager(File backupDirectory, Package _package, BackupConfiguration backupConfiguration) throws DatabaseException {

		try {
			if (getDatabaseDirectory().getCanonicalPath().startsWith(backupDirectory.getCanonicalPath()))
				throw new IllegalArgumentException("External backup managers cannot be located into the directory of the database !");
			Database d = this.sql_database.get(_package);
			if (d == null)
				return null;
			else
				return new BackupRestoreManager(this, new File(backupDirectory, getLongPackageName(_package)), d.configuration, backupConfiguration, true);
		}
		catch(IOException e)
		{
			throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
		}
	}

	protected abstract boolean isThreadSafe();

	protected abstract boolean supportFullSqlFieldName();

	/**
	 * Delete all the files associated to the database directory.
	 *
	 * @param _directory
	 *            the database directory
	 */
	public static void deleteDatabaseFiles(File _directory) {
		if (_directory.exists() && _directory.isDirectory()) {
			FileTools.deleteDirectory(_directory);
		}
	}


	/**
	 * Delete all the files associated to this database
	 *
	 */
	public void deleteDatabaseFiles() {
		close();
		deleteDatabaseFiles(getDatabaseDirectory());
	}


	protected abstract String getLimitSqlPart(long startPosition, long rowLimit);

	boolean areGeneratedValueReturnedWithPrimaryKeys()
	{
		return false;
	}

	static
	{
		initPreloadedClasses();
	}

	private static void initPreloadedClasses()
	{
		ArrayList<Class<? extends SecureExternalizableWithoutInnerSizeControl>> classes = new ArrayList<>(Arrays.asList(
				(Class<? extends SecureExternalizableWithoutInnerSizeControl>)DatabaseBackupMetaData.class,
				TransactionMetaData.class,
				DatabaseBackupMetaDataPerFile.class,
				EncryptedDatabaseBackupMetaDataPerFile.class,
				EncryptedBackupPartComingFromCentralDatabaseBackup.class,
				EncryptedBackupPartDestinedToCentralDatabaseBackup.class,
				EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup.class,
				//DatabaseWrapper.TransactionsInterval.class,
				DatabaseTransactionsIdentifiersToSynchronize.class,
				LastIDCorrection.class,
				TransactionConfirmationEvents.class,
				DatabaseEventsToSynchronizeP2P.class,
				HookAddRequest.class,
				BackupChannelInitializationMessageFromCentralDatabaseBackup.class,
				BackupChannelUpdateMessageFromCentralDatabaseBackup.class,
				EncryptedMetaDataFromCentralDatabaseBackup.class,
				AskForMetaDataPerFileToCentralDatabaseBackup.class
				));
		for (Class<?> c : classes)
			assert !Modifier.isAbstract(c.getModifiers()):""+c;

		ArrayList<Class<? extends Enum<?>>> enums = new ArrayList<>(Arrays.asList(
				DatabaseEventType.class,
				TransactionIsolation.class));

		SerializationTools.addPredefinedClasses(classes, enums);
	}


}
