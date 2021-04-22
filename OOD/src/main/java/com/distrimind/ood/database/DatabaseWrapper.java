
/*
Copyright or ©. Jason Mahdjoub (01/04/2013)

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

import com.distrimind.ood.database.DatabaseHooksTable.Record;
import com.distrimind.ood.database.Table.ColumnsReadQuery;
import com.distrimind.ood.database.Table.DefaultConstructorAccessPrivilegedAction;
import com.distrimind.ood.database.centraldatabaseapi.*;
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
import com.distrimind.util.crypto.ASymmetricPublicKey;
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
import java.util.Date;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Formatter;
import java.util.logging.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class represent a SqlJet database.
 * 
 * @author Jason Mahdjoub
 * @version 2.0
 * @since OOD 1.4
 */
@SuppressWarnings("ThrowFromFinallyBlock")
public abstract class DatabaseWrapper implements AutoCloseable {

	public static final int MAX_ACCEPTED_SIZE_IN_BYTES_OF_DECENTRALIZED_VALUE= ASymmetricPublicKey.MAX_SIZE_IN_BYTES_OF_NON_PQC_RSA_NON_HYBRID_PUBLIC_KEY;

	private static final Set<Class<?>> internalDatabaseClassesList = new HashSet<>(Arrays.asList(DatabaseDistantTransactionEvent.class, DatabaseDistantEventsTable.class,
			DatabaseEventsTable.class, DatabaseHooksTable.class, DatabaseTransactionEventsTable.class, DatabaseTransactionsPerHostTable.class, IDTable.class, DatabaseTable.class));
	private static final Set<Class<?>> internalCentralDatabaseClassesList =new HashSet<>(Arrays.asList(
			ClientCloudAccountTable.class,
			ClientTable.class,
			ConnectedClientsTable.class,
			DatabaseBackupPerClientTable.class,
			EncryptedBackupPartReferenceTable.class,
			LastValidatedDistantIDPerClientTable.class));
	static final Set<Package> reservedDatabases=new HashSet<>(Arrays.asList(DatabaseWrapper.class.getPackage(), CentralDatabaseBackupReceiverPerPeer.class.getPackage()));
	/*private static final List<DatabaseWrapper> openedDatabaseWrappers=new ArrayList<>();
	private static final Map<DecentralizedValue, CachedEPV> cachedEPVsForCentralDatabaseBackup=new HashMap<>();
	private static final Map<DecentralizedValue, CachedEPV> cachedEPVsForAuthenticatedP2PMessages=new HashMap<>();
	private static class CachedEPV
	{
		private final DatabaseWrapper wrapper;
		private final ProfileProviderTree.EPV epv;

		public CachedEPV(DatabaseWrapper wrapper, ProfileProviderTree.EPV epv) {
			if (wrapper==null)
				throw new NullPointerException();
			this.wrapper = wrapper;
			this.epv = epv;
		}
	}


	static void addOpenedDatabaseWrapper(DatabaseWrapper wrapper)
	{
		synchronized (DatabaseWrapper.class) {
			openedDatabaseWrappers.add(wrapper);
		}
	}
	static void removeClosedDatabaseWrapper(DatabaseWrapper wrapper)
	{
		synchronized (DatabaseWrapper.class) {
			openedDatabaseWrappers.remove(wrapper);
			removeCachedEncryptionProfileProvider(cachedEPVsForAuthenticatedP2PMessages, wrapper);
			removeCachedEncryptionProfileProvider(cachedEPVsForCentralDatabaseBackup, wrapper);
		}
	}

	static ProfileProviderTree.EPV getEncryptionProfileProviderForCentralDatabaseBackup(DecentralizedValue localHostID) throws DatabaseException {
		synchronized (DatabaseWrapper.class) {
			ProfileProviderTree.EPV epv=getCachedEncryptionProfileProvider(cachedEPVsForCentralDatabaseBackup, localHostID);
			if (epv==null)
			{
				Reference<ProfileProviderTree.EPV> encryptionProfileProviderForCentralDatabaseBackup=new Reference<>();
				Reference<ProfileProviderTree.EPV> protectedEncryptionProfileProviderForAuthenticatedP2PMessages=new Reference<>();
				refreshEPVCache(localHostID, null, encryptionProfileProviderForCentralDatabaseBackup, protectedEncryptionProfileProviderForAuthenticatedP2PMessages);
				return encryptionProfileProviderForCentralDatabaseBackup.get();
			}
			else
				return epv;
		}
	}

	
	static ProfileProviderTree.EPV getProtectedEncryptionProfileProviderForAuthenticatedP2PMessages(DecentralizedValue host1, DecentralizedValue host2) throws DatabaseException {
		synchronized (DatabaseWrapper.class) {
			ProfileProviderTree.EPV epv=getCachedEncryptionProfileProvider(cachedEPVsForAuthenticatedP2PMessages, host1);
			if (epv==null && host2!=null)
				epv=getCachedEncryptionProfileProvider(cachedEPVsForAuthenticatedP2PMessages, host2);
			if (epv==null)
			{
				Reference<ProfileProviderTree.EPV> encryptionProfileProviderForCentralDatabaseBackup=new Reference<>();
				Reference<ProfileProviderTree.EPV> protectedEncryptionProfileProviderForAuthenticatedP2PMessages=new Reference<>();
				refreshEPVCache(host1, host2, encryptionProfileProviderForCentralDatabaseBackup, protectedEncryptionProfileProviderForAuthenticatedP2PMessages);
				return protectedEncryptionProfileProviderForAuthenticatedP2PMessages.get();
			}
			else
				return epv;
		}
	}
	private static void refreshEPVCache(DecentralizedValue host1, DecentralizedValue host2, Reference<ProfileProviderTree.EPV> encryptionProfileProviderForCentralDatabaseBackup, Reference<ProfileProviderTree.EPV> protectedEncryptionProfileProviderForAuthenticatedP2PMessages) throws DatabaseException {
		for (DatabaseWrapper w : openedDatabaseWrappers)
		{
			DecentralizedValue localHostID=null;
			if (host1.equals(w.getSynchronizer().getLocalHostID()))
				localHostID=host1;
			else if (host2!=null && host2.equals(w.getSynchronizer().getLocalHostID()))
				localHostID=host2;
			if (localHostID!=null)
			{
				removeCachedEncryptionProfileProvider(cachedEPVsForAuthenticatedP2PMessages, w);
				removeCachedEncryptionProfileProvider(cachedEPVsForCentralDatabaseBackup, w);
				if (w.getDatabaseConfigurationsBuilder().getEncryptionProfileProviderForCentralDatabaseBackup()!=null)
					encryptionProfileProviderForCentralDatabaseBackup.set(new ProfileProviderTree.EPV(w.getDatabaseConfigurationsBuilder().getEncryptionProfileProviderForCentralDatabaseBackup(),
						w.getDatabaseConfigurationsBuilder().getSecureRandom()));
				if (w.getDatabaseConfigurationsBuilder().getProtectedEncryptionProfileProviderForAuthenticatedP2PMessages()!=null)
					protectedEncryptionProfileProviderForAuthenticatedP2PMessages.set(new ProfileProviderTree.EPV(w.getDatabaseConfigurationsBuilder().getProtectedEncryptionProfileProviderForAuthenticatedP2PMessages(),
						w.getDatabaseConfigurationsBuilder().getSecureRandom()));
				cachedEPVsForCentralDatabaseBackup.put(localHostID, new CachedEPV(w,encryptionProfileProviderForCentralDatabaseBackup.get()));
				cachedEPVsForAuthenticatedP2PMessages.put(localHostID, new CachedEPV(w,protectedEncryptionProfileProviderForAuthenticatedP2PMessages.get()));
				break;
			}
		}
	}
	private static void removeCachedEncryptionProfileProvider(Map<DecentralizedValue, CachedEPV> cachedEPVs, DatabaseWrapper wrapper)
	{
		cachedEPVs.entrySet().removeIf(decentralizedValueCachedEPVEntry -> decentralizedValueCachedEPVEntry.getValue().wrapper == wrapper);
	}
	private static ProfileProviderTree.EPV getCachedEncryptionProfileProvider(Map<DecentralizedValue, CachedEPV> cachedEPVs, DecentralizedValue localHostID)
	{
		CachedEPV cepv=cachedEPVs.get(localHostID);
		if (cepv==null)
			return null;
		else
			return cepv.epv;
	}*/

	// protected Connection sql_connection;
	private static final String NATIVE_BACKUPS_DIRECTORY_NAME="native_backups";
	private static final String EXTERNAL_TEMPORARY_BACKUPS_DIRECTORY_NAME="external_temporary_backups";
	private static final String TEMPORARY_BACKUPS_COMING_FROM_DISTANT_DATABASE_BACKUP_DIRECTORY_NAME="temporary_backups_coming_from_distant_db_backup";
	private static final String TEMPORARY_BACKUPS_COMING_FROM_INTERNAL_BACKUP_DIRECTORY_NAME="temporary_backups_coming_from_internal_db_backup";

	private static final String HOST_CHANNEL_ID_FILE_NAME="host_channel_id";
	private volatile boolean closed = false;
	private volatile boolean openedOneTime = false;
	protected final String databaseName;
	protected final boolean loadToMemory;
	protected final File databaseDirectory;
	protected final String databaseIdentifier;
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
	volatile int maxTransactionsEventsKeptIntoMemory = 100;
	protected Database actualDatabaseLoading = null;
	volatile int maxTransactionEventsKeptIntoMemoryDuringImportInBytes =10000000;
	private volatile boolean hasOnePeerSynchronized =false;
	private volatile AbstractSecureRandom randomForKeys;
	private final boolean alwaysDisconnectAfterOnTransaction;
	private static final int MAX_TRANSACTIONS_TO_SYNCHRONIZE_AT_THE_SAME_TIME=1000000;
	public static final int MAX_DISTANT_PEERS=50;
	public static final int MAX_PACKAGE_TO_SYNCHRONIZE=200;
	private static volatile int MAX_HOST_NUMBERS=5;
	public static final String NETWORK_LOGGER_NAME="OOD_NETWORK_LOGGER_NAME";
	private final DatabaseConfigurationsBuilder databaseConfigurationsBuilder;
	private volatile Logger networkLogger=null;
	private volatile Logger databaseLogger=null;

	public static int getMaxHostNumbers() {
		return MAX_HOST_NUMBERS;
	}

	public static void setMaxHostNumbers(int maxHostNumbers) {
		if (maxHostNumbers>=0xFFFF)
			throw new IllegalArgumentException();
		MAX_HOST_NUMBERS = maxHostNumbers;
	}

	public Logger getNetworkLogger() {
		return networkLogger;
	}
	public void setDatabaseLogLevel(Level level)
	{

		lockWrite();
		try {
			if (level==Level.OFF)
			{
				/*if (databaseLogger!=null && databaseLoggerHandler !=null)
				{
					databaseLogger.removeHandler(databaseLoggerHandler);
				}*/
				databaseLogger=null;
			}
			else {
				if (databaseLogger == null) {
					Logger networkLogger = Logger.getAnonymousLogger();
					Handler databaseLoggerHandler = new ConsoleHandler();
					databaseLoggerHandler.setFormatter(generatesLogFormatter());

					networkLogger.addHandler(databaseLoggerHandler);
					this.databaseLogger = networkLogger;
				}
				databaseLogger.setUseParentHandlers(false);
				for (Handler h : databaseLogger.getHandlers())
					h.setLevel(level);
				databaseLogger.setLevel(level);
			}
		}
		finally {
			unlockWrite();
		}

	}

	private Formatter generatesLogFormatter()
	{
		return new Formatter() {

			@Override
			public String format(LogRecord record) {
				try {
					StringBuilder sb=new StringBuilder(getSynchronizer().getLocalHostID()+"");
					while (sb.length()<55)
						sb.append(" ");
					sb.append("::");
					sb.append(new Date(record.getMillis()));
					sb.append("::");
					sb.append(record.getLevel());
					sb.append("::  ");
					sb.append(record.getMessage());
					sb.append("\n");
					return sb.toString();
				} catch (DatabaseException e) {
					e.printStackTrace();
					return new Date(System.currentTimeMillis()) + "::Error log: " + e.getMessage();
				}
			}
		};
	}
	public void setNetworkLogLevel(Level level)
	{

		lockWrite();
		try {
			if (level==Level.OFF)
			{
				/*if (networkLogger!=null && networkLoggerHandler !=null)
				{
					networkLogger.removeHandler(networkLoggerHandler);
				}*/
				networkLogger=null;
			}
			else {
				if (networkLogger == null) {
					Logger networkLogger = Logger.getAnonymousLogger();
					Handler networkLoggerHandler = new ConsoleHandler();
					networkLoggerHandler.setFormatter(generatesLogFormatter());

					networkLogger.addHandler(networkLoggerHandler);
					this.networkLogger = networkLogger;
				}
				networkLogger.setUseParentHandlers(false);
				for (Handler h : networkLogger.getHandlers())
					h.setLevel(level);
				networkLogger.setLevel(level);
			}
		}
		finally {
			unlockWrite();
		}

	}




	public DatabaseConfigurationsBuilder getDatabaseConfigurationsBuilder() {
		return databaseConfigurationsBuilder;
	}

	public boolean isAlwaysDisconnectAfterOnTransaction() {
        return alwaysDisconnectAfterOnTransaction;
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

	public File prepareDatabaseRestorationFromExternalDatabaseBackupAndGetTemporaryBackupDirectoryName(DatabaseConfiguration databaseConfiguration) throws DatabaseException {
		return prepareDatabaseRestorationFromExternalDatabaseBackupAndGetTemporaryBackupDirectoryName(databaseConfiguration.getDatabaseSchema());
	}
	public File prepareDatabaseRestorationFromExternalDatabaseBackupAndGetTemporaryBackupDirectoryName(DatabaseSchema schema) throws DatabaseException {
		return prepareDatabaseRestorationFromExternalDatabaseBackupAndGetTemporaryBackupDirectoryName(schema.getPackage());
	}
	public File prepareDatabaseRestorationFromExternalDatabaseBackupAndGetTemporaryBackupDirectoryName(Package databasePackage) throws DatabaseException {
		if (databasePackage==null)
			throw new NullPointerException();
		lockWrite();
		try {
			Database db = sql_database.get(databasePackage);
			if (db == null)
				throw new DatabaseException("The database " + databasePackage.getName() + " was not loaded");
			return db.prepareDatabaseRestorationFromExternalDatabaseBackupAndGetTemporaryBackupDirectoryName();
		}
		finally {
			unlockWrite();
		}
	}
	void prepareDatabaseRestorationFromDistantDatabaseBackupChannel(Package databasePackage) throws DatabaseException {
		if (databasePackage==null)
			throw new NullPointerException();
		lockWrite();
		try {
			Database db = sql_database.get(databasePackage);
			if (db == null)
				throw new DatabaseException("The database " + databasePackage.getName() + " was not loaded");
			db.prepareDatabaseRestorationFromDistantDatabaseBackupChannel();
			getSynchronizer().checkNotAskForDatabaseBackupPartDestinedToCentralDatabaseBackup(db);
		}
		finally {
			unlockWrite();
		}
	}
	boolean prepareDatabaseRestorationFromInternalBackupChannel(Package databasePackage) throws DatabaseException {
		if (databasePackage==null)
			throw new NullPointerException();
		lockWrite();
		try {
			Database db = sql_database.get(databasePackage);
			if (db == null)
				throw new DatabaseException("The database " + databasePackage.getName() + " was not loaded");
			db.prepareDatabaseRestorationFromInternalBackup();
			return getSynchronizer().checkNotAskForDatabaseBackupPartDestinedToCentralDatabaseBackup(db);
		}
		finally {
			unlockWrite();
		}
	}
	public void restoreDatabaseToDateUTCFromExternalTemporaryDatabaseBackupAndRemoveBackupFiles(DatabaseConfiguration databaseConfiguration, long dateUTCInMs) throws DatabaseException {
		restoreDatabaseToDateUTCFromExternalTemporaryDatabaseBackupAndRemoveBackupFiles(databaseConfiguration.getDatabaseSchema(), dateUTCInMs);
	}
	public void restoreDatabaseToDateUTCFromExternalTemporaryDatabaseBackupAndRemoveBackupFiles(DatabaseSchema schema, long dateUTCInMs) throws DatabaseException {
		restoreDatabaseToDateUTCFromExternalTemporaryDatabaseBackupAndRemoveBackupFiles(schema.getPackage(), dateUTCInMs);
	}
	public void restoreDatabaseToDateUTCFromExternalTemporaryDatabaseBackupAndRemoveBackupFiles(Package databasePackage, long dateUTCInMs) throws DatabaseException {
		if (databasePackage==null)
			throw new NullPointerException();
		lockWrite();
		try {
			Database db = sql_database.get(databasePackage);
			if (db == null)
				throw new DatabaseException("The database " + databasePackage.getName() + " was not loaded");

			db.restoreDatabaseToDateUTCFromExternalTemporaryDatabaseBackupAndRemoveBackupFiles(dateUTCInMs);
		}
		finally {
			unlockWrite();
		}
	}
	public void cancelRestorationFromExternalDatabaseBackup(DatabaseConfiguration databaseConfiguration) throws DatabaseException {

		cancelRestorationFromExternalDatabaseBackup(databaseConfiguration.getDatabaseSchema());
	}

	public void cancelRestorationFromExternalDatabaseBackup(DatabaseSchema databaseSchema) throws DatabaseException {
		cancelRestorationFromExternalDatabaseBackup(databaseSchema.getPackage());
	}
	void restorationFromInternalDatabaseBackupFinished(Package databasePackage) throws DatabaseException {
		if (databasePackage==null)
			throw new NullPointerException();
		lockWrite();
		try {
			Database db = sql_database.get(databasePackage);
			if (db == null)
				throw new DatabaseException("The database " + databasePackage.getName() + " was not loaded");

			db.restorationFromInternalDatabaseBackupFinished();
		}
		finally {
			unlockWrite();
		}
	}
	public void cancelRestorationFromExternalDatabaseBackup(Package databasePackage) throws DatabaseException {
		if (databasePackage==null)
			throw new NullPointerException();
		lockWrite();
		try {
			Database db = sql_database.get(databasePackage);
			if (db == null)
				throw new DatabaseException("The database " + databasePackage.getName() + " was not loaded");

			db.cancelRestorationFromExternalDatabaseBackup();
		}
		finally {
			unlockWrite();
		}
	}
	public boolean isCurrentDatabaseInRestorationProcessFromExternalBackup(DatabaseConfiguration databaseConfiguration) throws DatabaseException {
		return isCurrentDatabaseInRestorationProcessFromExternalBackup(databaseConfiguration.getDatabaseSchema());
	}
	public boolean isCurrentDatabaseInRestorationProcessFromExternalBackup(DatabaseSchema databaseSchema) throws DatabaseException {
		return isCurrentDatabaseInRestorationProcessFromExternalBackup(databaseSchema.getPackage());
	}
	public boolean isCurrentDatabaseInRestorationProcessFromExternalBackup(Package databasePackage) throws DatabaseException {
		if (databasePackage==null)
			throw new NullPointerException();
		lockRead();
		try {
			Database db = sql_database.get(databasePackage);
			if (db == null)
				throw new DatabaseException("The database " + databasePackage.getName() + " was not loaded");

			return db.isCurrentDatabaseInRestorationProcessFromExternalBackup();
		}
		finally {
			unlockRead();
		}
	}


	// private final boolean isWindows;
	class Database {
		final HashMap<Integer, DatabasePerVersion> tables_per_versions = new HashMap<>();
		private long lastValidatedTransactionUTCForCentralBackup=Long.MIN_VALUE;
		BackupRestoreManager backupRestoreManager=null, temporaryBackupRestoreManagerComingFromDistantBackupManager=null;
		private DecentralizedValue temporaryBackupRestoreManagerChannelComingFromDistantBackupManager=null;
		private int currentVersion=-1;
		private boolean currentDatabaseInRestorationProcessFromExternalBackup;
		private boolean currentDatabaseInRestorationProcessFromInternalBackup;

		private final DatabaseConfiguration configuration;

		public Database(DatabaseConfiguration configuration) throws DatabaseException {
			if (configuration == null)
				throw new NullPointerException("configuration");
			this.configuration = configuration;
			currentDatabaseInRestorationProcessFromExternalBackup=getExternalTemporaryDatabaseBackupFileName().exists();
			currentDatabaseInRestorationProcessFromInternalBackup=getTemporaryDatabaseBackupFileNameForBackupComingFromInternalBackup().exists();
			File f=getTemporaryDatabaseBackupFileNameForBackupComingFromDistantDatabaseBackup();
			if (f.exists()) {
				initTemporaryBackupRestoreManagerComingFromDistantBackupRestoreManager(f);
			}

		}
		private void initTemporaryBackupRestoreManagerComingFromDistantBackupRestoreManager(File f) throws DatabaseException {
			temporaryBackupRestoreManagerComingFromDistantBackupManager=new BackupRestoreManager(DatabaseWrapper.this, f, configuration, true );
			File hostChannelFile=new File(f, HOST_CHANNEL_ID_FILE_NAME);
			if (hostChannelFile.exists())
			{
				try(RandomFileInputStream fis=new RandomFileInputStream(hostChannelFile))
				{
					temporaryBackupRestoreManagerChannelComingFromDistantBackupManager=fis.readObject(false);
				} catch (IOException | ClassNotFoundException e) {
					throw DatabaseException.getDatabaseException(e);
				}
			}
			else
				temporaryBackupRestoreManagerChannelComingFromDistantBackupManager=null;
		}
		void setTemporaryBackupRestoreManagerChannelComingFromDistantBackupManager(DecentralizedValue hostChannel) throws DatabaseException {
			if (temporaryBackupRestoreManagerChannelComingFromDistantBackupManager!=null)
			{
				if (!temporaryBackupRestoreManagerChannelComingFromDistantBackupManager.equals(hostChannel))
					throw DatabaseException.getDatabaseException(new IllegalArgumentException());
			}
			else {
				File hostChannelFile=new File(temporaryBackupRestoreManagerComingFromDistantBackupManager.getBackupDirectory(), HOST_CHANNEL_ID_FILE_NAME);
				try(RandomOutputStream ros=new RandomFileOutputStream(hostChannelFile)) {
					ros.writeObject(hostChannel, false);
				} catch (IOException e) {
					throw DatabaseException.getDatabaseException(e);
				}
				temporaryBackupRestoreManagerChannelComingFromDistantBackupManager = hostChannel;
			}

		}

		int getCurrentVersion() throws DatabaseException {
			if (currentVersion==-1)
				DatabaseWrapper.this.getCurrentDatabaseVersion(configuration.getDatabaseSchema().getPackage(), true);
			return currentVersion;
		}

		void updateCurrentVersion()
		{
			this.currentVersion=-1;
		}

		DatabaseConfiguration getConfiguration() {
			return configuration;
		}
		void checkOldDatabaseBackupsRemoving(DatabaseWrapper wrapper, File databaseDirectory, DatabaseConfiguration configuration) throws DatabaseException {
			ArrayList<DatabaseSchema> l=new ArrayList<>();
			DatabaseSchema c=configuration.getDatabaseSchema();
			while (c!=null)
			{
				l.add(c);
				c=c.getOldSchema();
			}
			if (l.size()>1)
			{
				for (int i=l.size()-1;i>0;i--)
				{
					c=l.get(i);
					DatabaseSchema cn=l.get(i-1);
					if (!c.getPackage().equals(cn.getPackage())) {
						File f=getDatabaseBackupFileName(databaseDirectory, c);
						if (f.exists()) {
							boolean setNull;
							if (configuration.getBackupConfiguration()==null)
							{
								setNull=true;
								long defaultMaxBackupConfiguration=3L*30L*24L*60L*60L*1000L;
								configuration.setBackupConfiguration(new BackupConfiguration(defaultMaxBackupConfiguration,defaultMaxBackupConfiguration,100000, 1, null ));
							}
							else
								setNull=false;
							this.backupRestoreManager = new BackupRestoreManager(wrapper, f, configuration, true);
							this.backupRestoreManager.cleanOldBackups();
							if (setNull)
								configuration.setBackupConfiguration(null);
							if (this.backupRestoreManager.getFinalTimestamps().size()==0)
							{
								FileTools.deleteDirectory(f);
							}
						}
					}
				}
			}
		}
		private File getDatabaseBackupFileName(File databaseDirectory, DatabaseSchema schema)
		{
			return new File(new File(databaseDirectory, NATIVE_BACKUPS_DIRECTORY_NAME), DatabaseWrapper.getLongPackageName(schema.getPackage()));
		}
		private File getExternalTemporaryDatabaseBackupFileName()
		{
			return new File(new File(databaseDirectory, EXTERNAL_TEMPORARY_BACKUPS_DIRECTORY_NAME), DatabaseWrapper.getLongPackageName(this.configuration.getDatabaseSchema().getPackage()));
		}
		private File getTemporaryDatabaseBackupFileNameForBackupComingFromDistantDatabaseBackup()
		{
			return new File(new File(databaseDirectory, TEMPORARY_BACKUPS_COMING_FROM_DISTANT_DATABASE_BACKUP_DIRECTORY_NAME), DatabaseWrapper.getLongPackageName(this.configuration.getDatabaseSchema().getPackage()));
		}
		private File getTemporaryDatabaseBackupFileNameForBackupComingFromInternalBackup()
		{
			return new File(new File(databaseDirectory, TEMPORARY_BACKUPS_COMING_FROM_INTERNAL_BACKUP_DIRECTORY_NAME), DatabaseWrapper.getLongPackageName(this.configuration.getDatabaseSchema().getPackage()));
		}
		private void checkRestorationNotInProgress() throws DatabaseException {
			if (isCurrentDatabaseInRestorationProcess())
				throw new DatabaseException("The database "+this.configuration.getDatabaseSchema().getPackage().getName()+" is already in a restoration process !");
		}
		@SuppressWarnings("ResultOfMethodCallIgnored")
		private void prepareDatabaseRestorationFromDistantDatabaseBackupChannel() throws DatabaseException {
			checkRestorationNotInProgress();
			File f=getTemporaryDatabaseBackupFileNameForBackupComingFromDistantDatabaseBackup();
			if (f.exists())
				throw new IllegalAccessError();
			FileTools.checkFolderRecursive(f);
			f.mkdir();
			initTemporaryBackupRestoreManagerComingFromDistantBackupRestoreManager(f);

		}
		@SuppressWarnings("ResultOfMethodCallIgnored")
		private void prepareDatabaseRestorationFromInternalBackup() throws DatabaseException {
			checkRestorationNotInProgress();
			File f=getTemporaryDatabaseBackupFileNameForBackupComingFromInternalBackup();
			if (f.exists())
				throw new IllegalAccessError();
			FileTools.checkFolderRecursive(f);
			f.mkdir();
			currentDatabaseInRestorationProcessFromInternalBackup=true;

		}
		@SuppressWarnings("ResultOfMethodCallIgnored")
		private File prepareDatabaseRestorationFromExternalDatabaseBackupAndGetTemporaryBackupDirectoryName() throws DatabaseException {
			checkRestorationNotInProgress();
			File f=getExternalTemporaryDatabaseBackupFileName();
			if (f.exists())
				throw new IllegalAccessError();
			FileTools.checkFolderRecursive(f);
			f.mkdir();
			currentDatabaseInRestorationProcessFromExternalBackup=true;
			return f;
		}

		private void restoreDatabaseToDateUTCFromExternalTemporaryDatabaseBackupAndRemoveBackupFiles(long dateUTCInMs) throws DatabaseException {
			File directory=getExternalTemporaryDatabaseBackupFileName();
			if (!directory.exists())
				throw new DatabaseException("The external database directory does not exists. Please place backup file into directory returned by the function getExternalTemporaryDatabaseBackupFileName");

			DatabaseConfiguration dc=getDatabaseConfiguration(this.configuration.getDatabaseSchema().getPackage());
			if (dc==null)
				throw new DatabaseException("The database "+this.configuration.getDatabaseSchema().getPackage().getName()+" was not loaded !");

			BackupRestoreManager backupRestoreManager =new BackupRestoreManager(DatabaseWrapper.this, directory, dc, true);
			backupRestoreManager.restoreDatabaseToDateUTC(dateUTCInMs);
			FileTools.deleteDirectory(directory);
		}
		private void restorationFromInternalDatabaseBackupFinished() throws DatabaseException {
			File directory=getTemporaryDatabaseBackupFileNameForBackupComingFromInternalBackup();
			if (currentDatabaseInRestorationProcessFromInternalBackup) {
				currentDatabaseInRestorationProcessFromInternalBackup=false;
				FileTools.deleteDirectory(directory);
			}
			else
				throw new DatabaseException("There is no database restoration to cancel with the database "+this.configuration.getDatabaseSchema().getPackage().getName());
		}
		private void cancelRestorationFromExternalDatabaseBackup() throws DatabaseException {
			File directory=getExternalTemporaryDatabaseBackupFileName();

			if (currentDatabaseInRestorationProcessFromExternalBackup) {
				currentDatabaseInRestorationProcessFromExternalBackup=false;
				FileTools.deleteDirectory(directory);
			}
			else
				throw new DatabaseException("There is no database restoration to cancel with the database "+this.configuration.getDatabaseSchema().getPackage().getName());
		}
		private boolean isCurrentDatabaseInRestorationProcessFromExternalBackup()
		{
			return currentDatabaseInRestorationProcessFromExternalBackup;
		}
		private boolean isCurrentDatabaseInRestorationProcessFromCentralDatabaseBackup()
		{
			return temporaryBackupRestoreManagerComingFromDistantBackupManager!=null;
		}
		private boolean isCurrentDatabaseInRestorationProcess()
		{
			return currentDatabaseInRestorationProcessFromExternalBackup || temporaryBackupRestoreManagerComingFromDistantBackupManager!=null || currentDatabaseInRestorationProcessFromInternalBackup;
		}
		void initBackupRestoreManager(DatabaseWrapper wrapper, File databaseDirectory, DatabaseConfiguration configuration) throws DatabaseException {
			if (this.backupRestoreManager!=null)
				return;
			if (databaseLogger!=null)
				databaseLogger.fine("Loading backup/restore: "+configuration);
			if (configuration.getBackupConfiguration()!=null)
			{
				this.backupRestoreManager =new BackupRestoreManager(wrapper, getDatabaseBackupFileName(databaseDirectory, configuration.getDatabaseSchema()), configuration, false);
			}
			checkOldDatabaseBackupsRemoving(wrapper, databaseDirectory, configuration);
			if (databaseLogger!=null)
				databaseLogger.info("Backup/restore loaded: "+configuration);
		}

		void cancelCurrentDatabaseRestorationProcessFromCentralDatabaseBackup() {
			File f=temporaryBackupRestoreManagerComingFromDistantBackupManager.getBackupDirectory();
			temporaryBackupRestoreManagerComingFromDistantBackupManager=null;
			temporaryBackupRestoreManagerChannelComingFromDistantBackupManager=null;
			FileTools.deleteDirectory(f);
		}
	}

	/**
	 * Set the maximum number of events kept into memory during the current
	 * transaction. If this maximum is reached, the transaction is stored into the
	 * disk
	 * 
	 * @param v
	 *            the maximum number of events kept into memory (minimum=1)
	 * 
	 * 
	 */
	public void setMaxTransactionEventsKeptIntoMemory(int v) {
		if (v < 1)
			throw new IllegalArgumentException("v must be greater or equal than 1");
		this.maxTransactionsEventsKeptIntoMemory = v;
	}

	/**
	 * Gets the maximum number of events kept into memory during the current
	 * transaction. If this maximum is reached, the transaction is stored into the
	 * disk
	 * 
	 * @return the maximum number of events kept into memory during the current
	 *         transaction.
	 */
	public int getMaxTransactionEventsKeptIntoMemory() {
		return this.maxTransactionsEventsKeptIntoMemory;
	}

	public int getMaxTransactionEventsKeptIntoMemoryDuringImportInBytes()
	{
		return maxTransactionEventsKeptIntoMemoryDuringImportInBytes;
	}
	
	public void setMaxTransactionEventsKeptIntoMemoryDuringImportInBytes(int maxTransactionEventsKeptIntoMemoryDuringImportInBytes)
	{
		this.maxTransactionEventsKeptIntoMemoryDuringImportInBytes = maxTransactionEventsKeptIntoMemoryDuringImportInBytes;
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
	protected DatabaseWrapper(String _database_name, File databaseDirectory, boolean alwaysDisconnectAfterOnTransaction, boolean loadToMemory,
							  DatabaseConfigurations databaseConfigurations,
							  DatabaseLifeCycles databaseLifeCycles,
							  EncryptionProfileProvider signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,
							  EncryptionProfileProvider encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup,
							  EncryptionProfileProvider protectedEncryptionProfileProviderForAuthenticatedP2PMessages,
							  AbstractSecureRandom secureRandom,
							  boolean createDatabasesIfNecessaryAndCheckIt) throws DatabaseException {

		if (loadToMemory)
		{
			this.alwaysDisconnectAfterOnTransaction =false;
			this.databaseName =_database_name;
			this.loadToMemory=true;
			this.databaseIdentifier =this.databaseName;
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
			this.alwaysDisconnectAfterOnTransaction = alwaysDisconnectAfterOnTransaction;
			/*
			 * if (_sql_connection==null) throw new NullPointerException("_sql_connection");
			 */

			databaseName = _database_name;
			this.databaseDirectory = databaseDirectory;
			Disk disk = null;
			try {
				Partition p = HardDriveDetect.getInstance().getConcernedPartition(databaseDirectory);
				disk = p.getDisk();
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (disk != null)
				databaseIdentifier = disk.toString();
			else
				databaseIdentifier = _database_name;
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
		databaseConfigurationsBuilder=new DatabaseConfigurationsBuilder(databaseConfigurations, this, databaseLifeCycles, signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup, protectedEncryptionProfileProviderForAuthenticatedP2PMessages, secureRandom, createDatabasesIfNecessaryAndCheckIt);

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
		private final DecentralizedValue hookID;
		final Set<String> compatibleDatabasesFromDirectPeer=new HashSet<>();
		final Set<String> compatibleDatabases=new HashSet<>();


		private boolean connected;
		private boolean initializing;
		private boolean transferInProgress = false;
		private boolean localHost;

		boolean isLocalHost()
		{
			return localHost;
		}

		ConnectedPeers(DecentralizedValue hookID, boolean connected) {
			super();
			if (hookID==null)
				throw new NullPointerException();
			this.hookID=hookID;
			this.connected=connected;
			this.localHost=false;
		}
		ConnectedPeers(Record _hook, boolean connected, boolean localHost) {
			this(_hook.getHostID(), connected);
			this.localHost=localHost;
		}



		void setConnected(boolean connected) {
			if (this.connected && !connected) {
				transferInProgress = false;
			}
			this.connected = connected;
			this.initializing = false;
		}

		boolean isConnected()
		{
			return connected;
		}
		boolean isInitializing()
		{
			return initializing;
		}

		void setInitializing(boolean initializing)
		{
			this.initializing = initializing;
		}


		public boolean isTransferInProgress() {
			return transferInProgress;
		}

		public void setTransferInProgress(boolean _transferInProgress) {
			transferInProgress = _transferInProgress;
		}

		public DecentralizedValue getHostID() {
			return hookID;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null)
				return false;
			if (o instanceof ConnectedPeers) {
				return ((ConnectedPeers) o).hookID.equals(this.hookID);
			} else if (o instanceof DatabaseHooksTable.Record) {
				return this.hookID.equals(((Record) o).getHostID());
			}
			return false;
		}

		@Override
		public int hashCode() {
			return hookID.hashCode();
		}

		boolean isConnectable(DatabaseWrapper wrapper) {
			return wrapper.getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurations(hookID).allMatch(compatibleDatabasesFromDirectPeer::contains)
					&& compatibleDatabasesFromDirectPeer.stream().allMatch(p-> wrapper.getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurations(hookID).anyMatch(p::equals));
			/*return compatibleDatabasesFromDirectPeer.size()==compatibleDatabasesFromDirectPeer.stream().noneMatch(d -> wrapper.getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurations(hookID).noneMatch(d::equals))
					&& wrapper.getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurations(hookID).noneMatch(d -> compatibleDatabasesFromDirectPeer.stream().noneMatch(d::equals));*/
		}
	}
	static class ConnectedPeersWithCentralBackup {
		private final DecentralizedValue hookID;
		final Set<String> compatibleDatabasesFromCentralDatabaseBackup=new HashSet<>();
		Map<String, Long> lastValidatedDistantTransactionIDPerDatabaseFromDatabaseBackup=new HashMap<>();
		private final ValidatedIDPerDistantHook validatedIDPerDistantHook=new ValidatedIDPerDistantHook();
		private boolean otherBackupDatabasePartsSynchronizingWithCentralDatabaseBackup=false;
		private boolean otherBackupMetaDataDatabasePartsSynchronizingWithCentralDatabaseBackup=false;
		//private boolean synchronizationWithCentralBackupSuspended=true;

		ConnectedPeersWithCentralBackup(Record _hook) {
			super();
			hookID = _hook.getHostID();
		}

		@Override
		public String toString() {
			return getClass().getName() + "@" + Integer.toHexString(super.hashCode());
		}

		@Override
		public boolean equals(Object o) {
			if (o == null)
				return false;
			if (o instanceof ConnectedPeersWithCentralBackup) {
				return ((ConnectedPeersWithCentralBackup) o).hookID.equals(this.hookID);
			} else if (o instanceof DatabaseHooksTable.Record) {
				return this.hookID.equals(((Record) o).getHostID());
			}
			return false;
		}

		@Override
		public int hashCode() {
			return hookID.hashCode();
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
		/*long getFirstTransactionID(String packageString)
		{
			TreeSet<DatabaseBackupMetaDataPerFile> ts=metaData.get(packageString);
			if (ts==null || ts.size()==0)
				return Long.MAX_VALUE;
			else {
				for (DatabaseBackupMetaDataPerFile t : ts) {
					Long l = t.getFirstTransactionID();
					if (l != null)
						return l;
				}
				return Long.MAX_VALUE;
			}
		}*/
		boolean addMetaData(String packageString, DatabaseBackupMetaDataPerFile m)  {
			if (packageString==null)
				throw new NullPointerException();
			if (packageString.trim().length()==0)
				throw new IllegalArgumentException();
			TreeSet<DatabaseBackupMetaDataPerFile> ts = metaData.computeIfAbsent(packageString, k -> new TreeSet<>());
			for (DatabaseBackupMetaDataPerFile me : ts) {
				if (me.timeStampUTC == m.timeStampUTC)
					return false;
				if (me.timeStampUTC > m.timeStampUTC)
					break;
			}
			ts.add(m);
			return true;
		}
		/*long getFirstFileTimestamp(String packageString) {
			TreeSet<DatabaseBackupMetaDataPerFile> ts=metaData.get(packageString);

			if (ts==null || ts.size()==0)
				return Long.MIN_VALUE;
			else {
				return ts.first().getFileTimestampUTC();
			}
		}*/
		/*long getLastFileTimestamp(String packageString) {
			TreeSet<DatabaseBackupMetaDataPerFile> ts=metaData.get(packageString);

			if (ts==null || ts.size()==0)
				return Long.MIN_VALUE;
			else {
				return ts.last().getFileTimestampUTC();
			}
		}*/
		long getLastTransactionID(String packageString)  {
			TreeSet<DatabaseBackupMetaDataPerFile> ts=metaData.get(packageString);

			if (ts!=null) {
				for (Iterator<DatabaseBackupMetaDataPerFile> it=ts.descendingIterator();it.hasNext();) {
					Long l=it.next().getLastTransactionID();
					if (l!=null)
						return l;
				}
				//throw DatabaseException.getDatabaseException(new IllegalAccessException("ts.size="+ts.size()));
			}
			return Long.MIN_VALUE;
		}
		FileCoordinate getTimestampTransactionToAsk(String packageString, long lastDistantTransactionID, long lastValidatedDistantTransactionID)  {
			if (lastDistantTransactionID<=lastValidatedDistantTransactionID)
				return null;

			TreeSet<DatabaseBackupMetaDataPerFile> ts=metaData.get(packageString);

			if (ts!=null) {

				Iterator<DatabaseBackupMetaDataPerFile> it=ts.descendingIterator();
				DatabaseBackupMetaDataPerFile previous=it.hasNext()?it.next():null;
				if (previous==null)
					return new FileCoordinate(Long.MAX_VALUE, FileCoordinate.Boundary.UPPER_LIMIT);
				Long lastID=previous.getLastTransactionID();
				if (lastID!=null && lastID<lastDistantTransactionID) {
					return new FileCoordinate(previous.timeStampUTC, FileCoordinate.Boundary.LOWER_LIMIT);
				}
				Long firstTID=previous.getFirstTransactionID();

				if (firstTID!=null)
					firstTID=firstTID-1;
				while (it.hasNext()) {
					DatabaseBackupMetaDataPerFile actual=it.next();
					Long lastTID=actual.getLastTransactionID();
					if (lastTID==null) {
						previous=actual;
					}
					else {
						/*if (firstTID==null) {
							return new FileCoordinate(actual.timeStampUTC, FileCoordinate.Boundary.LOWER_LIMIT);
						}*/
						if (firstTID!=null && lastTID < firstTID)
							return new FileCoordinate(previous.timeStampUTC, FileCoordinate.Boundary.UPPER_LIMIT);
						previous = actual;
						firstTID = previous.getFirstTransactionID()-1;

					}
				}
				if (firstTID==null || firstTID>lastValidatedDistantTransactionID) {
					return new FileCoordinate(previous.timeStampUTC, FileCoordinate.Boundary.UPPER_LIMIT);
				}

				return null;
			}
			return new FileCoordinate(Long.MAX_VALUE, FileCoordinate.Boundary.UPPER_LIMIT);
		}

		long getFileUTCToTransfer(String packageString, long lastValidatedDistantTransactionID) {
			TreeSet<DatabaseBackupMetaDataPerFile> ts=metaData.get(packageString);
			if (ts==null)
				return Long.MIN_VALUE;
			++lastValidatedDistantTransactionID;
			for (DatabaseBackupMetaDataPerFile m : ts)
			{
				Long fid=m.getFirstTransactionID();
				if (fid==null) {
					continue;
				}
				if (lastValidatedDistantTransactionID>=fid && lastValidatedDistantTransactionID<=m.getLastTransactionID()) {
					return m.getFileTimestampUTC();
				}

			}
			return Long.MIN_VALUE;
		}

		void validateLastTransaction(String packageString, long lastValidatedTransaction) {
			TreeSet<DatabaseBackupMetaDataPerFile> ts=metaData.get(packageString);
			if (ts==null)
				return;
			for (Iterator<DatabaseBackupMetaDataPerFile> it = ts.iterator(); it.hasNext(); ) {
				Long l=it.next().getLastTransactionID();
				if (l==null || l <= lastValidatedTransaction) {
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
		//protected final Set<DecentralizedValue> initializingHooks = new HashSet<>();
		protected final HashMap<DecentralizedValue, ConnectedPeersWithCentralBackup> initializedHooksWithCentralBackup = new HashMap<>();
		//protected final HashMap<DecentralizedValue, LastValidatedLocalAndDistantID> suspendedHooksWithCentralBackup = new HashMap<>();
		//protected final HashMap<DecentralizedValue, Long> lastValidatedTransactionIDFromCentralBackup = new HashMap<>();
		//private final HashMap<DecentralizedValue, ValidatedIDPerDistantHook> validatedIDPerDistantHook =new HashMap<>();
		protected boolean centralBackupInitialized=false;
		protected boolean centralBackupAvailable=false;
		private final Condition newEventCondition=locker.newCondition();
		private boolean extendedTransactionInProgress=false;
		private long lastTransactionID=Long.MIN_VALUE;
		/*private final TreeSet<DatabaseBackupToIncorporateFromCentralDatabaseBackup> differedDatabaseBackupToIncorporate=new TreeSet<>();
		private final HashMap<DatabaseBackupToIncorporateFromCentralDatabaseBackup, InputStreamGetter> differedDatabaseBackupToIncorporateInputStreams=new HashMap<>();*/
		final Set<String> backupDatabasePartsSynchronizingWithCentralDatabaseBackup=new HashSet<>();
		//private final Set<DecentralizedValue> otherBackupDatabasePartsSynchronizingWithCentralDatabaseBackup=new HashSet<>();
		private boolean sendIndirectTransactions;

		private long minFilePartDurationBeforeBecomingFinalFilePart=Long.MAX_VALUE;

		DatabaseSynchronizer() {
		}

		public void loadCentralDatabaseClassesIfNecessary() throws DatabaseException {
			if (!sql_database.containsKey(CentralDatabaseBackupReceiver.class.getPackage()))
				loadDatabase(new DatabaseConfiguration(new DatabaseSchema(CentralDatabaseBackupReceiver.class.getPackage(), internalCentralDatabaseClassesList)), null);
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
			DatabaseHooksTable.Record r=getDatabaseHooksTable().getLocalDatabaseHost();
			return r != null && isInitialized(r.getHostID());
		}
		void sendAvailableDatabaseTo(DecentralizedValue hostDestination) throws DatabaseException {
			sendAvailableDatabaseTo(hostDestination, getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurationsString(), getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurationsString(hostDestination));
		}
		void sendAvailableDatabaseTo(DecentralizedValue hostDestination, Set<String> compatibleDatabases, Set<String> compatibleDatabasesWithDestinationPeer) throws DatabaseException {

			addNewDatabaseEvent(new CompatibleDatabasesP2PMessage(compatibleDatabases, compatibleDatabasesWithDestinationPeer,getLocalHostID(), hostDestination));
		}

		void sendAvailableDatabaseToCentralDatabaseBackup() throws DatabaseException {
			sendAvailableDatabaseToCentralDatabaseBackup(getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurationsString());
		}
		void sendAvailableDatabaseToCentralDatabaseBackup(Set<String> packages) throws DatabaseException {
			if (centralBackupInitialized) {
				addNewDatabaseEvent(new CompatibleDatabasesMessageDestinedToCentralDatabaseBackup(packages, getLocalHostID(), DatabaseWrapper.this.getDatabaseConfigurationsBuilder().getSecureRandom(), DatabaseWrapper.this.getDatabaseConfigurationsBuilder().getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup()));
			}
		}

		void broadcastAvailableDatabase() throws DatabaseException {
			if (!isInitialized())
				return;
			Set<String> p=getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurationsString();
			for (ConnectedPeers cp : initializedHooks.values())
			{
				if (cp.isLocalHost())
					continue;
				if (!cp.isConnectable(DatabaseWrapper.this) && cp.isConnected())
					disconnectHook(cp.hookID);
				sendAvailableDatabaseTo(cp.getHostID(), p, getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurationsString(cp.hookID));

			}
			sendAvailableDatabaseToCentralDatabaseBackup(p);
		}

		void notifyNewAuthenticatedMessage(AuthenticatedP2PMessage authenticatedP2PMessage) throws DatabaseException {
			lockWrite();
			try {
				ConnectedPeers cp = initializedHooks.get(authenticatedP2PMessage.getHostDestination());
				if (cp != null)
					addNewDatabaseEvent((DatabaseEvent) authenticatedP2PMessage);
				if (isInitializedWithCentralBackup())
					addNewDatabaseEvent(new IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup(Collections.singletonList(authenticatedP2PMessage), databaseConfigurationsBuilder.getSecureRandom(), databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup()));
				/*else {
					getDatabaseHooksTable().getLocalDatabaseHost().addAuthenticatedP2PMessageToSendToCentralDatabaseBackup(authenticatedP2PMessage, getDatabaseHooksTable());
				}*/
			}
			finally {
				unlockWrite();
			}
		}

		void checkInitLocalPeer() throws DatabaseException {
			DecentralizedValue dv=databaseConfigurationsBuilder.getConfigurations().getLocalPeer();
			if (dv!=null && !isInitialized(dv)) {
				initLocalHostID(dv, databaseConfigurationsBuilder.getConfigurations().isPermitIndirectSynchronizationBetweenPeers());
			}
		}

		public boolean isInitialized(DecentralizedValue hostID) {
			lockRead();
			try {
				ConnectedPeers cp=initializedHooks.get(hostID);
				return cp != null && cp.isConnected();
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

		public DatabaseEvent nextEvent() throws DatabaseException {
			
			try
			{
				lockWrite();
				if (this.extendedTransactionInProgress)
					return null;

				checkForNewCentralBackupDatabaseEvent();
				if (events.isEmpty())
					return null;
				else {
					DatabaseEvent e = events.removeFirst();
					if (e instanceof AuthenticatedP2PMessage)
					{
						((AuthenticatedP2PMessage) e).messageSent(DatabaseWrapper.this);
					}
					if (events.isEmpty())
						canNotify = true;
					if (networkLogger!=null) {
						networkLogger.finer("Send message " + e);
					}
					return e;
				}
			}
			finally
			{

				unlockWrite();
			}
		}

		/*Stream<String> getSynchronizedDatabases()
		{
			return getDatabaseConfigurationsBuilder().getConfigurations().getConfigurations()
					.stream()
					.filter(dc -> dc.getSynchronizationType()!= DatabaseConfiguration.SynchronizationType.NO_SYNCHRONIZATION &&
							DatabaseWrapper.this.sql_database.values().stream().anyMatch(sd -> sd.configuration.getDatabaseSchema().getPackage().equals(dc.getDatabaseSchema().getPackage())) )
					.map(dc -> dc.getDatabaseSchema().getPackage().getName());
		}*/

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

		public DatabaseEvent waitNextEvent() throws InterruptedException, DatabaseException {

			try {
				lockWrite();
				DatabaseEvent de;
				while ((de = nextEvent()) == null) {
					//noinspection ResultOfMethodCallIgnored
					newEventCondition.await(minFilePartDurationBeforeBecomingFinalFilePart, TimeUnit.MILLISECONDS);
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
		void initLocalHostID(DecentralizedValue localHostID, boolean sendIndirectTransactions) throws DatabaseException {
			DatabaseHooksTable.Record local = getDatabaseHooksTable().getLocalDatabaseHost();
			if (local != null && !local.getHostID().equals(localHostID))
				throw new DatabaseException("The given local host id "+localHostID+" is different from the stored local host id "+local.getHostID()+" !");
			if (local == null) {
				local=getDatabaseHooksTable().initLocalHook(localHostID);
			}
			getDatabaseTransactionEventsTable().cleanTmpTransactions();
			
			try {
				lockWrite();
				this.sendIndirectTransactions=sendIndirectTransactions;
                if (initializedHooks.containsKey(local.getHostID())) {
					throw DatabaseException.getDatabaseException(
							new IllegalAccessException("Local hostID " + localHostID + " already initialized !"));
				}

				initializedHooks.put(localHostID, new ConnectedPeers(local, true, true));
				if (networkLogger!=null)
					networkLogger.info("Local peer "+localHostID+" connected !");

				canNotify = true;
			}
			finally
			{
				unlockWrite();
			}
			
		}





		/*public void addHookForLocalDatabaseHost(DecentralizedValue hostID, Package ...databasePackages)
				throws DatabaseException {
			HashMap<String, Boolean> packages = new HashMap<>();
			for (Package p : databasePackages) {
				packages.put(p.getName(), false);
			}
			getDatabaseHooksTable().addHooks(hostID, true, new ArrayList<>(),
					packages);
		}*/

		/*public void askForHookAddingAndSynchronizeDatabase(DecentralizedValue hostID,
														   boolean replaceDistantConflictualRecords, Package... packages) throws DatabaseException {
			ArrayList<String> packagesString = new ArrayList<>();
			for (Package p : packages) {
				Database d=sql_database.get(p);
				if (d==null)
					throw new IllegalArgumentException("The database "+p+" is not loaded");
				String ps=p.getName();
				runSynchronizedTransaction(new SynchronizedTransaction<Void>() {
					@Override
					public Void run() throws Exception {
						DatabaseTable.Record r=getDatabaseTable().getRecord("databasePackageName", ps);
						if (r==null)
							throw new IllegalAccessError();
						getDatabaseTable().updateRecord(r, "toSynchronizeWithCentralDatabaseBackup", !d.getConfiguration().getDatabaseConfigurationParameters().isSynchronizedWithCentralBackupDatabase());
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
					public void initOrReset()  {

					}
				});

				if ()
				{

				}
				else
				{

				}
				packagesString.add(ps);
			}

		}*/

		/*public HookAddRequest askForHookAddingAndSynchronizeDatabase(DecentralizedValue hostID,
				boolean replaceDistantConflictualRecords, Package... packages) throws DatabaseException {
			ArrayList<String> packagesString = new ArrayList<>();
			for (Package p : packages)
				packagesString.add(p.getName());
			return askForHookAddingAndSynchronizeDatabase(hostID, true, replaceDistantConflictualRecords,
					packagesString);
		}

		private HookAddRequest askForHookAddingAndSynchronizeDatabase(DecentralizedValue hostID,
				boolean mustReturnMessage, boolean replaceDistantConflictualRecords, Set<String> packages)
				throws DatabaseException {
			if (packages.size()>MAX_PACKAGE_TO_SYNCHRONIZE)
				throw new DatabaseException("The number of packages to synchronize cannot be greater than "+MAX_PACKAGE_TO_SYNCHRONIZE);

			if (hostAlreadySynchronized.size()>=MAX_DISTANT_PEERS)
				throw new DatabaseException("The maximum number of distant peers ("+MAX_DISTANT_PEERS+") is reached");
			return new HookAddRequest(getDatabaseHooksTable().getLocalDatabaseHost().getHostID(), hostID, packages,
					hostAlreadySynchronized, mustReturnMessage, replaceDistantConflictualRecords);
		}*/




		void receivedHookSynchronizeRequest(HookSynchronizeRequest hookSynchronizeRequest) throws DatabaseException {
			Map<String, Boolean> packagesToSynchronize=hookSynchronizeRequest.getPackagesToSynchronize(getLocalHostID());
			lockRead();
			try {
				if (packagesToSynchronize.keySet().stream().anyMatch(p -> getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurations().noneMatch(p::equals)))
					return;
			}
			finally {
				unlockRead();
			}


			getDatabaseHooksTable().addHooks(packagesToSynchronize,
					hookSynchronizeRequest.getConcernedPeers(), !hookSynchronizeRequest.getHostSource().equals(getLocalHostID()));
			/*if (hookSynchronizeRequest.getBackRequest()!=null)
			{
				lockRead();
				try {
					DatabaseHooksTable.Record r=getDatabaseHooksTable().getHook(hookSynchronizeRequest.getBackRequest().getHostDestination());
					r.offerNewAuthenticatedP2PMessage(getDatabaseHooksTable(), hookSynchronizeRequest.getBackRequest());
				}
				finally {
					unlockRead();
				}

			}*/
			isReliedToDistantHook();
		}
		private void received(HookRemoveRequest m) throws DatabaseException {
			getDatabaseHooksTable().removeHook(false, m.getRemovedHookID());
		}

		void sendP2PConnexionInitializationMessage(DatabaseHooksTable.Record peer) throws DatabaseException {
			if (hookCannotBeConnected(peer)) {
				return;
			}
			ConnectedPeers cp=initializedHooks.get(peer.getHostID());
			if (cp!=null) {
				if (cp.isInitializing())
					return;
				if (cp.isConnected())
					return;
				cp.setInitializing(true);
				//initializingHooks.add(peer.getHostID());
				//new IllegalAccessError().printStackTrace();
				addNewDatabaseEvent(new P2PConnexionInitialization(getLocalHostID(), peer.getHostID(), peer.getLastValidatedDistantTransactionID()));
			}
		}

		void connectPeerIfAvailable(DatabaseHooksTable.Record concernedSenderHook) throws DatabaseException {
			lockWrite();
			try {
				ConnectedPeers cp=this.initializedHooks.get(concernedSenderHook.getHostID());
				if (cp!=null)
				{
					sendP2PConnexionInitializationMessage(concernedSenderHook);
				}
			}
			finally {
				unlockWrite();
			}


		}

		private void received(AuthenticatedP2PMessage m) throws MessageExternalizationException, DatabaseException {
			DatabaseHooksTable.Record localDatabaseHost = getDatabaseHooksTable().getLocalDatabaseHost();
			if (localDatabaseHost == null)
				throw new DatabaseException("Function must be called before addHookForLocalDatabaseHost.");
			if (m.getHostDestination().equals(getLocalHostID()))
			{
				Integrity i=m.checkHashAndSignatures(databaseConfigurationsBuilder.getProtectedSignatureProfileProviderForAuthenticatedP2PMessages());
				if (i!=Integrity.OK) {
					throw new MessageExternalizationException(i);
				}
				DatabaseHooksTable.Record r =getDatabaseHookRecord(m.getHostSource(), false);
				if (r==null)
				{
					if (m instanceof HookSynchronizeRequest) {
						receivedHookSynchronizeRequest((HookSynchronizeRequest) m);
					}
					else
						return;
					r =getDatabaseHookRecord(m.getHostSource());
					r.validateDistantAuthenticatedP2PMessage(m, getDatabaseHooksTable());
					checkConnexionsToInit(r);
					/*if (r.hasNoAuthenticatedMessagesQueueToSend())
						sendP2PConnexionInitializationMessage(r);*/
				}
				else
				{
					if (r.validateDistantAuthenticatedP2PMessage(m, getDatabaseHooksTable())) {
						if (m instanceof HookSynchronizeRequest)
							receivedHookSynchronizeRequest((HookSynchronizeRequest) m);
						else if (m instanceof HookDesynchronizeRequest)
							receivedHookDesynchronizeRequest((HookDesynchronizeRequest) m);
						else if (m instanceof HookRemoveRequest)
							received((HookRemoveRequest)m);
						else if (m instanceof RestorationOrderMessage)
							receivedRestorationOrderMessage((RestorationOrderMessage)m);
						else
							throw new MessageExternalizationException(Integrity.FAIL);
					}
				}

			}
			else
				throw new MessageExternalizationException(Integrity.FAIL);
		}

		private void cleanTransactionsAfterRestoration(String databasePackage, long timeUTCOfRestorationInMs, Long transactionToDeleteUpperLimitUTC, boolean launchRestoration, boolean chooseNearestBackupIfNoBackupMatch) throws DatabaseException {
			if (transactionToDeleteUpperLimitUTC!=null)
				getDatabaseTransactionEventsTable().removeRecordsWithCascade( "concernedDatabasePackage=%c and timeUTC<=%l", "c", databasePackage, "l", transactionToDeleteUpperLimitUTC);
			else
				getDatabaseTransactionEventsTable().removeRecordsWithCascade( "concernedDatabasePackage=%c", "c", databasePackage);
			//getDatabaseDistantTransactionEvent().removeAllRecordsWithCascade();
			getDatabaseHooksTable().actualizeLastTransactionID(Collections.emptyList());
			if (launchRestoration)
			{
				getDatabaseConfigurationsBuilder().restoreGivenDatabaseStringToOldVersion(databasePackage, timeUTCOfRestorationInMs, false , chooseNearestBackupIfNoBackupMatch, false);
			}

		}

		void receivedRestorationOrderMessage(RestorationOrderMessage m) throws DatabaseException {
			runSynchronizedTransaction(new SynchronizedTransaction<Object>() {
				@Override
				public Object run() throws Exception {
					cleanTransactionsAfterRestoration(m.getDatabasePackage(), m.getTimeUTCOfRestorationInMs(), null, m.getHostThatApplyRestoration().equals(getLocalHostID()) && !m.getHostSource().equals(getLocalHostID()), m.isChooseNearestBackupIfNoBackupMatch());
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



		void removeHook(DecentralizedValue hostID) throws DatabaseException {
			if (isInitialized(hostID))
				disconnectHook(hostID);
			getDatabaseHooksTable().removeHook(true, hostID);

		}


		void receivedHookDesynchronizeRequest(HookDesynchronizeRequest hookDesynchronizeRequest) throws DatabaseException {
			getDatabaseHooksTable().desynchronizeDatabases(hookDesynchronizeRequest.getPackagesToUnsynchronize(), hookDesynchronizeRequest.getConcernedPeers());
			isReliedToDistantHook();
		}

		/*void desynchronizeDatabases(Set<String> packages) throws DatabaseException {
			getDatabaseHooksTable().desynchronizeDatabases(packages);
			isReliedToDistantHook();
		}*/
		
		@SuppressWarnings("UnusedReturnValue")
        protected boolean isReliedToDistantHook() throws DatabaseException
		{
			return hasOnePeerSynchronized = getDatabaseHooksTable().hasRecordsWithAllFields("concernsDatabaseHost", Boolean.FALSE);
		}
		

		long getLastValidatedDistantIDSynchronization(DecentralizedValue hostID) throws DatabaseException {
			return getDatabaseHookRecord(hostID).getLastValidatedDistantTransactionID();

		}

		/*long getLastValidatedLocalIDSynchronization(DecentralizedValue hostID) throws DatabaseException {


			return getDatabaseHookRecord(hostID).getLastValidatedLocalTransactionID();

		}*/



		void validateLastSynchronization(DecentralizedValue hostID, long lastTransferredTransactionID, boolean fromCentral)
				throws DatabaseException {
			if (hostID == null)
				throw new NullPointerException("hostID");
			if (!isInitialized())
				throw new DatabaseException("The Synchronizer must be initialized (initLocalHostID function) !");
			DecentralizedValue peer=null;
			ConnectedPeers cp=null;
			try {
				lockWrite();

				if (fromCentral) {
					ConnectedPeersWithCentralBackup cpcb= initializedHooksWithCentralBackup.get(hostID);
					if (cpcb!=null)
						peer=cpcb.hookID;
				}
				else {
					cp = initializedHooks.get(hostID);
					if (cp != null && cp.isConnected())
						peer = cp.hookID;
				}
			}
			finally
			{
				unlockWrite();
			}
			if (peer == null)
				throw DatabaseException.getDatabaseException(
						new IllegalArgumentException("The host ID " + hostID + " has not been initialized !"));
			DatabaseHooksTable.Record r = getDatabaseHooksTable().getHook(hostID);

			if (r.concernsLocalDatabaseHost())
				throw new DatabaseException("The given host ID correspond to the local database host !");

			if (!fromCentral && (lastTransferredTransactionID>=0 || r.getLastValidatedLocalTransactionID()<0)) {
				/*if (r.getLastValidatedLocalTransactionID() > lastTransferredTransactionID) {
					throw new DatabaseException("The given transfer ID limit " + lastTransferredTransactionID
								+ " is lower than the stored transfer ID limit " + r.getLastValidatedLocalTransactionID() + ". LastValidatedDistantTransactionID=" + r.getLastValidatedDistantTransactionID() + " ; hook=" + hostID + " ; localHostID=" + getLocalHostID() + " ; last local transaction id=" + getTransactionIDTable().getLastTransactionID());
				}
				else {*/


					long l = getDatabaseTransactionsPerHostTable().validateTransactions(r, lastTransferredTransactionID);
					if (l < lastTransferredTransactionID)
						throw new IllegalAccessError("l=" + l + "; lastTransferredTransactionID=" + lastTransferredTransactionID);
					if (l != lastTransferredTransactionID) {
						addNewDatabaseEvent(new LastIDCorrection(getLocalHostID(),
								hostID, l));
					}
					cp.setTransferInProgress(false);
					synchronizedDataIfNecessary(cp);
				//}
			}
			synchronizeMetaData();
		}

		void sendLastValidatedIDIfConnected(DatabaseHooksTable.Record hook) throws DatabaseException {
			
			try {
				lockWrite();
				ConnectedPeers cp=initializedHooks.get(hook.getHostID());
				if (cp!=null && cp.isConnected()) {
					addNewDatabaseEvent(new TransactionConfirmationEvents(
							getLocalHostID(), hook.getHostID(),
							hook.getLastValidatedDistantTransactionID()));
				}

			}
			finally
			{
				unlockWrite();
			}
			

		}



		private void synchronizeMetaData() throws DatabaseException {
			Map<DecentralizedValue, Long> lastIds = getDatabaseHooksTable()
					.getLastValidatedLocalTransactionIDs();

			try {
				lockWrite();
				DecentralizedValue localHostID=getLocalHostID();
				for (DecentralizedValue host : lastIds.keySet()) {
					if (/*!localHostID.equals(host) && */isInitialized(host)) {
                        Map<DecentralizedValue, Long> map = new HashMap<>(lastIds);
						// map.remove(hostID);
						map.remove(host);
						if (map.size() > 0) {
							addNewDatabaseEvent(new DatabaseTransactionsIdentifiersToSynchronize(
									localHostID, host, map));
						}
					}
				}

				if (centralBackupInitialized && initializedHooksWithCentralBackup.size()>0) {
					for (ConnectedPeersWithCentralBackup cp : initializedHooksWithCentralBackup.values()) {
						checkMetaDataUpdate(cp.hookID);
					}
				}
			} finally
			{
				unlockWrite();
			}
			
		}

		void notifyNewTransactionsIfNecessary() throws DatabaseException {
			final long lastID = getTransactionIDTable().getLastTransactionID();

			try {
				lockWrite();

				getDatabaseHooksTable().getRecords(new Filter<DatabaseHooksTable.Record>() {

					@Override
					public boolean nextRecord(Record _record) throws DatabaseException {
						ConnectedPeers cp = initializedHooks.get(_record.getHostID());
						if (cp != null && cp.isConnected() && !_record.concernsLocalDatabaseHost()) {
							if (!cp.isTransferInProgress())
							{
								if (lastID > _record.getLastValidatedLocalTransactionID()) {
									cp.setTransferInProgress(true);
									addNewDatabaseEvent(new DatabaseEventsToSynchronizeP2P(
											getLocalHostID(), _record, lastID,
											maxTransactionsToSynchronizeAtTheSameTime));
								}

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


		@SuppressWarnings("UnusedReturnValue")
        private long synchronizedDataIfNecessary(ConnectedPeers peer) throws DatabaseException {
			if (peer.isTransferInProgress())
				return -1;
			long lastID = getTransactionIDTable().getLastTransactionID();
			
			try {
				lockWrite();
				DatabaseHooksTable.Record hook = getDatabaseHooksTable().getHook(peer.getHostID());
				if (lastID > hook.getLastValidatedLocalTransactionID() && !peer.isTransferInProgress()) {
					peer.setTransferInProgress(true);
					addNewDatabaseEvent(new DatabaseEventsToSynchronizeP2P(
							getLocalHostID(), hook, lastID,
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
				/*boolean notify=false;
				if (isInitialized(hostDestination)) {
					notify=true;
					events.add(new TransactionConfirmationEvents(
							getLocalHostID(), hostDestination,
							getDatabaseHookRecord(hostDestination).getLastValidatedDistantTransactionID()));
				}*/
				ConnectedPeersWithCentralBackup cp=this.initializedHooksWithCentralBackup.get(hostDestination);
				if (cp!=null)
				{
					cp.validatedIDPerDistantHook.validateLastTransaction(packageString, lastValidatedTransaction);
				}
				if (centralBackupInitialized) {
					try {
						addNewDatabaseEvent(new LastValidatedDistantTransactionDestinedToCentralDatabaseBackup(getLocalHostID(), hostDestination, lastValidatedTransaction, databaseConfigurationsBuilder.getSecureRandom(), databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup()));
					} catch (IOException e) {
						throw DatabaseException.getDatabaseException(e);
					}
				}
				/*if (notify)
					notifyNewEvent();*/

			}
			finally {
				unlockWrite();
			}

		}
		private void addNewAuthenticatedDatabaseEvents(DatabaseHooksTable.Record hook) throws DatabaseException {
			lockWrite();
			try
			{
				/*if (!hook.hasNoAuthenticatedMessagesQueueToSend())
					disconnectHook(hook.getHostID());*/
				List<DatabaseEvent> authenticatedMessages=tryToMerge(hook.getAuthenticatedMessagesQueueToSend(initializedHooks));
				if (authenticatedMessages.size()>0) {

					boolean add = true;
					for (ListIterator<DatabaseEvent> it = events.listIterator(events.size()); it.hasPrevious(); ) {
						if (it.previous() instanceof AuthenticatedP2PMessage) {
							it.next();
							for (DatabaseEvent a : authenticatedMessages)
								it.add(a);
							add = false;
							break;
						}
					}
					if (add) {
						for (DatabaseEvent a : authenticatedMessages) {
							events.addFirst(a);
						}
					}
					notifyNewEvent();
				}

			}
			finally {
				unlockWrite();
			}
		}

		void deleteMessage(DatabaseEvent event) throws DatabaseException {
			if (event instanceof AuthenticatedP2PMessage)
				getDatabaseHooksTable().authenticatedMessageSent((AuthenticatedP2PMessage)event);
		}
		List<DatabaseEvent> tryToMerge(List<?> c) throws DatabaseException {
			ArrayList<DatabaseEvent> res=new ArrayList<>();
			if (c==null || c.size()==0)
				return res;
			ArrayList<DatabaseEvent> messagesToRemove=new ArrayList<>();


			for (Object e : c) {
				boolean add=true;
				for (ListIterator<DatabaseEvent> it = events.listIterator(events.size()); it.hasPrevious(); ) {
					DatabaseEvent de = it.previous();
					DatabaseEvent.MergeState ms = de.tryToMerge((DatabaseEvent)e);
					if (ms == DatabaseEvent.MergeState.DELETE_BOTH || ms == DatabaseEvent.MergeState.DELETE_OLD) {

						messagesToRemove.add(de);
						it.remove();
					}
					if (ms == DatabaseEvent.MergeState.DELETE_BOTH || ms == DatabaseEvent.MergeState.DELETE_NEW) {

						messagesToRemove.add((DatabaseEvent)e);
						add=false;
					}
				}
				if (add)
					res.add((DatabaseEvent)e);
			}
			for (DatabaseEvent m : messagesToRemove)
				deleteMessage(m);
			return res;
		}

		void addNewDatabaseEvent(DatabaseEvent e) throws DatabaseException {
			if (e == null)
				throw new NullPointerException("e");


			try {
				lockWrite();
				if (e instanceof AuthenticatedMessageDestinedToCentralDatabaseBackup)
				{
					((AuthenticatedMessageDestinedToCentralDatabaseBackup) e).generateAndSetSignatures(databaseConfigurationsBuilder.getSecureRandom(), databaseConfigurationsBuilder.getSignatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup());
				}
				List<DatabaseEvent> r = tryToMerge(Collections.singletonList(e));
				boolean add=r.size()>0;


				if (add) {
					assert r.size()==1;
					e=r.get(0);

					if (e instanceof AuthenticatedP2PMessage) {
						for (ListIterator<DatabaseEvent> it=events.listIterator(events.size());it.hasPrevious();)
						{
							if (it.previous() instanceof AuthenticatedP2PMessage)
							{
								it.next();
								it.add(e);
								add=false;
								break;
							}
						}
						if (add)
							events.addFirst(e);
					}
					else
						events.addLast(e);
					notifyNewEvent();
				}
			}
			finally
			{
				unlockWrite();
			}
			
		}
		public DecentralizedValue getLocalHostID() throws DatabaseException {
			DatabaseHooksTable.Record r= getDatabaseHooksTable().getLocalDatabaseHost();
			if (r==null)
				return null;
			else
				return r.getHostID();
		}

		void resetSynchronizerAndRemoveAllHosts() throws DatabaseException {
			resetSynchronizerAndGetAllHosts();
		}
		@SuppressWarnings("UnusedReturnValue")
		private Collection<DatabaseHooksTable.Record> resetSynchronizerAndGetAllHosts() throws DatabaseException {
			if (getLocalHostID()==null)
				return null;
			disconnectAll();
			Collection<DatabaseHooksTable.Record> r= getDatabaseHooksTable().resetAllHosts();
			getDatabaseTransactionEventsTable().resetAllTransactions();
			return r;
		}
		/*private void restoreHosts(Collection<DatabaseHooksTable.Record> hosts, HashMap<String, Boolean> replaceDistantConflictualRecords) throws DatabaseException {
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
			Set<String> lsp=local.getDatabasePackageNames();
			{
				HashMap<String, Boolean> hm=new HashMap<>();
				lsp.forEach(v->hm.put(v, false));
				getDatabaseHooksTable().addHooks(local.getHostID(), true, new ArrayList<>(),
						hm);
			}
			{
				HashMap<String, Boolean> hm=new HashMap<>();
				lsp.forEach(v->{
					Boolean replace=replaceDistantConflictualRecords.get(v);
					hm.put(v, replace!=null && replace);
				});
				for (DatabaseHooksTable.Record r : hosts)
					getDatabaseHooksTable().addHooks(r.getHostID(), false,
							 new ArrayList<>(), hm);
			}
			isReliedToDistantHook();

		}*/

		void disconnectAll() throws DatabaseException {
			DecentralizedValue hostID=getLocalHostID();
			if (isInitialized(hostID))
				disconnectHook(hostID);
			disconnectAllHooksFromThereBackups();
		}

		
		void disconnectHook(final DecentralizedValue hostID) throws DatabaseException {
			if (hostID == null)
				throw new NullPointerException("hostID");

			//LastValidatedLocalAndDistantID backupChannelInitializationMessageFromCentralDatabaseBackup=null;
			DatabaseHooksTable.Record hook = null;
			try {
				lockWrite();
				ConnectedPeers peer = initializedHooks.get(hostID);

				if (peer != null && peer.isConnected())
					hook = getDatabaseHooksTable().getHook(peer.getHostID());
				if (hook == null)
					throw DatabaseException.getDatabaseException(
							new IllegalAccessException("hostID " + hostID + " has not been initialized !"));
				peer.setInitializing(false);

				if (hook.concernsLocalDatabaseHost()) {
					peer.setConnected(false);
					if (networkLogger!=null)
						networkLogger.info("Local peer "+hook.getHostID()+" disconnected !");
					for (ConnectedPeers h : initializedHooks.values()) {
						h.setConnected(false);
						if (networkLogger!=null)
							networkLogger.info(h.getHostID()+" disconnected !");
					}
					this.events.clear();
					initializedHooks.remove(hook.getHostID());
					if (notifier!=null) {
						for (ConnectedPeers h : initializedHooks.values()) {
							notifier.hostDisconnected(h.getHostID());
						}
					}
				} else {
					if (networkLogger!=null)
						networkLogger.info("peer "+hook.getHostID()+" disconnected !");
					peer.setConnected(false);

					cancelEventToSend(hostID, false);
					if (notifier!=null)
						notifier.hostDisconnected(hostID);
					ConnectedPeersWithCentralBackup cp=initializedHooksWithCentralBackup.get(hostID);
					if (cp!=null)
						checkMetaDataUpdate(cp.hookID);

					//backupChannelInitializationMessageFromCentralDatabaseBackup=suspendedHooksWithCentralBackup.remove(hostID);
				}
			}
			finally
			{
				unlockWrite();
			}
			/*if (backupChannelInitializationMessageFromCentralDatabaseBackup!=null)
			{
				if (isInitialized(getLocalHostID()))
					initConnexionWithDistantBackupCenter();
				//initDistantBackupCenter(hook, backupChannelInitializationMessageFromCentralDatabaseBackup.getLastValidatedDistantID(), backupChannelInitializationMessageFromCentralDatabaseBackup.getLastValidatedLocalID());
			}*/


		}


		public boolean isPairedWith(final DecentralizedValue hostID) throws DatabaseException {
			DatabaseHooksTable.Record r = getDatabaseHooksTable().getHook(hostID, true);
			return r!=null;
		}
		private boolean isSynchronizationActivatedWithChannelAndThroughCentralDatabaseBackup(ConnectedPeersWithCentralBackup cp)
		{
			if (cp==null)
				return false;
			ConnectedPeers cps=this.initializedHooks.get(cp.hookID);
			return cps==null || !cps.isConnected();
		}
		public boolean isSynchronizationActivatedWithChannelAndThroughCentralDatabaseBackup(DecentralizedValue hostChannel)
		{
			lockRead();
			try {
				ConnectedPeersWithCentralBackup cp = this.initializedHooksWithCentralBackup.get(hostChannel);
				return isSynchronizationActivatedWithChannelAndThroughCentralDatabaseBackup(cp);
			}
			finally {
				unlockRead();
			}
		}
		private boolean isSynchronizationSuspendedWith(DecentralizedValue hostChannel)
		{
			ConnectedPeers cp = this.initializedHooks.get(hostChannel);
			return (cp!=null && cp.isConnected()) && this.initializedHooksWithCentralBackup.containsKey(hostChannel);
		}
		private void checkMetaDataUpdate(DecentralizedValue hostChannel) throws DatabaseException {

			ConnectedPeersWithCentralBackup cp = this.initializedHooksWithCentralBackup.get(hostChannel);
			if (cp!=null) {
				if (cp.otherBackupMetaDataDatabasePartsSynchronizingWithCentralDatabaseBackup)
					return;

				Collection<String> databasePackagesToSynchronizeWithCentralBackup = getDatabasePackagesToSynchronizeWithCentralBackup();
				if (databasePackagesToSynchronizeWithCentralBackup.size() == 0)
					return;
				DatabaseHooksTable.Record r = getDatabaseHookRecord(hostChannel);
				long lastValidatedDistantID = r.getLastValidatedDistantTransactionID();
				for (String packageString : databasePackagesToSynchronizeWithCentralBackup) {
					Long lastValidatedDistantTransactionIDFromDatabaseBackup=cp.lastValidatedDistantTransactionIDPerDatabaseFromDatabaseBackup.get(packageString);
					if (lastValidatedDistantTransactionIDFromDatabaseBackup==null)
						continue;
					FileCoordinate fc = cp.validatedIDPerDistantHook.getTimestampTransactionToAsk(packageString, lastValidatedDistantTransactionIDFromDatabaseBackup, lastValidatedDistantID);
					if (fc == null) {
						checkAskForEncryptedBackupFilePart(hostChannel, packageString);
					} else {
						cp.otherBackupMetaDataDatabasePartsSynchronizingWithCentralDatabaseBackup=true;

						addNewDatabaseEvent(new AskForMetaDataPerFileToCentralDatabaseBackup(getLocalHostID(), hostChannel, fc, packageString));
					}
				}
			}
		}
		private void checkAskForEncryptedBackupFilePart(DecentralizedValue hostChannel, String packageString) throws DatabaseException {
			ConnectedPeersWithCentralBackup cp=initializedHooksWithCentralBackup.get(hostChannel);

			if (isSynchronizationActivatedWithChannelAndThroughCentralDatabaseBackup(cp)) {
				if (cp.otherBackupDatabasePartsSynchronizingWithCentralDatabaseBackup)
					return;


				DatabaseHooksTable.Record r=getDatabaseHookRecord(hostChannel);
				long fileUTC=cp.validatedIDPerDistantHook.getFileUTCToTransfer(packageString, r.getLastValidatedDistantTransactionID());
				if (fileUTC!=Long.MIN_VALUE) {
					cp.otherBackupDatabasePartsSynchronizingWithCentralDatabaseBackup=true;
					addNewDatabaseEvent(new AskForDatabaseBackupPartDestinedToCentralDatabaseBackup(packageString, getLocalHostID(), hostChannel, new FileCoordinate(fileUTC-1, FileCoordinate.Boundary.LOWER_LIMIT), false));
				}
			}
		}

		private void received(EncryptedMetaDataFromCentralDatabaseBackup metaData) throws DatabaseException, IOException {


			ConnectedPeersWithCentralBackup cp=initializedHooksWithCentralBackup.get(metaData.getHostSource());

			if (cp==null)
				return;
			if (!metaData.getHostDestination().equals(getLocalHostID()))
				throw new DatabaseException("Invalid host destination");
			if (metaData.getHostDestination().equals(metaData.getHostSource()))
				return;


			cp.otherBackupMetaDataDatabasePartsSynchronizingWithCentralDatabaseBackup=false;
			if (!cp.validatedIDPerDistantHook.addMetaData(metaData.getMetaData().getPackageString(), metaData.getMetaData().decodeMetaData(databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup()))) {
				return;
			}


			long mn=cp.validatedIDPerDistantHook.getLastTransactionID(metaData.getMetaData().getPackageString());
			if (mn!=Long.MIN_VALUE) {
				Long lastValidatedDistantTransactionIDFromDatabaseBackup=cp.lastValidatedDistantTransactionIDPerDatabaseFromDatabaseBackup.get(metaData.getMetaData().getPackageString());
				if (lastValidatedDistantTransactionIDFromDatabaseBackup!=null)
					mn=Math.max(lastValidatedDistantTransactionIDFromDatabaseBackup, mn);
				cp.lastValidatedDistantTransactionIDPerDatabaseFromDatabaseBackup.put(metaData.getMetaData().getPackageString(), mn);
			}


			checkMetaDataUpdate(metaData.getHostSource());

		}

		private void initDistantBackupCenter(final DatabaseHooksTable.Record r, final Map<String, Long> lastValidatedDistantTransactionIDPerDatabase, final long lastValidatedLocalTransactionID, Set<String> compatibleDatabases) throws DatabaseException {
			if (r==null)
				return;
			try {
				lockWrite();
				DecentralizedValue hostID=r.getHostID();
				ConnectedPeersWithCentralBackup cp=initializedHooksWithCentralBackup.get(hostID);

				if (cp!=null && isSynchronizationSuspendedWith(hostID))
					throw DatabaseException.getDatabaseException(
							new IllegalAccessException("hostID " + hostID + " already initialized !"));
				if (cp==null) {
					cp = new ConnectedPeersWithCentralBackup(r);
					if (compatibleDatabases!=null)
						cp.compatibleDatabasesFromCentralDatabaseBackup.addAll(compatibleDatabases);
					initializedHooksWithCentralBackup.put(hostID, cp);
				}
				
				cp.lastValidatedDistantTransactionIDPerDatabaseFromDatabaseBackup=lastValidatedDistantTransactionIDPerDatabase;//cp.lastValidatedTransactionIDFromCentralBackup==null?lastValidatedDistantTransactionID:Math.max(cp.lastValidatedTransactionIDFromCentralBackup, lastValidatedDistantTransactionID);
				if (lastValidatedLocalTransactionID!=Long.MIN_VALUE) {
					validateLastSynchronization(hostID,
							lastValidatedLocalTransactionID, true);
				}
				else {
					synchronizeMetaData();
				}
			} finally {
				unlockWrite();
			}
		}
		private void updateDistantBackupCenter(final DatabaseHooksTable.Record r, String databasePackage, final long lastValidatedDistantTransactionID, final long lastValidatedLocalTransactionID) throws DatabaseException {
			if (r==null)
				return;
			try {
				lockWrite();
				ConnectedPeersWithCentralBackup cp=initializedHooksWithCentralBackup.get(r.getHostID());
				if (cp==null)
					return;
				DecentralizedValue hostID=r.getHostID();
				Long lastValidatedTransactionIDFromCentralBackup=cp.lastValidatedDistantTransactionIDPerDatabaseFromDatabaseBackup.get(databasePackage);
				if (lastValidatedDistantTransactionID==Long.MIN_VALUE && lastValidatedTransactionIDFromCentralBackup!=null && lastValidatedTransactionIDFromCentralBackup!=Long.MIN_VALUE)
					throw DatabaseException.getDatabaseException(new IllegalAccessException("hostID="+r.getHostID()));
				cp.lastValidatedDistantTransactionIDPerDatabaseFromDatabaseBackup.put(databasePackage, lastValidatedDistantTransactionID);
				//cp.lastValidatedTransactionIDFromCentralBackup=cp.lastValidatedTransactionIDFromCentralBackup==null?lastValidatedDistantTransactionID:Math.max(cp.lastValidatedTransactionIDFromCentralBackup, lastValidatedDistantTransactionID);
				if (lastValidatedLocalTransactionID!=Long.MIN_VALUE) {

					validateLastSynchronization(hostID,
							lastValidatedLocalTransactionID, true);
				}
				else {
					synchronizeMetaData();
				}

			} finally {
				unlockWrite();
			}
		}
		private void checkForNewCentralBackupDatabaseEvent() throws DatabaseException {
			if (centralBackupInitialized) {
				for (Map.Entry<Package, Database> e : sql_database.entrySet()) {
					if (e.getValue().backupRestoreManager != null) {
						checkForNewBackupFilePartToSendToCentralDatabaseBackup(e.getKey());
					}
				}
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
				return centralBackupInitialized && initializedHooksWithCentralBackup.containsKey(hostID);
			}
			finally {
				unlockRead();
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

				for (Database d : sql_database.values())
					d.lastValidatedTransactionUTCForCentralBackup=Long.MIN_VALUE;
				Set<String> authorizedPackagesToBeSynchronizedWithCentralDatabaseBackup=getDatabasePackagesToSynchronizeWithCentralBackup();
				for (Map.Entry<String, Long> e : lastValidatedTransactionsUTC.entrySet()) {
					if (authorizedPackagesToBeSynchronizedWithCentralDatabaseBackup.contains(e.getKey())) {
						validateLastSynchronizationWithCentralDatabaseBackup(e.getKey(), e.getValue());
					}
					else {
						addNewDatabaseEvent(new DatabaseBackupToRemoveDestinedToCentralDatabaseBackup(getLocalHostID(), e.getKey(), databaseConfigurationsBuilder.getConfigurations().getCentralDatabaseBackupCertificate()));
					}
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
		
		private void received(InitialMessageComingFromCentralBackup initialMessageComingFromCentralBackup) throws DatabaseException, IOException {
			if (!initialMessageComingFromCentralBackup.getHostDestination().equals(getLocalHostID()))
				throw new IllegalArgumentException();
			initDistantBackupCenterForThisHostWithStringPackages(initialMessageComingFromCentralBackup.getLastValidatedTransactionsUTCForDestinationHost());
			for (Map.Entry<DecentralizedValue, LastValidatedLocalAndDistantID> e : initialMessageComingFromCentralBackup.getLastValidatedIDsPerHost(databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup()).entrySet()) {
				assert !e.getKey().equals(getLocalHostID());
				initDistantBackupCenter(e.getKey(), e.getValue().getLastValidatedDistantIDPerDatabase(), e.getValue().getLastValidatedLocalID(), initialMessageComingFromCentralBackup.getDecryptedCompatibleDatabases(e.getKey(), getDatabaseConfigurationsBuilder().getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup()));
			}
			for (AuthenticatedP2PMessage a : initialMessageComingFromCentralBackup.getAuthenticatedP2PMessages(databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup()))
				received(a);

		}
		/*private void initDistantBackupCenterForThisHost(Map<Package, Long> lastValidatedTransactionsUTC) throws DatabaseException {
			Map<String, Long> m=new HashMap<>();
			for (Map.Entry<Package, Long> e : lastValidatedTransactionsUTC.entrySet())
				m.put(e.getKey().getName(), e.getValue());
			initDistantBackupCenterForThisHostWithStringPackages(m);
		}*/
		private void privInitConnectionWithDistantBackupCenter() throws DatabaseException {
			final Map<DecentralizedValue, Long> lastValidatedDistantIDs=new HashMap<>();
			minFilePartDurationBeforeBecomingFinalFilePart=Long.MAX_VALUE;
			for (Database d : sql_database.values())
			{
				if (d.backupRestoreManager!=null && d.configuration.getBackupConfiguration().getMaxBackupFileAgeInMs()<minFilePartDurationBeforeBecomingFinalFilePart)
					minFilePartDurationBeforeBecomingFinalFilePart=d.configuration.getBackupConfiguration().getMaxBackupFileAgeInMs();
			}
			getDatabaseHooksTable()
					.getRecords(new Filter<Record>() {
						@Override
						public boolean nextRecord(Record _record) {
							if (!_record.concernsLocalDatabaseHost())
							{
								lastValidatedDistantIDs.put(_record.getHostID(), _record.getLastValidatedDistantTransactionID());
							}
							return false;
						}
					});

			addNewDatabaseEvent(new DistantBackupCenterConnexionInitialisation(getLocalHostID(), lastValidatedDistantIDs, databaseConfigurationsBuilder.getSecureRandom(), databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup(), databaseConfigurationsBuilder.getConfigurations().getCentralDatabaseBackupCertificate()));
			sendAvailableDatabaseToCentralDatabaseBackup();
			checkForNewAuthenticatedMessagesToSendToCentralDatabaseBackup();
		}
		private void checkForNewAuthenticatedMessagesToSendToCentralDatabaseBackup() throws DatabaseException {
			if (centralBackupInitialized) {
				for (IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup i : getDatabaseHooksTable().getLocalDatabaseHost().getAuthenticatedMessagesQueueToSendToCentralDatabaseBackup(databaseConfigurationsBuilder.getSecureRandom(), databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup(),
						initializedHooks, initializedHooksWithCentralBackup)) {
					addNewDatabaseEvent(i);
				}
			}
		}

		public void centralDatabaseBackupAvailable() throws DatabaseException {
			lockWrite();
			try
			{
				centralBackupAvailable=true;
				if (!centralBackupInitialized && isInitialized())
				{
					centralBackupInitialized=true;
					privInitConnectionWithDistantBackupCenter();
				}
			}
			finally {
				unlockWrite();
			}
		}
		public void centralDatabaseBackupDisconnected() throws DatabaseException {
			lockWrite();
			try
			{
				centralBackupAvailable=false;
				if (centralBackupInitialized)
				{
					disconnectAllHooksFromThereBackups();
				}
			}
			finally {
				unlockWrite();
			}
		}


		/*private void received(final BackupChannelInitializationMessageFromCentralDatabaseBackup message) throws DatabaseException, IOException {
			if (message == null)
				throw new NullPointerException();
			if (!message.getHostDestination().equals(getLocalHostID()))
				throw new IllegalArgumentException();

			initDistantBackupCenter(message.getHostChannel(), message.getLastValidatedDistantID(databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup()), message.getLastValidatedLocalID(databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup()), null);
		}*/
		private void received(final BackupChannelUpdateMessageFromCentralDatabaseBackup message) throws DatabaseException, IOException {
			if (message == null)
				throw new NullPointerException();
			if (!message.getHostDestination().equals(getLocalHostID()))
				throw new IllegalArgumentException();
			updateDistantBackupCenter(message.getHostChannel(), message.getDatabasePackage(), message.getLastValidatedDistantID(databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup()), message.getLastValidatedLocalID(databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup()));
		}
		private void initDistantBackupCenter(final DecentralizedValue hostChannel, final Map<String, Long> lastValidatedDistantTransactionIDPerDatabase, final long lastValidatedLocalTransactionID, Set<String> compatibleDatabases) throws DatabaseException {


			lockWrite();


			initDistantBackupCenter(getDatabaseHookRecord(hostChannel), lastValidatedDistantTransactionIDPerDatabase, lastValidatedLocalTransactionID, compatibleDatabases);
		}
		DatabaseHooksTable.Record getDatabaseHookRecord(final DecentralizedValue hostChannel) throws DatabaseException {
			return getDatabaseHookRecord(hostChannel, true);
		}
		DatabaseHooksTable.Record getDatabaseHookRecord(final DecentralizedValue hostChannel, boolean generateExceptionIfNotFound) throws DatabaseException {
			DatabaseHooksTable.Record r = null;
			List<DatabaseHooksTable.Record> l = getDatabaseHooksTable()
					.getRecordsWithAllFields("hostID", hostChannel);
			if (l.size() == 1)
				r = l.iterator().next();
			else if (l.size() > 1)
				throw new IllegalAccessError();
			if (r == null && generateExceptionIfNotFound)
				throw DatabaseException.getDatabaseException(
						new IllegalArgumentException("The host ID " + hostChannel + " has not been initialized !"));
			return r;
		}
		private void checkCentralDatabaseBackupInitialized() throws DatabaseException {
			if (!centralBackupInitialized)
				throw new DatabaseException("Distant database backup must be initialized first with function initDistantBackupCenterForThisHost");
		}
		private void updateDistantBackupCenter(final DecentralizedValue hostChannel, String databasePackage, final long lastValidatedDistantTransactionID, final long lastValidatedLocalTransactionID) throws DatabaseException {


			lockWrite();


			updateDistantBackupCenter(getDatabaseHookRecord(hostChannel), databasePackage, lastValidatedDistantTransactionID, lastValidatedLocalTransactionID);
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
				centralBackupInitialized=false;
				backupDatabasePartsSynchronizingWithCentralDatabaseBackup.clear();
			}
			finally {
				unlockWrite();
			}
		}

		/*void disconnectHookFromItsBackup(final DecentralizedValue hostID) throws DatabaseException {
			if (hostID == null)
				throw new NullPointerException("hostID");
			if (hostID.equals(getLocalHostID())) {
				disconnectAllHooksFromThereBackups();
				return;
			}

			try {
				lockWrite();
				ConnectedPeersWithCentralBackup peer = initializedHooksWithCentralBackup.remove(hostID);

				if (peer == null) {

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
		}*/

		void disconnectAllHooksFromThereBackups() throws DatabaseException {
			try {
				lockWrite();
				disconnectLocalHookFromItsCentralBackup();
				


				for (ConnectedPeersWithCentralBackup peer : initializedHooksWithCentralBackup.values()) {
					cancelEventToSend(peer.hookID, true);
				}
				initializedHooksWithCentralBackup.clear();
			}
			finally
			{
				unlockWrite();
			}
		}
		void checkConnexionsToInit() throws DatabaseException
		{
			lockWrite();
			try {
				if (databaseConfigurationsBuilder.getConfigurations().getLocalPeer() != null) {

					if (!isInitialized(databaseConfigurationsBuilder.getConfigurations().getLocalPeer()))
						initLocalHostID(databaseConfigurationsBuilder.getConfigurations().getLocalPeer(), databaseConfigurationsBuilder.getConfigurations().isPermitIndirectSynchronizationBetweenPeers());
					for (Map.Entry<DecentralizedValue, ConnectedPeers> e : initializedHooks.entrySet()) {
						if (!e.getValue().isConnected()) {
							checkConnexionsToInit(e.getKey());
						}
					}
				}

			}
			finally {
				unlockWrite();
			}
		}
		void checkDisconnections() throws DatabaseException
		{
			lockWrite();
			try {
				if (databaseConfigurationsBuilder.getConfigurations().getLocalPeer() != null) {

					if (isInitialized(databaseConfigurationsBuilder.getConfigurations().getLocalPeer()))
					{
						for (Map.Entry<DecentralizedValue, ConnectedPeers> e : initializedHooks.entrySet()) {
							if (e.getValue().isConnected() && !e.getKey().equals(databaseConfigurationsBuilder.getConfigurations().getLocalPeer()) && !databaseConfigurationsBuilder.getConfigurations().getDistantPeers().contains(e.getKey()))
							{
								disconnectHook(e.getKey());
							}
						}
					}
				}
				else
				{
					if (isInitialized())
						disconnectAll();
				}

			}
			finally {
				unlockWrite();
			}
		}

		void checkConnexionsToInit(DatabaseHooksTable.Record r) throws DatabaseException
		{
			assert r!=null;
			ConnectedPeers cp=initializedHooks.get(r.getHostID());
			assert cp!=null;
			boolean connectable=cp.isConnectable(DatabaseWrapper.this);
			if (!connectable && cp.isConnected())
				disconnectHook(r.getHostID());

			if (!r.hasNoAuthenticatedMessagesQueueToSend()) {
				List<AuthenticatedP2PMessage> las = r.getAuthenticatedMessagesQueueToSend(initializedHooks);
				for (AuthenticatedP2PMessage a : las) {

					//if (!(a instanceof HookSynchronizeRequest) || cp.compatibleDatabases.containsAll(((HookSynchronizeRequest) a).getPackagesToSynchronize(a.getHostDestination()).keySet()) ) {
					addNewDatabaseEvent((DatabaseEvent) a);
					//}

				}
			} else if (connectable) {
				sendP2PConnexionInitializationMessage(r);
			}
		}
		private void checkConnexionsToInit(DecentralizedValue peerID) throws DatabaseException
		{
			//if (databaseConfigurationsBuilder.getConfigurations().getDistantPeers().contains(peerID))
			{
				DatabaseHooksTable.Record r = getDatabaseHooksTable().getHook(peerID, true);

				if (r == null)
					return;

				checkConnexionsToInit(r);
			}
		}
		public void connectionLost() throws DatabaseException {
			if (!isInitialized())
				return;
			try {
				lockWrite();
				disconnectAll();
				initializedHooks.clear();
				//initializingHooks.clear();
				centralBackupAvailable=false;
			}
			finally {
				unlockWrite();
			}
		}



		public void peerConnected(DecentralizedValue peerID) throws DatabaseException {
			if (peerID == null)
				throw new NullPointerException("hostID");
			if (!isInitialized())
				return;

			try {
				lockWrite();

				ConnectedPeers cp=initializedHooks.get(peerID);
				if (cp!=null)
					return;
				if (peerID.equals(getLocalHostID()))
					return;
				initializedHooks.put(peerID, new ConnectedPeers(peerID, false));
				sendAvailableDatabaseTo(peerID);
				if (networkLogger!=null)
					networkLogger.info("Peer "+peerID+" available for connection !");

			}
			finally {
				unlockWrite();
			}

		}

		public void peerDisconnected(DecentralizedValue peerID) throws DatabaseException {
			try {
				lockWrite();
				ConnectedPeers cp=initializedHooks.get(peerID);

				if (cp!=null && cp.isConnected())
					disconnectHook(peerID);
				initializedHooks.remove(peerID);


			}
			finally {
				unlockWrite();
			}
		}

		void initHook(final DecentralizedValue hostID, final long lastValidatedTransactionID) throws DatabaseException {
			if (hostID == null)
				throw new NullPointerException("hostID");
			try {
				lockWrite();
				ConnectedPeers cp=initializedHooks.get(hostID);
				if (cp==null)
					throw DatabaseException.getDatabaseException(
							new IllegalAccessException("hostID not annouced !"));
				if (cp.isConnected()) {
					initializedHooks.remove(hostID);
					throw DatabaseException.getDatabaseException(
							new IllegalAccessException("hostID " + hostID + " already initialized !"));
				}
			}
			finally
			{
				unlockWrite();
			}
			DatabaseHooksTable.Record r = getDatabaseHooksTable().getHook(hostID, true);
			if (r==null)
				return;

			initHook(r, lastValidatedTransactionID);
		}

		private boolean hookCannotBeConnected(DatabaseHooksTable.Record hook)
		{
			return hook.getPairingState() == DatabaseHooksTable.PairingState.REMOVED && databaseConfigurationsBuilder.getConfigurations().getDistantPeers().contains(hook.getHostID());
		}

		@SuppressWarnings("unlikely-arg-type")
		void initHook(final DatabaseHooksTable.Record r, final long lastValidatedTransactionID)
				throws DatabaseException {
			if (r == null)
				throw new DatabaseException("r");
			if (!isInitialized()) {
				throw new DatabaseException("The Synchronizer must be initialized (initLocalHostID function) ! (Local host ID=" + getDatabaseConfigurationsBuilder().getConfigurations().getLocalPeer() + ")");
			}
			if (hookCannotBeConnected(r)) {
				return;
			}


			
			try {
				lockWrite();
				ConnectedPeers cp=initializedHooks.get(r.getHostID());
				if (cp==null)
					throw DatabaseException.getDatabaseException(
							new IllegalAccessException("hostID not annouced !"));
				if (cp.isConnected()) {
					throw DatabaseException.getDatabaseException(
							new IllegalAccessException("hostID " + r.getHostID() + " already initialized !"));
				}

				if (!cp.isInitializing())
					sendP2PConnexionInitializationMessage(r);
				cp.setConnected(true);

				addNewAuthenticatedDatabaseEvents(r);
				validateLastSynchronization(r.getHostID(),
						lastValidatedTransactionID, false);
				if (networkLogger!=null)
					networkLogger.info("Peer "+cp.getHostID()+" connected !");
				if (notifier!=null && !r.concernsLocalDatabaseHost())
					notifier.hostConnected(r.getHostID());
			}
			finally
			{
				unlockWrite();
			}
			
		}

		public Collection<DecentralizedValue> getDistantHostsIDs() throws DatabaseException {
			HashSet<DecentralizedValue> res=new HashSet<>();
			for (DatabaseHooksTable.Record hook : getDatabaseHooksTable().getRecords())
			{
				if (hook.concernsLocalDatabaseHost())
					continue;
				res.add(hook.getHostID());
			}
			return res;
		}

		public void received(DatabaseTransactionsIdentifiersToSynchronize d) throws DatabaseException {


			getDatabaseHooksTable().validateDistantTransactions(d.getHostSource(),
					d.getLastDistantTransactionIdentifiers(), true);
		}

		public void received(P2PBigDatabaseEventToSend data, InputStreamGetter inputStream) throws DatabaseException {
			data.importFromInputStream(DatabaseWrapper.this, inputStream);
			//synchronizedDataIfNecessary();
			notifyNewTransactionsIfNecessary();
		}


		public void received(final LastIDCorrection idCorrection) throws DatabaseException {
			if (idCorrection == null)
				throw new NullPointerException();

			
			try {
				lockWrite();
				ConnectedPeers cp;
				cp = initializedHooks.get(idCorrection.getHostSource());
				if (cp == null || !cp.isConnected())
					throw new DatabaseException("The host " + idCorrection.getHostSource() + " is not connected !");
				cp.setTransferInProgress(false);
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
					DatabaseHooksTable.Record hook = getDatabaseHooksTable().getHook(hostSource);

					getDatabaseHooksTable().updateRecord(hook, "lastValidatedDistantTransactionID",
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


		/*private void validateLastSynchronizationWithCentralDatabaseBackup(Package _package, @SuppressWarnings("SameParameterValue") long lastValidatedTransactionUTC) throws DatabaseException {

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
		}*/
		private void validateLastSynchronizationWithCentralDatabaseBackup(String _package, long lastValidatedTransactionUTC) throws DatabaseException {
			lockWrite();
			try {
				if(centralBackupInitialized)
				{
					for (Map.Entry<Package, Database> e : sql_database.entrySet())
					{
						if (e.getKey().getName().equals(_package))
						{
							validateLastSynchronizationWithCentralDatabaseBackup(e.getValue(), lastValidatedTransactionUTC);
							return;
						}
					}
				}

			}
			finally {
				unlockWrite();
			}
		}
		private void validateLastSynchronizationWithCentralDatabaseBackup(Database d, long lastValidatedTransactionUTC) throws DatabaseException {

	  		if (d.backupRestoreManager==null) {
				return;
			}
			long timeStamp;
			d.lastValidatedTransactionUTCForCentralBackup = lastValidatedTransactionUTC;
			if (lastValidatedTransactionUTC == Long.MIN_VALUE) {
				Long l=d.backupRestoreManager.getFirstValidatedTransactionUTCInMs();
				timeStamp = l==null?Long.MIN_VALUE:l;
			} else {
				timeStamp = d.backupRestoreManager.getNearestFileUTCFromGivenTimeNotIncluded(lastValidatedTransactionUTC);
			}

			if (timeStamp != Long.MIN_VALUE) {
				if (!backupDatabasePartsSynchronizingWithCentralDatabaseBackup.contains(d.configuration.getDatabaseSchema().getPackage().getName())) {
					//File f = d.backupRestoreManager.getFile(timeStamp);
					backupDatabasePartsSynchronizingWithCentralDatabaseBackup.add(d.configuration.getDatabaseSchema().getPackage().getName());
					addNewDatabaseEvent(d.backupRestoreManager.getEncryptedFilePartWithMetaData(getLocalHostID(), timeStamp, d.backupRestoreManager.isReference(timeStamp), databaseConfigurationsBuilder.getSecureRandom(), databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup()));
					//addNewDatabaseEvent(new DatabaseBackupToIncorporateFromCentralDatabaseBackup(getLocalHostID(), getHooksTransactionsTable().getLocalDatabaseHost(), _package,timeStamp, b.isReference(timeStamp), b.extractTransactionInterval(f), f ));
				}

			}
			else {
				checkAskForDatabaseBackupPartDestinedToCentralDatabaseBackup(d);

			}
		}
		void notifyOtherPeersThatDatabaseRestorationWasDone(Package p, long timeUTCOfRestorationInMs, Long transactionToDeleteUpperLimitUTC) throws DatabaseException {
			DecentralizedValue hostThatApplyRestoration=getLocalHostID();
			if (hostThatApplyRestoration==null)
				return;
			notifyOtherPeersThatDatabaseRestorationWasDone(p, timeUTCOfRestorationInMs, hostThatApplyRestoration, transactionToDeleteUpperLimitUTC, false);
		}
		void notifyOtherPeersThatDatabaseRestorationWasDone(Package p, long timeUTCOfRestorationInMs, DecentralizedValue hostThatApplyRestoration, boolean chooseNearestBackupIfNoBackupMatch) throws DatabaseException {
			notifyOtherPeersThatDatabaseRestorationWasDone(p, timeUTCOfRestorationInMs, hostThatApplyRestoration, null, chooseNearestBackupIfNoBackupMatch);
		}
		void notifyOtherPeersThatDatabaseRestorationWasDone(Package p, long timeUTCOfRestorationInMs, DecentralizedValue hostThatApplyRestoration, Long transactionToDeleteUpperLimitUTC, boolean chooseNearestBackupIfNoBackupMatch) throws DatabaseException {
			if (p==null)
				throw new NullPointerException();
			if (hostThatApplyRestoration==null)
				throw new NullPointerException();
			runSynchronizedTransaction(new SynchronizedTransaction<Object>() {
				@Override
				public Object run() throws Exception {
					Reference<DatabaseHooksTable.Record> localRecord=new Reference<>(null);
					getDatabaseHooksTable().updateRecords(new AlterRecordFilter<DatabaseHooksTable.Record>() {

						@Override
						public void nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException {
							if (_record.concernsLocalDatabaseHost())
								localRecord.set(_record);
							else
								_record.offerNewAuthenticatedP2PMessage(DatabaseWrapper.this, new RestorationOrderMessage(getLocalHostID(), _record.getHostID(), hostThatApplyRestoration, p.getName(), timeUTCOfRestorationInMs, chooseNearestBackupIfNoBackupMatch), getDatabaseConfigurationsBuilder().getSecureRandom(), getDatabaseConfigurationsBuilder().getProtectedSignatureProfileProviderForAuthenticatedP2PMessages(), this);
						}

					});
					if (localRecord.get()!=null) {
						getSynchronizer().cleanTransactionsAfterRestoration(p.getName(), timeUTCOfRestorationInMs, transactionToDeleteUpperLimitUTC, transactionToDeleteUpperLimitUTC==null, chooseNearestBackupIfNoBackupMatch);
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

		void checkForNewBackupFilePartToSendToCentralDatabaseBackup(Package p) throws DatabaseException {
			lockWrite();
			try {
				if (this.centralBackupInitialized)
				{
					if (backupDatabasePartsSynchronizingWithCentralDatabaseBackup.contains(p.getName())) {
						return;
					}
					Database db=sql_database.get(p);
					if (db!=null && db.configuration.isSynchronizedWithCentralBackupDatabase()){
						validateLastSynchronizationWithCentralDatabaseBackup(db, db.lastValidatedTransactionUTCForCentralBackup );
					}
				}
			}
			finally {
				unlockWrite();
			}
		}


		private void checkAskForTemporaryDatabaseBackupPartDestinedToCentralDatabaseBackup(Database d) throws DatabaseException {
			if (!d.isCurrentDatabaseInRestorationProcessFromCentralDatabaseBackup())
				return;
			if (!isInitializedWithCentralBackup())
				return;
			for (DatabaseConfiguration dc : getDatabaseConfigurationsBuilder().getConfigurations().getConfigurations()) {
				if (dc.getDatabaseSchema().getPackage().equals(d.configuration.getDatabaseSchema().getPackage())) {
					Long timeUTCInMs=dc.getTimeUTCInMsForRestoringDatabaseToOldVersion();
					if (timeUTCInMs==null)
					{
						d.cancelCurrentDatabaseRestorationProcessFromCentralDatabaseBackup();
					}
					else
					{
						long lts=d.temporaryBackupRestoreManagerComingFromDistantBackupManager.getFirstFileReferenceUTCInMs();
						if (timeUTCInMs<lts)
							addNewDatabaseEvent(new AskForDatabaseBackupPartDestinedToCentralDatabaseBackup(d.configuration.getDatabaseSchema().getPackage().getName(), getLocalHostID(), d.temporaryBackupRestoreManagerChannelComingFromDistantBackupManager, new FileCoordinate(lts-1, FileCoordinate.Boundary.UPPER_LIMIT), true));
						else {
							checkNotAskForDatabaseBackupPartDestinedToCentralDatabaseBackup(d);
						}
					}
					break;
				}
			}
		}
		private void checkAskForDatabaseBackupPartDestinedToCentralDatabaseBackup(Database d) throws DatabaseException {
			if (d.lastValidatedTransactionUTCForCentralBackup>Long.MIN_VALUE)
			{
				long lts=d.backupRestoreManager.getLastTransactionUTCInMS();
				if (lts<d.lastValidatedTransactionUTCForCentralBackup) {
					addNewDatabaseEvent(new AskForDatabaseBackupPartDestinedToCentralDatabaseBackup(d.configuration.getDatabaseSchema().getPackage().getName(), getLocalHostID(), new FileCoordinate(lts, FileCoordinate.Boundary.LOWER_LIMIT), false));
				}
				else {
					getDatabaseConfigurationsBuilder().applyRestorationIfNecessary(d);
					checkAskForTemporaryDatabaseBackupPartDestinedToCentralDatabaseBackup(d);
				}
			}
			else
				checkAskForTemporaryDatabaseBackupPartDestinedToCentralDatabaseBackup(d);
		}

		boolean checkNotAskForDatabaseBackupPartDestinedToCentralDatabaseBackup(Database d) throws DatabaseException {
			boolean restore=false;

			if (d.configuration.isSynchronizedWithCentralBackupDatabase()) {
				if (d.lastValidatedTransactionUTCForCentralBackup > Long.MIN_VALUE) {
					long lts = d.backupRestoreManager.getLastTransactionUTCInMS();
					if (lts >= d.lastValidatedTransactionUTCForCentralBackup) {
						restore = true;
					}
				}
			}
			else
				restore=true;

			if (restore) {
				if (d.isCurrentDatabaseInRestorationProcessFromCentralDatabaseBackup())
					checkAskForTemporaryDatabaseBackupPartDestinedToCentralDatabaseBackup(d);
				else
					getDatabaseConfigurationsBuilder().applyRestorationIfNecessary(d);
			}
			return restore;
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
							if (backupPart instanceof EncryptedBackupPartForRestorationComingFromCentralDatabaseBackup)
							{
								if (d.temporaryBackupRestoreManagerComingFromDistantBackupManager==null)
									return;
								d.setTemporaryBackupRestoreManagerChannelComingFromDistantBackupManager(((EncryptedBackupPartForRestorationComingFromCentralDatabaseBackup) backupPart).getChannelHost());
								if (d.temporaryBackupRestoreManagerComingFromDistantBackupManager.importEncryptedBackupPartComingFromCentralDatabaseBackup(backupPart, databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup(), true))
									checkAskForTemporaryDatabaseBackupPartDestinedToCentralDatabaseBackup(d);
							}
							else if (d.backupRestoreManager.importEncryptedBackupPartComingFromCentralDatabaseBackup(backupPart, databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup(), false))
								checkAskForDatabaseBackupPartDestinedToCentralDatabaseBackup(d);
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
				ConnectedPeersWithCentralBackup cp=initializedHooksWithCentralBackup.get(backupPart.getHostSource());
				if (isSynchronizationActivatedWithChannelAndThroughCentralDatabaseBackup(cp)) {
					try (RandomCacheFileOutputStream out = RandomCacheFileCenter.getSingleton().getNewBufferedRandomCacheFileOutputStream(true, RandomFileOutputStream.AccessMode.READ_AND_WRITE)) {
						EncryptionTools.decode(databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup(), backupPart.getPartInputStream(), out);
						out.flush();
						cp.otherBackupDatabasePartsSynchronizingWithCentralDatabaseBackup=false;
						if (getDatabaseTransactionsPerHostTable().alterDatabaseFromBackup(backupPart.getMetaData().getPackageString(), backupPart.getHostSource(), out.getRandomInputStream(), backupPart.getMetaData().isReferenceFile())) {
							checkAskForEncryptedBackupFilePart(backupPart.getHostSource(), backupPart.getMetaData().getPackageString());
						}
					} catch (IOException e) {
						throw DatabaseException.getDatabaseException(e);
					}
				}
			}
		}

		private void received(EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup confirmation) throws DatabaseException {

			if (!confirmation.getHostDestination().equals(getLocalHostID()))
				throw DatabaseException.getDatabaseException(new MessageExternalizationException(Integrity.FAIL));
			this.backupDatabasePartsSynchronizingWithCentralDatabaseBackup.remove(confirmation.getPackageString());
			validateLastSynchronizationWithCentralDatabaseBackup(confirmation.getPackageString(), confirmation.getLastTransactionUTC());
		}

		private void received(CompatibleDatabasesP2PMessage message) throws DatabaseException {
			if (!isInitialized())
				throw new DatabaseException("The Synchronizer must be initialized (initLocalHostID function) !");
			if (!message.getHostDestination().equals(getLocalHostID()))
				throw new DatabaseException("Invalid destination host !");
			ConnectedPeers cp=initializedHooks.get(message.getHostSource());
			if (cp!=null)
			{
				cp.compatibleDatabasesFromDirectPeer.clear();
				cp.compatibleDatabasesFromDirectPeer.addAll(message.getCompatibleDatabasesWithDestinationPeer());
				cp.compatibleDatabases.clear();
				cp.compatibleDatabases.addAll(message.getCompatibleDatabases());
			}
			checkConnexionsToInit(message.getHostSource());

		}

		private void received(CompatibleDatabasesMessageComingFromCentralDatabaseBackup message) throws DatabaseException {
			if (!isInitialized())
				throw new DatabaseException("The Synchronizer must be initialized (initLocalHostID function) !");
			if (!message.getHostDestination().equals(getLocalHostID()))
				throw new DatabaseException("Invalid destination host !");
			ConnectedPeersWithCentralBackup cp=initializedHooksWithCentralBackup.get(message.getHostSource());
			if (cp!=null)
			{
				cp.compatibleDatabasesFromCentralDatabaseBackup.clear();
				cp.compatibleDatabasesFromCentralDatabaseBackup.addAll(message.getDecryptedCompatibleDatabases(DatabaseWrapper.this.getDatabaseConfigurationsBuilder().getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup()));
			}
			checkForNewAuthenticatedMessagesToSendToCentralDatabaseBackup();
		}

		public void received(DatabaseEventToSend data) throws DatabaseException, IOException {
			if (networkLogger!=null)
				networkLogger.finer("Received message : "+data);
			if (data instanceof AuthenticatedP2PMessage)
				received((AuthenticatedP2PMessage)data);
			else if (data instanceof P2PDatabaseEventToSend)
				received((P2PDatabaseEventToSend)data);
			else if (data instanceof MessageComingFromCentralDatabaseBackup)
				received((MessageComingFromCentralDatabaseBackup)data);
		}
		private void received(P2PDatabaseEventToSend data) throws DatabaseException {
			if (!isInitialized())
				throw new DatabaseException("The Synchronizer must be initialized (initLocalHostID function) !");
			if (data instanceof CompatibleDatabasesP2PMessage)
			{
				received((CompatibleDatabasesP2PMessage)data);
			}
			else if (data instanceof P2PConnexionInitialization)
			{
				received((P2PConnexionInitialization)data);
			}
			else {
				if (!isInitialized(data.getHostSource()))
					throw new DatabaseException("Host "+data.getHostSource()+" not initialized into host "+data.getHostDestination()+". message="+data);
				if (data instanceof DatabaseTransactionsIdentifiersToSynchronize)
					received((DatabaseTransactionsIdentifiersToSynchronize) data);
				else if (data instanceof TransactionConfirmationEvents) {
					TransactionConfirmationEvents tce = (TransactionConfirmationEvents) data;
					//if (isInitialized(tce.getHostSource()))
						validateLastSynchronization(tce.getHostSource(),
								tce.getLastValidatedTransaction(), false);
					/*else {
						try {
							initHook(tce.getHostSource(),
									tce.getLastValidatedTransaction());
						} catch (MessageExternalizationException e) {
							throw DatabaseException.getDatabaseException(e);
						}
					}*/
				} else if (data instanceof LastIDCorrection) {
					received((LastIDCorrection) data);
				}
				else
					throw new DatabaseException("Invalid message "+data, new IllegalArgumentException());
			}

		}
		private void received(P2PConnexionInitialization p2PConnexionInitialization) throws DatabaseException {
			initHook(p2PConnexionInitialization.getHostSource(), p2PConnexionInitialization.getLastValidatedTransactionID());
		}
		private void received(IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup message) throws DatabaseException, IOException {

			for (AuthenticatedP2PMessage a : message.getAuthenticatedP2PMessages(databaseConfigurationsBuilder.getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup()))
				received(a);
		}
		private void received(MessageComingFromCentralDatabaseBackup data) throws DatabaseException, IOException {
			if (!isInitialized())
				throw new DatabaseException("Not initialized !");
			if (data instanceof InitialMessageComingFromCentralBackup)
			{
				received((InitialMessageComingFromCentralBackup)data);
			}
			else {
				checkCentralDatabaseBackupInitialized();
				if (data instanceof CompatibleDatabasesMessageComingFromCentralDatabaseBackup)
				{
					received((CompatibleDatabasesMessageComingFromCentralDatabaseBackup) data);
				}
				else if (data instanceof EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup) {
					received((EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup) data);
				} else if (data instanceof EncryptedBackupPartComingFromCentralDatabaseBackup) {
					received((EncryptedBackupPartComingFromCentralDatabaseBackup) data);
				} else if (data instanceof EncryptedMetaDataFromCentralDatabaseBackup) {
					received((EncryptedMetaDataFromCentralDatabaseBackup) data);
				} else if (data instanceof IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup) {

					received((IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup) data);
				} else if (data instanceof BackupChannelUpdateMessageFromCentralDatabaseBackup) {
					if (data instanceof BackupChannelInitializationMessageFromCentralDatabaseBackup) {
						received((BackupChannelInitializationMessageFromCentralDatabaseBackup) data);
					} else
						received((BackupChannelUpdateMessageFromCentralDatabaseBackup) data);
				}
			}

		}





	}

	public enum SynchronizationAnomalyType {
		RECORD_TO_REMOVE_NOT_FOUND, RECORD_TO_REMOVE_HAS_DEPENDENCIES, RECORD_TO_UPDATE_NOT_FOUND, RECORD_TO_UPDATE_HAS_INCOMPATIBLE_PRIMARY_KEYS, RECORD_TO_UPDATE_HAS_DEPENDENCIES_NOT_FOUND, RECORD_TO_ADD_ALREADY_PRESENT, RECORD_TO_ADD_HAS_INCOMPATIBLE_PRIMARY_KEYS, RECORD_TO_ADD_HAS_DEPENDENCIES_NOT_FOUND,
	}
	boolean checkNotAskForDatabaseBackupPartDestinedToCentralDatabaseBackup(Package databasePackage) throws DatabaseException {
		Database d=sql_database.get(databasePackage);
		if (d==null)
			throw new DatabaseException("Database "+databasePackage.getName()+" is not loaded !");

		return getSynchronizer().checkNotAskForDatabaseBackupPartDestinedToCentralDatabaseBackup(d);
	}


	public Set<DatabaseConfiguration> getLoadedDatabaseConfigurations()
	{
		lockRead();
		try {
			Set<DatabaseConfiguration> res = new HashSet<>();
			for (Database d : sql_database.values()) {
				res.add(d.configuration);
			}
			return res;
		}
		finally {
			unlockRead();
		}
	}

	/*private Stream<DatabaseConfiguration> getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurations()
	{
		return sql_database.values()
				.stream()
				.filter(d-> getDatabaseConfigurationsBuilder().getConfigurations().getConfigurations().stream().anyMatch(dc-> dc.getDatabaseSchema().getPackage().equals(d.configuration.getDatabaseSchema().getPackage())))
				.map(d-> d.configuration);
	}*/
	private Stream<String> getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurations()
	{
		return sql_database.values()
				.stream()
				.map(d-> d.configuration)
				.filter(dc -> {
					if (getDatabaseConfigurationsBuilder().getConfigurations().getConfigurations().stream().noneMatch(dc2-> dc2.getDatabaseSchema().getPackage().equals(dc.getDatabaseSchema().getPackage())))
						return false;
					return dc.isDecentralized();
				})
				.map(c -> c.getDatabaseSchema().getPackage().getName());
	}
	private Set<String> getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurationsString()
	{
		return getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurations()
				.collect(Collectors.toSet());
	}
	private Set<String> getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurationsString(DecentralizedValue hostID)
	{
		return getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurations(hostID)
				.collect(Collectors.toSet());
	}
	private Stream<String> getLoadedDatabaseConfigurationsPresentIntoGlobalDatabaseConfigurations(DecentralizedValue hostID)
	{
		return sql_database.values()
				.stream()
				.map(d-> d.configuration)
				.filter(dc -> {
					if (getDatabaseConfigurationsBuilder().getConfigurations().getConfigurations().stream().noneMatch(dc2-> dc2.getDatabaseSchema().getPackage().equals(dc.getDatabaseSchema().getPackage())))
						return false;
					if (dc.isDecentralized())
					{
						Set<DecentralizedValue> s=dc.getDistantPeersThatCanBeSynchronizedWithThisDatabase();
						return s!=null && s.contains(hostID);
					}
					return false;
				})
				.map(c -> c.getDatabaseSchema().getPackage().getName())
			;
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
		public String toString() {
			return "DatabaseEventsToSynchronizeP2P{" +
					"lastTransactionIDIncluded=" + lastTransactionIDIncluded +
					", maxEventsRecords=" + maxEventsRecords +
					", hookID=" + hookID +
					", hostIDSource=" + hostIDSource +
					", hostIDDestination=" + hostIDDestination +
					'}';
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

		@Override
		public MergeState mergeWithP2PDatabaseEventToSend(DatabaseEvent newEvent) throws DatabaseException {
			if (newEvent instanceof DatabaseEventsToSynchronizeP2P) {
				DatabaseEventsToSynchronizeP2P ne=(DatabaseEventsToSynchronizeP2P)newEvent;
				if (hookID == ne.hookID) {
					lastTransactionIDIncluded = Math.max(lastTransactionIDIncluded, ne.lastTransactionIDIncluded);
					return MergeState.DELETE_NEW;
				} else
					return MergeState.NO_FUSION;
			}
			else
				return super.mergeWithP2PDatabaseEventToSend(newEvent);
		}
	}
	public static abstract class AbstractDatabaseEventsToSynchronizeP2P extends DatabaseEvent implements P2PBigDatabaseEventToSend, SecureExternalizable {
		protected transient DatabaseHooksTable.Record hook;
		protected int hookID;
		protected DecentralizedValue hostIDSource, hostIDDestination;

		@Override
		public boolean cannotBeMerged() {
			return false;
		}

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
			String f = databaseIdentifier;

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

	DatabaseHooksTable getDatabaseHooksTable() throws DatabaseException {
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

	void checkMinimumValidatedTransactionIds() throws DatabaseException {
		long minIds=Long.MIN_VALUE;
		for (Database d : sql_database.values())
		{
			if (d.backupRestoreManager!=null) {
				long lastTransactionID = d.backupRestoreManager.getLastTransactionID();
				if (lastTransactionID > minIds)
					minIds = lastTransactionID;
			}
		}
		if (minIds>Long.MIN_VALUE)
			getTransactionIDTable().setMinimumValidatedTransactionID(minIds);
	}

	protected abstract Connection reopenConnectionImpl() throws DatabaseLoadingException;

	final Connection reopenConnection() throws DatabaseException {
		try {
			Connection c = reopenConnectionImpl();

			disableAutoCommit(c);
			if (!openedOneTime)
			{
				//addOpenedDatabaseWrapper(this);
				openedOneTime=true;

			}
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
						setDatabaseLogLevel(Level.OFF);
						setNetworkLogLevel(Level.OFF);
						closed = true;
						//removeClosedDatabaseWrapper(this);
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
						DatabaseWrapper.lockers.remove(databaseIdentifier);
						Integer i=DatabaseWrapper.number_of_shared_lockers.get(databaseIdentifier);
						if (i!=null) {
							int v = i - 1;
							if (v == 0)
								DatabaseWrapper.number_of_shared_lockers.remove(databaseIdentifier);
							else if (v > 0)
								DatabaseWrapper.number_of_shared_lockers.put(databaseIdentifier, v);
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
		return databaseName.hashCode();
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
		return databaseName;
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
		private final HashMap<Package, BackupRestoreManager.AbstractTransaction> backupManager=new HashMap<>();
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

		private BackupRestoreManager.AbstractTransaction getBackupManagerAndStartTransactionIfNecessary(Package p, boolean transactionToSynchronizeFromCentralDatabaseBackup) throws DatabaseException {
			if (!backupManager.containsKey(p)) {
				BackupRestoreManager brm = getBackupRestoreManager(p);

				if (brm!=null)
				{
					BackupRestoreManager.AbstractTransaction res=brm.startTransaction(transactionToSynchronizeFromCentralDatabaseBackup);
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

			BackupRestoreManager.AbstractTransaction backupTransaction=getBackupManagerAndStartTransactionIfNecessary(p, applySynchro);
			if (backupTransaction!=null)
				backupTransaction.backupRecordEvent(table, de);
			if (!applySynchro)
				return false;

			if (p.equals(DatabaseWrapper.class.getPackage()))
				return false;
			if (!table.supportSynchronizationWithOtherPeers())
				return false;

			if (!getDatabaseHooksTable().supportPackage(p))
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

			if (eventsStoredIntoMemory && actualTransactionEventsNumber.get() >= getMaxTransactionEventsKeptIntoMemory()) {
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
					res = new TransactionPerDatabase(t, getMaxTransactionEventsKeptIntoMemory());
				else
					res = new TransactionPerDatabase(getDatabaseTransactionEventsTable().addRecord(t), 0);
				temporaryTransactions.put(concernedDatabase, res);
			}
			return res;
		}


        void addNewTemporaryEvent(final Table<?> table, final TableEvent<?> event) throws DatabaseException {
			final TransactionPerDatabase transaction = getAndCreateIfNecessaryTemporaryTransaction(
					table.getDatabaseConfiguration().getDatabaseSchema().getPackage());
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
			for (Map.Entry<Package, BackupRestoreManager.AbstractTransaction> e : this.backupManager.entrySet())
			{
				if (e.getValue()!=null)
					hashMap.put(e.getKey(), e.getValue().getBackupPosition());
			}
			return hashMap;
		}

		void cancelTmpTransactionEvents(final int position, Map<Package, Long> backupPositions) throws DatabaseException {
			if (position == actualPosition.get())
				return;
			for (Iterator<Map.Entry<Package, BackupRestoreManager.AbstractTransaction>> it = this.backupManager.entrySet().iterator();it.hasNext();)
			{
				Map.Entry<Package, BackupRestoreManager.AbstractTransaction> e=it.next();
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
			for (BackupRestoreManager.AbstractTransaction t : backupManager.values())
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
			for (Map.Entry<Package, BackupRestoreManager.AbstractTransaction> e: backupManager.entrySet()) {
				if (e.getKey().toString().equals(concernedDatabasePackage))
					return e.getValue().getTransactionUTC();
			}
			return System.currentTimeMillis();
		}

		boolean validateTmpTransaction() throws DatabaseException {
			final HashMap<Package, Long> transactionsID=new HashMap<>();

			try {
				boolean res=transactionToSynchronize;
				if (transactionToSynchronize) {
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

										getDatabaseHooksTable().getRecords(new Filter<Record>() {

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
										getDatabaseHooksTable().getRecords(new Filter<Record>() {

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
				for (Map.Entry<Package, BackupRestoreManager.AbstractTransaction> e : backupManager.entrySet()) {

					BackupRestoreManager.AbstractTransaction t = e.getValue();
					if (t != null) {
						Long tid=transactionsID.get(e.getKey());
						t.validateTransaction(tid);
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
				for (Map.Entry<Package, BackupRestoreManager.AbstractTransaction> element: backupManager.entrySet())
				{
					BackupRestoreManager.AbstractTransaction t=element.getValue();
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

		TransactionPerDatabase(DatabaseTransactionEventsTable.Record transaction, int maxEventsNumberKeptIntoMemory) {
			if (transaction == null)
				throw new NullPointerException();
			this.transaction = transaction;
			this.eventsNumber = new AtomicInteger(0);
			concernedHosts = new HashSet<>();
			events = new ArrayList<>(maxEventsNumberKeptIntoMemory);
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
				if (closed || alwaysDisconnectAfterOnTransaction) {
					if (!c.getConnection().isClosed())
						closeConnection(c.getConnection(), true);
				}

				for (Iterator<Session> it = threadPerConnection.iterator(); it.hasNext();) {
					Session s = it.next();

					if (!s.getThread().isAlive()) {
						if (!s.getConnection().isClosed() && !s.getConnection().isClosed())
							closeConnection(s.getConnection(), alwaysDisconnectAfterOnTransaction);

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

	protected boolean mustReleaseSavepointAfterRollBack()
	{
		return true;
	}

	protected boolean mustReleaseSavepointAfterCommit()
	{
		return true;
	}

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
		return !isThreadSafe() || hasOnePeerSynchronized; 
	}

	private void checkAutoIncrementTable() throws DatabaseException {
		try {
			if ((!supportMultipleAutoPrimaryKeys() || !supportSingleAutoPrimaryKeys()) && !doesTableExists(DatabaseWrapper.AUTOINCREMENT_TABLE)) {
				try (Statement pst = getConnectionAssociatedWithCurrentThread().getConnection()
						.createStatement()) {
					pst.execute("CREATE TABLE " + DatabaseWrapper.AUTOINCREMENT_TABLE +
							" (TABLE_ID INT NOT NULL, TABLE_VERSION INT NOT NULL, AI BIGINT NOT NULL, CONSTRAINT AUTOINCREMENT_TABLE_PK_CONSTRAINT PRIMARY KEY(TABLE_ID, TABLE_VERSION))"
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
					.prepareStatement("SELECT AI FROM " + DatabaseWrapper.AUTOINCREMENT_TABLE + " WHERE TABLE_ID='"
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
						.prepareStatement("INSERT INTO " + DatabaseWrapper.AUTOINCREMENT_TABLE + " (TABLE_ID, TABLE_VERSION, AI) VALUES('"
								+ tableID+"', '" +databaseVersion+"', '"+ (res + 1) + "')" + getSqlComma())) {
					pst.executeUpdate();
				}
			}
			else
			{
				try (PreparedStatement pst = getConnectionAssociatedWithCurrentThread().getConnection()
						.prepareStatement("UPDATE " + DatabaseWrapper.AUTOINCREMENT_TABLE + " SET AI='"+(res+1)+"' WHERE TABLE_ID='"
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
		Package databasePackage=writeData?_transaction.getConcernedDatabasePackage():null;
		boolean needsLock=needsToLock();
		if (databasePackage!=null)
		{
			if (reservedDatabases.contains(databasePackage))
				databasePackage=null;
			else
				needsLock=true;
		}

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
				if (databasePackage!=null)
				{
					Database db=sql_database.get(databasePackage);
					if (db!=null)
					{
						if (db.isCurrentDatabaseInRestorationProcessFromExternalBackup())
							throw new DatabaseException("Impossible to write into the database "+databasePackage+" because this database is in a restoration process !");
					}
				}

				boolean retry=true;
				boolean disconnectionException=false;
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
							
							if (savePoint != null && mustReleaseSavepointAfterCommit())
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
									if (!disconnectionException) {
										retry = true;
										disconnectionException = true;
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
								if ((savePoint != null || savePointName != null) && mustReleaseSavepointAfterRollBack()) {
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
											if (!disconnectionException) {
												retry = true;
												disconnectionException = true;
											}
										}

										break;
									}
									t = t.getCause();
								}
								if (!retry)
									throw new DatabaseIntegrityException("Impossible to rollback the database changes", se);
							}
						}
						if (!retry) {
						 	throw e;
						}
					} catch (SQLException e) {

						try {
							if (writeData)
								cw.connection.clearTransactions(false);
							rollback(cw.connection.getConnection());
							if (writeData) {
								if ((savePoint != null || savePointName != null) && mustReleaseSavepointAfterRollBack()) {
									releasePoint(cw.connection.getConnection(), savePointName, savePoint);
								}
		
							}
						} catch (SQLException se) {
							throw new DatabaseIntegrityException("Impossible to rollback the database changes", se);
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
						if (mustReleaseSavepointAfterRollBack())
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
		 * DatabaseIntegrityException("Impossible to rollback the database changes",
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
				if (_class_table.getPackage().equals(DatabaseWrapper.class.getPackage()) && (actualDatabaseLoading == null
						|| !actualDatabaseLoading.getConfiguration().getDatabaseSchema().getPackage().equals(_class_table.getPackage()) )) {
					loadDatabase(new DatabaseConfiguration(new DatabaseSchema(_class_table.getPackage(), internalDatabaseClassesList)), 0, null);
					db=sql_database.get(_class_table.getPackage());
					dpv=db.tables_per_versions.get(databaseVersion);

				} else {
					try {
						lockWrite();
						if (databaseVersion<0)
							databaseVersion=getCurrentDatabaseVersion(_class_table.getPackage());
						if (actualDatabaseLoading != null && actualDatabaseLoading.getConfiguration().getDatabaseSchema().getPackage()
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
	final void deleteDatabase(final DatabaseConfiguration configuration, boolean notifyNewCompatibleDatabases) throws DatabaseException {
		deleteDatabase(configuration, notifyNewCompatibleDatabases, getCurrentDatabaseVersion(configuration.getDatabaseSchema().getPackage()));
	}
	final void deleteDatabase(final DatabaseConfiguration configuration, boolean notifyNewCompatibleDatabases, final int databaseVersion) throws DatabaseException {
		try  {

			lockWrite();
			if (databaseLogger!=null)
				databaseLogger.fine("Start database removing: "+configuration+" (version="+databaseVersion+")");
			runTransaction(new Transaction() {

				@Override
				public Void run(DatabaseWrapper sql_connection) throws DatabaseException {

					try {
						if (!sql_database.containsKey(configuration.getDatabaseSchema().getPackage())) {
							configuration.setCreateDatabaseIfNecessaryAndCheckItDuringCurrentSession(false);
							loadDatabase(configuration, databaseVersion, null);
						}

					} catch (DatabaseException e) {
						return null;
					}

					Database db = sql_database.get(configuration.getDatabaseSchema().getPackage());
					if (db == null)
						throw new IllegalAccessError();

					ArrayList<Table<?>> list_tables = new ArrayList<>(configuration.getDatabaseSchema().getTableClasses().size());
					for (Class<? extends Table<?>> c : configuration.getDatabaseSchema().getTableClasses()) {
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
					db=sd.get(configuration.getDatabaseSchema().getPackage());
					db.tables_per_versions.remove(databaseVersion);
					if (db.tables_per_versions.size()==0)
						sd.remove(configuration.getDatabaseSchema().getPackage());
					else
						db.updateCurrentVersion();
					sql_database = sd;

					getDatabaseTable().removeRecord("databasePackageName", configuration.getDatabaseSchema().getPackage().getName());
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
			if (databaseLogger!=null)
				databaseLogger.info("Database removed: "+configuration+" (version="+databaseVersion+")");
			if (notifyNewCompatibleDatabases)
				getSynchronizer().broadcastAvailableDatabase();

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
							if (!doesTableExists(DatabaseWrapper.VERSIONS_OF_DATABASE))
								return -1;
							PreparedStatement pst = getConnectionAssociatedWithCurrentThread().getConnection()
									.prepareStatement("SELECT CURRENT_DATABASE_VERSION FROM " + DatabaseWrapper.VERSIONS_OF_DATABASE + " WHERE PACKAGE_NAME='"
											+ getLongPackageName(p) + "'" + getSqlComma());
							ResultSet rs = pst.executeQuery();
							int res=defaultValue==-1?0:defaultValue;
							if (rs.next())
								res=rs.getInt(1);
							else
							{
								Statement st = getConnectionAssociatedWithCurrentThread().getConnection()
										.createStatement();
								st.executeUpdate("INSERT INTO " + DatabaseWrapper.VERSIONS_OF_DATABASE + " (PACKAGE_NAME, CURRENT_DATABASE_VERSION) VALUES('"
										+ getLongPackageName(p) + "', " + res + ")" + getSqlComma());
								st.close();
							}
							if (fdb!=null)
								fdb.currentVersion=res;
							return res;

						} catch (Exception e) {
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
	final void validateNewDatabaseVersionAndDeleteOldVersion(final DatabaseConfiguration configuration, final int oldDatabaseVersion, final int newDatabaseVersion) throws DatabaseException {
		try {

			lockWrite();
			runTransaction(new Transaction() {
				@Override
				public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {
					try {
						assert oldDatabaseVersion != newDatabaseVersion;
						int r = getConnectionAssociatedWithCurrentThread().getConnection().createStatement()
								.executeUpdate("UPDATE " + VERSIONS_OF_DATABASE + " SET CURRENT_DATABASE_VERSION=" + newDatabaseVersion
										+ " WHERE PACKAGE_NAME='" + getLongPackageName(configuration.getDatabaseSchema().getPackage()) + "'" + getSqlComma());
						if (r != 1)
							throw new DatabaseException("no record found (r="+r+")");
						if (oldDatabaseVersion >= 0) {
							Database db = sql_database.get(configuration.getDatabaseSchema().getPackage());
							HashMap<Class<? extends Table<?>>, Table<?>> hm = new HashMap<>(db.tables_per_versions.get(oldDatabaseVersion).tables_instances);
							deleteDatabase(configuration, false, oldDatabaseVersion);
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
	 * Associate a Sql database with a given list of database configuration.
	 * The database is also restored to a given state at a given time.
	 * The data is restored from distant peers if need or from central database backup.
	 *
	 * Every table/class in the given configuration which inherits to the class
	 * <code>Table&lsaquo;T extends DatabaseRecord&rsaquo;</code> will be included
	 * into the same database. This function must be called before every any
	 * operation with the corresponding tables.
	 *
	 * @param configurations
	 *            the database configuration list
	 * @param timeUTCOfRestorationInMs the time UTC in milliseconds of the point of restoration.
	 *                                 Every modification in the database after that point is excluded.
	 *                                 However, it is possible to recover deleted modification by restoring
	 *                                 the database to a point located after that point (see {@link BackupRestoreManager#restoreDatabaseToDateUTC(long)})
	 * @param peersToRemove peers to remove from synchronization
	 * @throws DatabaseException
	 *             if the given package is already associated to a database, or if
	 *             the database cannot be created.
	 * @throws NullPointerException
	 *             if the given parameters are null.
	 */
	final void loadDatabaseAndRestoreItToGivenTime(final Collection<DatabaseConfiguration> configurations,
														  long timeUTCOfRestorationInMs,
														  Collection<DecentralizedValue> peersToRemove,
												   			DatabaseLifeCycles lifeCycles) throws DatabaseException {
		loadDatabase(configurations,  timeUTCOfRestorationInMs, peersToRemove, lifeCycles);
	}
	/**
	 * Associate a Sql database with a given database configuration.
	 * The database is also restored to a given state at a given time.
	 * The data is restored from distant peers if need or from central database backup.
	 *
	 * Every table/class in the given configuration which inherits to the class
	 * <code>Table&lsaquo;T extends DatabaseRecord&rsaquo;</code> will be included
	 * into the same database. This function must be called before every any
	 * operation with the corresponding tables.
	 *
	 * @param configuration
	 *            the database configuration
	 * @param timeUTCOfRestorationInMs the time UTC in milliseconds of the point of restoration.
	 *                                 Every modification in the database after that point is excluded.
	 *                                 However, it is possible to recover deleted modification by restoring
	 *                                 the database to a point located after that point (see {@link BackupRestoreManager#restoreDatabaseToDateUTC(long)})
	 * @param peersToRemove peers to remove from synchronization
	 * @throws DatabaseException
	 *             if the given package is already associated to a database, or if
	 *             the database cannot be created.
	 * @throws NullPointerException
	 *             if the given parameters are null.
	 */
	final void loadDatabaseAndRestoreItToGivenTime(final DatabaseConfiguration configuration,
														  long timeUTCOfRestorationInMs,
														  Collection<DecentralizedValue> peersToRemove, DatabaseLifeCycles lifeCycles) throws DatabaseException {
		loadDatabase(Collections.singleton(configuration),  timeUTCOfRestorationInMs, peersToRemove, lifeCycles);
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
	 * @throws DatabaseException
	 *             if the given package is already associated to a database, or if
	 *             the database cannot be created.
	 * @throws NullPointerException
	 *             if the given parameters are null.
	 */
	final void loadDatabase(final DatabaseConfiguration configuration, DatabaseLifeCycles lifeCycles) throws DatabaseException {
		loadDatabase(Collections.singleton(configuration),  null, null, lifeCycles);
	}
	/**
	 * Associate a Sql database with a given list of database configuration. Every
	 * table/class in the given configuration which inherits to the class
	 * <code>Table&lsaquo;T extends DatabaseRecord&rsaquo;</code> will be included
	 * into the same database. This function must be called before every any
	 * operation with the corresponding tables.
	 *
	 * @param configurations
	 *            the database configuration list
	 * @throws DatabaseException
	 *             if the given package is already associated to a database, or if
	 *             the database cannot be created.
	 * @throws NullPointerException
	 *             if the given parameters are null.
	 */
	final boolean loadDatabase(final Collection<DatabaseConfiguration> configurations, DatabaseLifeCycles lifeCycles) throws DatabaseException {
		return loadDatabase(configurations, null, null, lifeCycles);
	}

	private boolean isDatabaseLoadedImpl(DatabaseConfiguration configuration, int databaseVersion) throws DatabaseException {
		Database db = sql_database.get(configuration.getDatabaseSchema().getPackage());
		if (db != null) {
			return (databaseVersion == -1 && db.tables_per_versions.containsKey(db.getCurrentVersion()))
					|| (databaseVersion != -1 && db.tables_per_versions.containsKey(databaseVersion));
		}
		return false;
	}
	boolean isDatabaseLoaded(DatabaseConfiguration configuration) throws DatabaseException {
		lockRead();
		try{
			return isDatabaseLoadedImpl(configuration, -1);

		}
		finally {
			unlockRead();
		}
	}

	final boolean loadDatabaseImpl(final DatabaseConfiguration configuration,
								final boolean internalPackage,
								int databaseVersion,
								/*final Reference<Boolean> restoreSynchronizerHosts,
								final Reference<Collection<DatabaseHooksTable.Record>> hosts,*/
								final Reference<Boolean> allNotFounds,
								final DatabaseLifeCycles lifeCycles) throws DatabaseException {

		//final AtomicBoolean allNotFound = new AtomicBoolean(true);
		if (this.closed)
			throw new DatabaseException("The given Database was closed : " + this);
		if (databaseLogger!=null)
			databaseLogger.fine("Start configuration loading: "+configuration);
		if (isDatabaseLoadedImpl(configuration, databaseVersion))
			throw new DatabaseException("There is already a database associated to the given wrapper ");

		boolean oldDatabaseReplaced=false;
		boolean allNotFound=loadDatabaseTables(configuration, databaseVersion);
		if (allNotFounds.get() && allNotFound)
			allNotFounds.set(true);

		Database actualDatabaseLoading=this.actualDatabaseLoading;
		assert actualDatabaseLoading!=null;
		if (databaseVersion==-1)
			databaseVersion=getCurrentDatabaseVersion(configuration.getDatabaseSchema().getPackage());
		boolean initBackupRestore=false;


		if (!internalPackage && allNotFound) {

			try {

				this.actualDatabaseLoading=null;
				/*if (hosts.get()==null)
					hosts.set(getSynchronizer().resetSynchronizerAndGetAllHosts());*/
				this.actualDatabaseLoading=actualDatabaseLoading;
				int currentVersion=getCurrentDatabaseVersion(configuration.getDatabaseSchema().getPackage());
				if (currentVersion==databaseVersion) {
					DatabaseSchema oldSchema = configuration.getDatabaseSchema().getOldSchema();
					DatabaseConfiguration oldConfig=null;
					boolean removeOldDatabase = false;
					if (oldSchema != null && lifeCycles != null) {
						try {
							Database db=sql_database.get(oldSchema.getPackage());
							if (db!=null)
								oldConfig=db.configuration;
							else {
								oldConfig = new DatabaseConfiguration(oldSchema, DatabaseConfiguration.SynchronizationType.NO_SYNCHRONIZATION, null, configuration.getBackupConfiguration(), false);
								this.actualDatabaseLoading = null;
								loadDatabase(oldConfig, lifeCycles);
							}
							this.actualDatabaseLoading=actualDatabaseLoading;
							if (databaseLogger!=null)
								databaseLogger.fine("Transfer database from old to new configuration: "+configuration);
							lifeCycles.transferDatabaseFromOldVersion(this, configuration);
							if (databaseLogger!=null)
								databaseLogger.fine("Transfer OK ");
							oldDatabaseReplaced=true;
							removeOldDatabase = lifeCycles.hasToRemoveOldDatabase(oldConfig);
						} catch (DatabaseException e) {
							e.printStackTrace();
							oldConfig = null;
						}
					}
					if (lifeCycles != null) {
						if (databaseLogger!=null)
							databaseLogger.fine("Starting post database creation script OK: "+configuration);
						lifeCycles.afterDatabaseCreation(this, configuration);
						if (databaseLogger!=null)
							databaseLogger.fine("Post database creation script OK: "+configuration);
						if (removeOldDatabase)
							deleteDatabase(oldConfig, false, databaseVersion);
					}
					initBackupRestore=true;
				}
				//restoreSynchronizerHosts.set(true);



			} catch (Exception e) {
				throw Objects.requireNonNull(DatabaseException.getDatabaseException(e));
			}
		}


		HashMap<Package, Database> sd = new HashMap<>(sql_database);
		Database db=sd.get(configuration.getDatabaseSchema().getPackage());

		if (db==null) {
			sd.put(configuration.getDatabaseSchema().getPackage(), actualDatabaseLoading);
		}
		else {
			DatabasePerVersion dpv=actualDatabaseLoading.tables_per_versions.get(databaseVersion);
			if (dpv==null)
				throw new InternalError();

			db.tables_per_versions.put(databaseVersion, dpv);
			//db.updateCurrentVersion();
		}
		sql_database = sd;
		configuration.getDatabaseSchema().setDatabaseWrapper(this);
		if (initBackupRestore)
			actualDatabaseLoading.initBackupRestoreManager(this, getDatabaseDirectory(), configuration);
		if (databaseLogger!=null)
			databaseLogger.info("Configuration loaded: "+configuration);
		return oldDatabaseReplaced;

	}

	/*final void postLoadDatabase(Collection<DatabaseConfiguration> configurations/*,
								Reference<Boolean> restoreSynchronizerHosts,
								Reference<Collection<DatabaseHooksTable.Record>> hosts,
								DatabaseLifeCycles lifeCycles*///) throws DatabaseException {
		/*if (restoreSynchronizerHosts.get()) {
			HashMap<String, Boolean> databases=new HashMap<>();
			configurations.forEach(v -> databases.put(v.getDatabaseSchema().getPackage().getName(), lifeCycles != null && lifeCycles.replaceDistantConflictualRecordsWhenDistributedDatabaseIsResynchronized(v)));

			getSynchronizer().restoreHosts(hosts.get(), databases);
		}*/
/*		if (configurations.stream().anyMatch(DatabaseConfiguration::isSynchronizedWithCentralBackupDatabase) && getSynchronizer().isInitializedWithCentralBackup())
			getSynchronizer().privInitConnectionWithDistantBackupCenter();*/
	//}
	final void postLoadDatabaseFinal( Reference<Boolean> allNotFound, boolean internalDatabasePackage) throws DatabaseException {
		actualDatabaseLoading = null;
		if (!allNotFound.get() && !internalDatabasePackage)
			getSynchronizer().isReliedToDistantHook();
	}
	final void loadDatabase(final DatabaseConfiguration configuration, int databaseVersion,
							final DatabaseLifeCycles lifeCycles) throws DatabaseException {

		boolean internalPackage=configuration.getDatabaseSchema().getPackage().equals(DatabaseWrapper.class.getPackage());
		if (!internalPackage) {
			if (configuration.isCreateDatabaseIfNecessaryAndCheckItDuringCurrentSession())
				getTableInstance(DatabaseTable.class, databaseVersion);
		}
		else
			databaseVersion=0;
		Reference<Boolean> allNotFound=new Reference<>(false);
		try  {
			lockWrite();
			loadDatabaseImpl(configuration,
					internalPackage,
					databaseVersion,
					/*restoreSynchronizerHosts,
					hosts,*/
					allNotFound,
					lifeCycles);
			//postLoadDatabase(Collections.singleton(configuration)/*, restoreSynchronizerHosts, hosts, lifeCycles*/);
			postLoadDatabaseFinal(allNotFound, internalPackage);

		}
		finally
		{
			unlockWrite();
		}
	}
	final void checkDatabaseToUnload() throws DatabaseException {
		try  {
			lockWrite();
			HashMap<Package, Database> sd = new HashMap<>(sql_database);
			for (Iterator<Map.Entry<Package, Database>> it=sd.entrySet().iterator();it.hasNext();)
			{
				Map.Entry<Package, Database> e=it.next();

				if (reservedDatabases.contains(e.getValue().getConfiguration().getDatabaseSchema().getPackage()))
					continue;
				boolean found=false;
				for (DatabaseConfiguration configuration : getDatabaseConfigurationsBuilder().getConfigurations().getConfigurations())
				{
					if (configuration.getDatabaseSchema().getPackage().equals(e.getKey()))
					{
						found=true;
						break;
					}
				}
				if (!found) {
					if (databaseLogger!=null)
						databaseLogger.info("Unloading configuration: "+e.getValue().getConfiguration());
					it.remove();
				}
			}
			sql_database = sd;
			getSynchronizer().broadcastAvailableDatabase();
		}
		finally
		{
			unlockWrite();
		}
	}
	//TODO manage time of restoration and peers to remove
	final boolean loadDatabase(final Collection<DatabaseConfiguration> configurations,
			 Long timeOfRestoration, Collection<DecentralizedValue> peersToRemove,
							final DatabaseLifeCycles lifeCycles) throws DatabaseException {
		if (configurations == null)
			throw new NullPointerException("tables is a null pointer.");
		if (configurations.size()==0)
			throw new IllegalArgumentException();

		if (peersToRemove!=null && peersToRemove.size()==0)
			peersToRemove=null;
		if (peersToRemove!=null)
		{
			if (peersToRemove.stream().anyMatch(Objects::isNull))
				throw new NullPointerException();
			DecentralizedValue ldv=getSynchronizer().getLocalHostID();
			if (ldv!=null && peersToRemove.contains(ldv))
				throw new IllegalArgumentException();

		}

		if (configurations.stream().anyMatch(DatabaseConfiguration::isCreateDatabaseIfNecessaryAndCheckItDuringCurrentSession))
			getTableInstance(DatabaseTable.class, -1);

		boolean isCentralDBBackupInitialized= getSynchronizer().isInitializedWithCentralBackup();
		if (isCentralDBBackupInitialized)
			getSynchronizer().centralDatabaseBackupDisconnected();

		Reference<Boolean> allNotFound=new Reference<>(false);
		try  {
			boolean oldDatabaseReplaced=false;
			lockWrite();

			for (DatabaseConfiguration configuration : configurations) {
				int databaseVersion = -1;

				oldDatabaseReplaced|=loadDatabaseImpl(configuration,
						false,
						databaseVersion,
						/*restoreSynchronizerHosts,
						hosts,*/
						allNotFound, lifeCycles);

			}
			if (isCentralDBBackupInitialized)
				getSynchronizer().centralDatabaseBackupAvailable();
			//postLoadDatabase(configurations/*, restoreSynchronizerHosts, hosts, lifeCycles*/);
			postLoadDatabaseFinal(allNotFound, false);
			getSynchronizer().broadcastAvailableDatabase();
			return oldDatabaseReplaced;
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

	protected boolean supportsItalicQuotesWithTableAndFieldNames()
	{
		return true;
	}

	/*
	 * result found into actualDatabaseLoading
	 */
	final boolean loadDatabaseTables(final DatabaseConfiguration configuration, final int _version) throws DatabaseException {

		return (boolean)runTransaction(new Transaction() {

			@Override
			public Boolean run(DatabaseWrapper sql_connection) throws DatabaseException {
				try {

					boolean allNotFound=true;
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
								+", CONSTRAINT ROW_PROPERTIES_OF_TABLES_PK_CONSTRAINT PRIMARY KEY(TABLE_ID, TABLE_VERSION))"+getPostCreateTable(1L)
								+ getSqlComma());
						st.close();
					}
					int version=_version;
					if (!doesTableExists(VERSIONS_OF_DATABASE)) {
						Statement st = getConnectionAssociatedWithCurrentThread().getConnection()
								.createStatement();
						st.executeUpdate("CREATE TABLE " + VERSIONS_OF_DATABASE
								+ " (PACKAGE_NAME VARCHAR(512), CURRENT_DATABASE_VERSION INTEGER"
								+", CONSTRAINT VERSION_DB_ID_PK PRIMARY KEY(PACKAGE_NAME))"+getPostCreateTable(null)
								+ getSqlComma());
						st.close();
						if (version==-1)
							version=0;
						st = getConnectionAssociatedWithCurrentThread().getConnection()
								.createStatement();
						st.executeUpdate("INSERT INTO " + DatabaseWrapper.VERSIONS_OF_DATABASE + " (PACKAGE_NAME, CURRENT_DATABASE_VERSION) VALUES('"
								+ getLongPackageName(configuration.getDatabaseSchema().getPackage()) + "', " + version + ")" + getSqlComma());
						st.close();
					}
					else {
						int v = getCurrentDatabaseVersion(configuration.getDatabaseSchema().getPackage(), false, version);
						if (version==-1)
							version=v;
					}



					ArrayList<Table<?>> list_tables = new ArrayList<>(configuration.getDatabaseSchema().getTableClasses().size());
					DatabasePerVersion dpv=new DatabasePerVersion();
					actualDatabaseLoading.tables_per_versions.put(version, dpv);
					//actualDatabaseLoading.updateCurrentVersion();
					for (Class<? extends Table<?>> class_to_load : configuration.getDatabaseSchema().getTableClasses()) {
						Table<?> t = newInstance(class_to_load, version);
						list_tables.add(t);
						dpv.tables_instances.put(class_to_load, t);
					}
					for (Table<?> t : list_tables) {
						t.initializeStep1(configuration);
					}
					for (Table<?> t : list_tables) {
						allNotFound=
								!t.initializeStep2(configuration.isCreateDatabaseIfNecessaryAndCheckItDuringCurrentSession()) && allNotFound;
					}
					for (Table<?> t : list_tables) {
						t.initializeStep3();
					}
					if (!configuration.getDatabaseSchema().getPackage().equals(this.getClass().getPackage())) {
						Database actualDatabaseLoading = DatabaseWrapper.this.actualDatabaseLoading;
						DatabaseWrapper.this.actualDatabaseLoading = null;
						DatabaseTable.Record dbt = getDatabaseTable().getRecord("databasePackageName", configuration.getDatabaseSchema().getPackage().getName());
						if (dbt == null) {
							getDatabaseTable().addRecord(new DatabaseTable.Record(configuration.getDatabaseSchema().getPackage().getName(), configuration.isSynchronizedWithCentralBackupDatabase()));
						}
						else if (dbt.isSynchronizedWithCentralDatabaseBackup()!=configuration.isSynchronizedWithCentralBackupDatabase())
						{
							getDatabaseTable().updateRecord(dbt, "synchronizedWithCentralDatabaseBackup", configuration.isSynchronizedWithCentralBackupDatabase());
						}
						DatabaseWrapper.this.actualDatabaseLoading = actualDatabaseLoading;
					}
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
			public void initOrReset() throws DatabaseException {
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
		DefaultConstructorAccessPrivilegedAction<TT> class_privilege = new DefaultConstructorAccessPrivilegedAction<>(
                _class_table);

		Constructor<TT> const_table = AccessController.doPrivileged(class_privilege);


		TT t = const_table.newInstance();
		t.initializeStep0(this, databaseVersion);
		return t;
	}

	protected abstract boolean doesTableExists(String tableName) throws Exception;

	protected final ColumnsReadQuery getColumnMetaData(String tableName) throws Exception
	{
		return getColumnMetaData(tableName, null);
	}


	protected abstract ColumnsReadQuery getColumnMetaData(String tableName, String columnName) throws Exception;

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


	protected abstract String getDropTableCascadeQuery(Table<?> table);


	protected abstract boolean supportSingleAutoPrimaryKeys();

	protected abstract boolean supportMultipleAutoPrimaryKeys();

	Collection<Table<?>> getListTables(Package p, int databaseVersion) {
		Database db = this.sql_database.get(p);
		if (db == null) {
			try  {
				lockWrite();
				if (actualDatabaseLoading != null && actualDatabaseLoading.getConfiguration().getDatabaseSchema().getPackage().equals(p))
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
	public static void deleteDatabasesFiles(File _directory) {
		if (_directory.exists() && _directory.isDirectory()) {
			FileTools.deleteDirectory(_directory);
		}
	}


	/**
	 * Delete all the files associated to this database
	 *
	 */
	public void deleteDatabasesFiles() {
		Logger logger=databaseLogger;
		close();
		if (logger!=null)
			logger.fine("Start databases files removing: "+getDatabaseDirectory());
		deleteDatabasesFiles(getDatabaseDirectory());
		if (logger!=null)
			logger.info("Start databases files removed: "+getDatabaseDirectory());
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
				DatabaseTransactionsIdentifiersToSynchronize.class,
				LastIDCorrection.class,
				LastIDCorrectionFromCentralDatabaseBackup.class,
				TransactionConfirmationEvents.class,
				DatabaseEventsToSynchronizeP2P.class,
				HookSynchronizeRequest.class,
				HookDesynchronizeRequest.class,
				HookRemoveRequest.class,
				BackupChannelInitializationMessageFromCentralDatabaseBackup.class,
				BackupChannelUpdateMessageFromCentralDatabaseBackup.class,
				EncryptedMetaDataFromCentralDatabaseBackup.class,
				AskForMetaDataPerFileToCentralDatabaseBackup.class,
				AskForDatabaseBackupPartDestinedToCentralDatabaseBackup.class,
				P2PConnexionInitialization.class,
				DatabaseBackupToRemoveDestinedToCentralDatabaseBackup.class,
				DistantBackupCenterConnexionInitialisation.class,
				DisconnectCentralDatabaseBackup.class,
				IndirectMessagesDestinedToAndComingFromCentralDatabaseBackup.class,
				InitialMessageComingFromCentralBackup.class,
				LastValidatedDistantTransactionDestinedToCentralDatabaseBackup.class,
				TransactionConfirmationEvents.class,
				EncryptedBackupPartForRestorationComingFromCentralDatabaseBackup.class,
				CompatibleDatabasesP2PMessage.class,
				CompatibleDatabasesMessageDestinedToCentralDatabaseBackup.class,
				CompatibleDatabasesMessageComingFromCentralDatabaseBackup.class

		));
		for (Class<?> c : classes)
			assert !Modifier.isAbstract(c.getModifiers()):""+c;

		ArrayList<Class<? extends Enum<?>>> enums = new ArrayList<>(Arrays.asList(
				DatabaseEventType.class,
				TransactionIsolation.class,
				DatabaseConfiguration.SynchronizationType.class));

		SerializationTools.addPredefinedClasses(classes, enums);
	}


}
