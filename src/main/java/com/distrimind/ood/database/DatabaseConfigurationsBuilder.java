package com.distrimind.ood.database;

import com.distrimind.ood.database.centraldatabaseapi.CentralDatabaseBackupCertificate;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.messages.PeerToAddMessageDestinedToCentralDatabaseBackup;
import com.distrimind.ood.database.messages.PeerToRemoveMessageDestinedToCentralDatabaseBackup;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.Reference;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.EncryptionProfileProvider;

import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Logger;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 3.0.0
 */
public class DatabaseConfigurationsBuilder {
	private final DatabaseConfigurations configurations;
	private final DatabaseWrapper wrapper;
	private final DatabaseLifeCycles lifeCycles;
	private final EncryptionProfileProvider signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,
			encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup,
			protectedSignatureProfileProviderForAuthenticatedP2PMessages;
	private final AbstractSecureRandom secureRandom;


	DatabaseConfigurationsBuilder(DatabaseConfigurations configurations,
								  DatabaseWrapper wrapper,
								  DatabaseLifeCycles lifeCycles,
								  EncryptionProfileProvider signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup,
								  EncryptionProfileProvider encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup,
								  EncryptionProfileProvider protectedSignatureProfileProviderForAuthenticatedP2PMessages,
								  AbstractSecureRandom secureRandom,
								  boolean createDatabasesIfNecessaryAndCheckIt) throws DatabaseException {
		if (configurations==null)
			configurations=new DatabaseConfigurations();
		if (wrapper==null)
			throw new NullPointerException();
		if (configurations.useCentralBackupDatabase()) {
			if (signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup == null)
				throw new NullPointerException();
			if (encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup == null)
				throw new NullPointerException();
		}
		if (protectedSignatureProfileProviderForAuthenticatedP2PMessages ==null && configurations.isDecentralized())
			throw new NullPointerException();
		if (secureRandom==null && (protectedSignatureProfileProviderForAuthenticatedP2PMessages !=null || signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup!=null || encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup!=null))
			throw new NullPointerException();

		this.configurations = configurations;
		this.wrapper = wrapper;
		this.lifeCycles = lifeCycles;
		this.signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup = signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup;
		this.encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup = encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup;
		this.protectedSignatureProfileProviderForAuthenticatedP2PMessages = protectedSignatureProfileProviderForAuthenticatedP2PMessages;
		this.secureRandom=secureRandom;
		boolean save=configurations.checkDistantPeers();
		configurations.setCreateDatabasesIfNecessaryAndCheckIt(createDatabasesIfNecessaryAndCheckIt);

		if (save && lifeCycles!=null)
		{
			lifeCycles.saveDatabaseConfigurations(configurations);
		}

	}

	public boolean isCommitInProgress() {
		synchronized (this)
		{
			return commitInProgress;
		}
	}


	private static class Transaction {
		final ArrayList<ConfigurationQuery> queries =new ArrayList<>();
		private boolean propagate=false;
		private boolean checkPeersToAdd;
		private boolean checkInitLocalPeer=false;
		private boolean checkDatabaseToSynchronize=false;
		private boolean checkDatabaseToUnload =false;
		private boolean checkDisconnections =false;
		private boolean updateConfigurationPersistence=false;
		private boolean checkNewConnexions=false;
		private boolean checkDatabaseToDesynchronize =false;
		private boolean checkDatabaseLoading=false;
		private boolean checkInitCentralDatabaseBackup=false;
		private Set<DecentralizedValue> removedPeersID=null;
		private Set<DatabaseConfiguration> configurationsToDefinitivelyDelete;
		private Set<DatabaseConfiguration> configurationsToLoad;
		private final boolean usedCentralBackupDatabase;

		Transaction(DatabaseConfigurationsBuilder builder)
		{
			usedCentralBackupDatabase=builder.configurations.useCentralBackupDatabase();
		}
		void updateConfigurationPersistence() {
			updateConfigurationPersistence = true;
		}

		void checkNewConnexions() {
			checkNewConnexions=true;
		}
		void checkInitCentralDatabaseBackup()
		{
			checkInitCentralDatabaseBackup=true;
		}
		void checkInitLocalPeer()
		{
			checkInitLocalPeer=true;
		}
		void checkPeersToAdd()
		{
			checkPeersToAdd=true;
		}
		void checkConnexionsToDesynchronize() {
			checkDatabaseToDesynchronize =true;
		}
		void addIDsToRemove(Collection<DecentralizedValue> ids)
		{
			if (ids==null)
				throw new NullPointerException();
			if (removedPeersID==null)
				removedPeersID=new HashSet<>();
			removedPeersID.addAll(ids);
		}
		void addIDToRemove(DecentralizedValue dv)
		{
			if (dv==null)
				throw new NullPointerException();
			if (removedPeersID==null)
				removedPeersID=new HashSet<>();
			removedPeersID.add(dv);
		}
  		void checkDatabaseLoading(DatabaseConfiguration loadedDatabase) throws DatabaseException {
			if(configurationsToDefinitivelyDelete!=null && configurationsToDefinitivelyDelete.contains(loadedDatabase))
				throw new DatabaseException("Cannot load database which was removed into the same transaction");
			if (loadedDatabase!=null) {
				if (configurationsToLoad == null)
					configurationsToLoad = new HashSet<>();
				configurationsToLoad.add(loadedDatabase);
			}
			checkDatabaseLoading=true;
			checkDatabaseToSynchronize();
			checkNewConnexions();
		}

		void checkNotRemovedID(DecentralizedValue localPeerId) throws DatabaseException {
			if (removedPeersID!=null && removedPeersID.contains(localPeerId))
				throw new DatabaseException("The id "+localPeerId+" was already removed !");
		}

		void checkNotRemovedIDs(Collection<DecentralizedValue> distantPeers) throws DatabaseException {
			if (removedPeersID!=null && distantPeers.stream().anyMatch(v-> removedPeersID.contains(v)))
				throw new DatabaseException("There are ids that have been removed !");
		}
		void checkDisconnections()
		{
			checkDisconnections =true;
		}
		void checkDatabaseToUnload(DatabaseConfiguration configurationToDefinitivelyDelete) throws DatabaseException {
			checkDatabaseToUnload =true;
			if(configurationsToLoad!=null && configurationsToLoad.contains(configurationToDefinitivelyDelete))
				throw new DatabaseException("Cannot remove database which was added into the same transaction");

			if (configurationToDefinitivelyDelete!=null) {
				if (this.configurationsToDefinitivelyDelete==null)
					this.configurationsToDefinitivelyDelete=new HashSet<>();
				this.configurationsToDefinitivelyDelete.add(configurationToDefinitivelyDelete);
			}
		}
		void checkDatabaseToSynchronize()
		{
			checkDatabaseToSynchronize=true;
		}


	}
	private interface ConfigurationQuery
	{
		void execute(Transaction transaction) throws DatabaseException;
	}

	private Transaction currentTransaction=null;
	private boolean commitInProgress=false;

	public DatabaseConfigurations getConfigurations() {
		return configurations;
	}
	public DatabaseConfiguration getDatabaseConfiguration(Package databasePackage) {
		return configurations.getDatabaseConfiguration(databasePackage);
	}
	public DatabaseConfiguration getDatabaseConfiguration(String databasePackage) {
		return configurations.getDatabaseConfiguration(databasePackage);
	}

	private void pushQuery(ConfigurationQuery query) {
		synchronized (this) {
			if (currentTransaction==null)
				currentTransaction=new Transaction(this);
			if (commitInProgress)
				throw new IllegalAccessError();
			currentTransaction.queries.add(query);
		}
	}


	public void commit() throws DatabaseException {
		Set<DecentralizedValue> peersAdded=null;
		synchronized (this) {
			if (currentTransaction==null)
				throw new DatabaseException("No query was added ! Nothing to commit !");
			if (commitInProgress)
				throw new IllegalAccessError();
			commitInProgress=true;
			wrapper.finalizer.lockWrite();

			try {
				for (ConfigurationQuery q : currentTransaction.queries)
					q.execute(currentTransaction);
				if (currentTransaction.updateConfigurationPersistence && lifeCycles!=null)
					lifeCycles.saveDatabaseConfigurations(configurations);
				if (currentTransaction.checkDatabaseLoading)
					checkDatabaseLoading();
				if (currentTransaction.checkDatabaseToDesynchronize)
					currentTransaction.checkDatabaseToUnload |=checkDatabaseToDesynchronize();
				if (currentTransaction.removedPeersID!=null)
					currentTransaction.checkDisconnections |=checkConnexionsToRemove();
				if (currentTransaction.checkDatabaseToUnload)
					checkDatabaseToUnload();
				if (currentTransaction.checkInitLocalPeer) {
					checkInitLocalPeer();
				}
				if (currentTransaction.checkPeersToAdd) {
					peersAdded=checkPeersToAdd();
					currentTransaction.checkDatabaseToSynchronize |=peersAdded!=null && peersAdded.size()>0;
				}
				if (currentTransaction.checkDatabaseToSynchronize)
					checkDatabaseToSynchronize();
				if (currentTransaction.checkNewConnexions) {
					checkConnexions();
				}
				if (currentTransaction.checkDisconnections) {
					checkDisconnections();
				}
				if (currentTransaction.checkInitCentralDatabaseBackup)
					checkInitCentralDatabaseBackup();
			}
			finally {
				currentTransaction = null;
				commitInProgress=false;
				wrapper.finalizer.unlockWrite();
			}
		}
		DatabaseNotifier notifier=wrapper.getSynchronizer().getNotifier();
		if (peersAdded!=null && notifier!=null)
		{
			notifier.hostsAdded(peersAdded);
		}
	}

	void databaseWrapperLoaded() throws DatabaseException {

		pushQuery(t-> {
			t.checkDatabaseLoading(null);
			t.checkConnexionsToDesynchronize();
			t.removedPeersID=new HashSet<>();
			t.checkDatabaseToUnload(null);
			t.checkPeersToAdd();
			t.checkInitLocalPeer();
			t.checkDatabaseToSynchronize();
			t.checkNewConnexions();
			t.checkDisconnections();
			t.checkInitCentralDatabaseBackup();
		});
		commit();
	}

	private void checkInitCentralDatabaseBackup() throws DatabaseException {
		if (wrapper.getSynchronizer().centralBackupAvailable)
			wrapper.getSynchronizer().centralDatabaseBackupAvailable();
	}

	public void rollBack()
	{
		synchronized (this)
		{
			if (commitInProgress)
				throw new IllegalAccessError();
			currentTransaction=null;
		}
	}


	public EncryptionProfileProvider getEncryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup() {
		return encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup;
	}

	public EncryptionProfileProvider getSignatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup() {
		return signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup;
	}

	public EncryptionProfileProvider getProtectedSignatureProfileProviderForAuthenticatedP2PMessages() {
		return protectedSignatureProfileProviderForAuthenticatedP2PMessages;
	}

	AbstractSecureRandom getSecureRandom() {
		return secureRandom;
	}
	public DatabaseConfigurationsBuilder addConfiguration(DatabaseConfiguration configuration, boolean makeConfigurationLoadingPersistent )
	{
		return addConfiguration(configuration, makeConfigurationLoadingPersistent, configuration.isCreateDatabaseIfNecessaryAndCheckItDuringLoading());
	}
	public DatabaseConfigurationsBuilder addConfiguration(DatabaseConfiguration configuration, boolean makeConfigurationLoadingPersistent, boolean createDatabaseIfNecessaryAndCheckItDuringCurrentSession )
	{
		if (DatabaseWrapper.reservedDatabases.contains(configuration.getDatabaseSchema().getPackage()))
			throw new IllegalArgumentException("Impossible to add a database whose package "+configuration.getDatabaseSchema().getPackage()+" corresponds to an internal OOD database : "+DatabaseWrapper.reservedDatabases);
		pushQuery((t) -> {
			if (configuration.isSynchronizedWithCentralBackupDatabase()) {
				if (encryptionProfileProviderForE2EDataDestinedCentralDatabaseBackup == null)
					throw new IllegalArgumentException("Cannot add configuration without encryption profile provider for central backup database");
				if (signatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup == null)
					throw new IllegalArgumentException("Cannot add configuration without signature profile provider for central backup database");
			}
			if (configuration.isDecentralized() && protectedSignatureProfileProviderForAuthenticatedP2PMessages ==null)
				throw new IllegalArgumentException("Cannot add configuration without protected encryption profile provider for authenticated P2P messages");
			configuration.setCreateDatabaseIfNecessaryAndCheckItDuringCurrentSession(createDatabaseIfNecessaryAndCheckItDuringCurrentSession);
			if (configurations.addConfiguration(configuration, makeConfigurationLoadingPersistent)) {
				t.checkConnexionsToDesynchronize();
			}
			t.propagate=true;
			if (makeConfigurationLoadingPersistent)
				t.updateConfigurationPersistence();
			t.checkDatabaseLoading(configuration);
			t.checkNewConnexions();
			t.checkDatabaseToSynchronize();
			t.checkPeersToAdd();

		});
		return this;
	}
	public DatabaseConfigurationsBuilder removeDatabaseConfiguration(DatabaseConfiguration databaseConfiguration)
	{
		return removeDatabaseConfiguration(databaseConfiguration, false);
	}
	public DatabaseConfigurationsBuilder removeDatabaseConfiguration(DatabaseConfiguration databaseConfiguration, boolean removeData)
	{
		return removeDatabaseConfiguration(databaseConfiguration.getDatabaseSchema().getPackage(), removeData);
	}
	public DatabaseConfigurationsBuilder removeDatabaseConfiguration(Package databasePackage)
	{
		return removeDatabaseConfiguration(databasePackage, false);
	}
	public DatabaseConfigurationsBuilder removeDatabaseConfiguration(Package databasePackage, boolean removeData)
	{
		return removeDatabaseConfiguration(databasePackage.getName(), removeData);
	}
	public DatabaseConfigurationsBuilder removeDatabaseConfiguration(String databasePackage)
	{
		return removeDatabaseConfiguration(databasePackage, false);
	}
	public DatabaseConfigurationsBuilder removeDatabaseConfiguration(String databasePackage, boolean removeData)
	{
		if (databasePackage==null)
			throw new NullPointerException();
		if (DatabaseWrapper.reservedDatabases.stream().map(Package::getName).anyMatch(c->c.equals(databasePackage)))
			throw new IllegalArgumentException("Impossible to remove a database whose package "+databasePackage+" corresponds to an internal OOD database : "+DatabaseWrapper.reservedDatabases);

		pushQuery((t) -> {
			DatabaseConfiguration c=configurations.getDatabaseConfiguration(databasePackage);
			if (c!=null && configurations.removeConfiguration(databasePackage))
			{
				t.updateConfigurationPersistence();
				t.checkDatabaseToUnload(removeData?c:null);
				t.checkDisconnections();
				t.checkConnexionsToDesynchronize();
			}

		});
		return this;
	}


	void checkInitLocalPeer() throws DatabaseException {

		wrapper.getSynchronizer().checkInitLocalPeer();
	}

	private void checkConnexions() throws DatabaseException {
		//checkInitLocalPeer();
		wrapper.getSynchronizer().checkConnexionsToInit();
	}
	private void checkDisconnections() throws DatabaseException {
		wrapper.getSynchronizer().checkDisconnections();
	}
	private Set<DecentralizedValue> checkPeersToAdd() throws DatabaseException {
		return wrapper.runSynchronizedTransaction(new SynchronizedTransaction<>() {
			@Override
			public Set<DecentralizedValue> run() throws Exception {

				Set<DecentralizedValue> peersAdded = null;
				Set<DecentralizedValue> peersID = configurations.getDistantPeers();
				if (peersID != null && peersID.size() > 0) {
					for (DecentralizedValue dv : peersID) {
						if (!wrapper.getDatabaseHooksTable().hasRecords("concernsDatabaseHost=%cdh and hostID=%h", "cdh", false, "h", dv)) {
							wrapper.getDatabaseHooksTable().initDistantHook(dv);
							if (peersAdded == null)
								peersAdded = new HashSet<>();
							peersAdded.add(dv);
							if (configurations.useCentralBackupDatabase()) {
								wrapper.getDatabaseHooksTable().offerNewAuthenticatedMessageDestinedToCentralDatabaseBackup(
										new PeerToAddMessageDestinedToCentralDatabaseBackup(getConfigurations().getLocalPeer(), getConfigurations().getCentralDatabaseBackupCertificate(), dv),
										getSecureRandom(), getSignatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup());
							}
						}
					}
				}
				return peersAdded;
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

	@SuppressWarnings("UnusedReturnValue")
	private boolean checkDatabaseToSynchronize() throws DatabaseException {
		return wrapper.runSynchronizedTransaction(new SynchronizedTransaction<>() {
			@Override
			public Boolean run() throws Exception {

				Set<DatabaseConfiguration> packagesToSynchronize = new HashSet<>();
				for (DatabaseConfiguration c : configurations.getDatabaseConfigurations()) {
					if (c.isDecentralized()) {
						Set<DecentralizedValue> sps = c.getDistantPeersThatCanBeSynchronizedWithThisDatabase();
						if (sps != null) {
							wrapper.getDatabaseHooksTable().getRecords(new Filter<>() {
								@Override
								public boolean nextRecord(DatabaseHooksTable.Record _record) {

									if (currentTransaction.removedPeersID == null || !currentTransaction.removedPeersID.contains(_record.getHostID())) {

										if (sps.contains(_record.getHostID()) &&
												!_record.isConcernedByDatabasePackage(c.getDatabaseSchema().getPackage().getName())) {
											packagesToSynchronize.add(c);
											stopTableParsing();
										}

									}
									return false;
								}
							}, "concernsDatabaseHost=%cdh", "cdh", false);
						}
					}

				}

				if (currentTransaction.propagate) {
					wrapper.getDatabaseHooksTable().updateRecords(new AlterRecordFilter<>() {
						@Override
						public void nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException {

							for (DatabaseConfiguration c : packagesToSynchronize) {
								Map<String, Boolean> hm = new HashMap<>();
								hm.put(c.getDatabaseSchema().getPackage().getName(), DatabaseConfigurationsBuilder.this.lifeCycles != null && DatabaseConfigurationsBuilder.this.lifeCycles.replaceDistantConflictualRecordsWhenDistributedDatabaseIsResynchronized(c));
								_record.offerNewAuthenticatedP2PMessage(wrapper, new HookSynchronizeRequest(configurations.getLocalPeer(), _record.getHostID(), hm, c.getDistantPeersThatCanBeSynchronizedWithThisDatabase()), getSecureRandom(), protectedSignatureProfileProviderForAuthenticatedP2PMessages, this);
							}
						}
					}, "concernsDatabaseHost=%cdh", "cdh", false);
				}
				for (DatabaseConfiguration c : packagesToSynchronize) {
					Map<String, Boolean> hm = new HashMap<>();
					hm.put(c.getDatabaseSchema().getPackage().getName(), DatabaseConfigurationsBuilder.this.lifeCycles != null && DatabaseConfigurationsBuilder.this.lifeCycles.replaceDistantConflictualRecordsWhenDistributedDatabaseIsResynchronized(c));
					wrapper.getSynchronizer().receivedHookSynchronizeRequest(new HookSynchronizeRequest(configurations.getLocalPeer(), configurations.getLocalPeer(), hm, c.getDistantPeersThatCanBeSynchronizedWithThisDatabase()));
				}
				return packagesToSynchronize.size() > 0;
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

	private boolean checkDatabaseToDesynchronize() throws DatabaseException {

		return wrapper.runSynchronizedTransaction(new SynchronizedTransaction<>() {
			@Override
			public Boolean run() throws Exception {
				HashMap<Set<String>, Set<DecentralizedValue>> packagesToUnsynchronize = new HashMap<>();
				Filter<DatabaseHooksTable.Record> f = new Filter<>() {
					@Override
					public boolean nextRecord(DatabaseHooksTable.Record _record) {

						Set<String> packages = _record.getDatabasePackageNames();
						if (packages != null && packages.size() > 0) {
							Set<String> ptu = new HashSet<>();
							for (String p : packages) {
								Optional<DatabaseConfiguration> o = configurations.getDatabaseConfigurations().stream().filter(c -> c.getDatabaseSchema().getPackage().getName().equals(p)).findAny();
								if (o.isEmpty() || !o.get().isDecentralized() || o.get().getDistantPeersThatCanBeSynchronizedWithThisDatabase() == null || o.get().getDistantPeersThatCanBeSynchronizedWithThisDatabase().contains(_record.getHostID())) {
									ptu.add(p);
								}
							}
							if (ptu.size() > 0) {
								Set<DecentralizedValue> dvs = packagesToUnsynchronize.computeIfAbsent(ptu, k -> new HashSet<>());
								dvs.add(_record.getHostID());
							}
						}
						return false;
					}
				};
				if (currentTransaction.removedPeersID == null)
					wrapper.getDatabaseHooksTable().getRecords(f, "concernsDatabaseHost=%cdh", "cdh", false);
				else
					wrapper.getDatabaseHooksTable().getRecords(f, "concernsDatabaseHost=%cdh and hostID not in %removedPeersID", "cdh", false, "removedPeersID", currentTransaction.removedPeersID);

				if (currentTransaction.propagate) {
					wrapper.getDatabaseHooksTable().updateRecords(new AlterRecordFilter<>() {
						@Override
						public void nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException {
							for (Map.Entry<Set<String>, Set<DecentralizedValue>> e : packagesToUnsynchronize.entrySet()) {
								_record.offerNewAuthenticatedP2PMessage(wrapper, new HookDesynchronizeRequest(configurations.getLocalPeer(), _record.getHostID(), e.getKey(), e.getValue()), getSecureRandom(), protectedSignatureProfileProviderForAuthenticatedP2PMessages, this);
							}
						}
					}, "concernsDatabaseHost=%cdh", "cdh", false);

					for (Map.Entry<Set<String>, Set<DecentralizedValue>> e : packagesToUnsynchronize.entrySet()) {

						wrapper.getSynchronizer().receivedHookDesynchronizeRequest(new HookDesynchronizeRequest(configurations.getLocalPeer(), configurations.getLocalPeer(), e.getKey(), e.getValue()));
					}
				}
				return packagesToUnsynchronize.size() > 0;
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

	private boolean checkConnexionsToRemove() throws DatabaseException {
		return wrapper.runSynchronizedTransaction(new SynchronizedTransaction<>() {
			@Override
			public Boolean run() throws Exception {
				if (currentTransaction.removedPeersID != null) {
					for (DecentralizedValue peerIDToRemove : currentTransaction.removedPeersID) {
						Reference<Boolean> removeLocalNow = new Reference<>(true);
						wrapper.getDatabaseHooksTable().updateRecords(new AlterRecordFilter<>() {
							@Override
							public void nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException {
								removeLocalNow.set(false);
								_record.offerNewAuthenticatedP2PMessage(wrapper, new HookRemoveRequest(configurations.getLocalPeer(), _record.getHostID(), peerIDToRemove), getSecureRandom(), protectedSignatureProfileProviderForAuthenticatedP2PMessages, this);
							}
						}, "concernsDatabaseHost=%cdh", "cdh", false);
						if (configurations.useCentralBackupDatabase() || (currentTransaction.usedCentralBackupDatabase && wrapper.getSynchronizer() != null && wrapper.getSynchronizer().isInitializedWithCentralBackup())) {
							wrapper.getDatabaseHooksTable().offerNewAuthenticatedMessageDestinedToCentralDatabaseBackup(
									new PeerToRemoveMessageDestinedToCentralDatabaseBackup(getConfigurations().getLocalPeer(), getConfigurations().getCentralDatabaseBackupCertificate(), peerIDToRemove),
									getSecureRandom(), getSignatureProfileProviderForAuthenticatedMessagesDestinedToCentralDatabaseBackup());
						}
						if (removeLocalNow.get())
							wrapper.getDatabaseHooksTable().removeHook(false, peerIDToRemove);
						else {
							wrapper.getDatabaseHooksTable().desynchronizeDatabases(peerIDToRemove);
							wrapper.getSynchronizer().initializedHooksWithCentralBackup.remove(peerIDToRemove);
						}
					}


					return currentTransaction.removedPeersID.size() > 0;
				}
				return false;
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


	private void checkDatabaseToUnload() throws DatabaseException {
		if (currentTransaction.configurationsToDefinitivelyDelete!=null) {
			for (DatabaseConfiguration c : currentTransaction.configurationsToDefinitivelyDelete)
				wrapper.deleteDatabase(c, true);
		}
		wrapper.checkDatabaseToUnload();
	}

	private void checkDatabaseLoading() throws DatabaseException {
		ArrayList<DatabaseConfiguration> dls=new ArrayList<>();
		for (DatabaseConfiguration dc : configurations.getDatabaseConfigurations())
		{
			if (!wrapper.isDatabaseLoaded(dc)) {
				dls.add(dc);
			}
		}
		if (!dls.isEmpty())
			if (wrapper.loadDatabase(dls, lifeCycles)) {
				currentTransaction.checkDisconnections();
				currentTransaction.propagate=true;
				currentTransaction.checkDatabaseToUnload(null);
			}
		//TOTO revisit this part : take account of the restoration and time of restoration
	}

	public DatabaseConfigurationsBuilder setLocalPeerIdentifier(DecentralizedValue localPeerId, boolean permitIndirectSynchronizationBetweenPeers, boolean replace) {
		if (localPeerId==null)
			throw new NullPointerException();
		if (protectedSignatureProfileProviderForAuthenticatedP2PMessages ==null)
			throw new IllegalArgumentException("Cannot set local peer without protected encryption profile provider for authenticated P2P messages");
		pushQuery((t)-> {
			t.checkNotRemovedID(localPeerId);

			if (localPeerId.equals(configurations.getLocalPeer())) {
				if (configurations.isPermitIndirectSynchronizationBetweenPeers() != permitIndirectSynchronizationBetweenPeers) {
					if (replace) {
						wrapper.getSynchronizer().disconnectAll();
						configurations.setPermitIndirectSynchronizationBetweenPeers(permitIndirectSynchronizationBetweenPeers);
						t.updateConfigurationPersistence();
						t.checkNewConnexions();
					} else
						throw new DatabaseException("Local peer identifier is already configured !");
				}
			} else {
				DecentralizedValue removedHostID=configurations.getLocalPeer();
				if (removedHostID!=null) {
					if (replace) {
						if (wrapper.getSynchronizer().isInitialized()) {
							if (!wrapper.getSynchronizer().getLocalHostID().equals(removedHostID))
								throw new IllegalAccessError();
							wrapper.getSynchronizer().disconnectAll();
							wrapper.getSynchronizer().removeHook(removedHostID);
						}

					} else
						throw new DatabaseException("Local peer identifier is already configured !");
				}
				else if (wrapper.getSynchronizer().isInitialized())
					throw new IllegalAccessError();
				configurations.setPermitIndirectSynchronizationBetweenPeers(permitIndirectSynchronizationBetweenPeers);
				configurations.setLocalPeer(localPeerId);
				t.updateConfigurationPersistence();
				t.checkDisconnections();
				t.checkInitLocalPeer();
				t.checkNewConnexions();
				t.checkDatabaseToSynchronize();
				if (removedHostID!=null)
					t.addIDToRemove(removedHostID);

			}

		});
		return this;
	}

	public DatabaseConfigurationsBuilder synchronizeDistantPeerWithGivenAdditionalPackages(DecentralizedValue distantPeer, Package ... packages)
	{
		String[] packagesString=new String[packages.length];
		int i=0;
		for (Package p : packages)
		{
			if (p==null)
				throw new NullPointerException();
			packagesString[i++]=p.getName();
		}
		return synchronizeDistantPeerWithGivenAdditionalPackages(distantPeer, packagesString);
	}
	public DatabaseConfigurationsBuilder synchronizeDistantPeerWithGivenAdditionalPackages(DecentralizedValue distantPeer, String ... packagesString)
	{
		return synchronizeDistantPeersWithGivenAdditionalPackages(Collections.singletonList(distantPeer), packagesString);
	}
	public DatabaseConfigurationsBuilder synchronizeDistantPeersWithGivenAdditionalPackages(Collection<DecentralizedValue> distantPeers, String ... packagesString)
	{
		return synchronizeDistantPeersWithGivenAdditionalPackages(true, distantPeers, packagesString);
	}
	public DatabaseConfigurationsBuilder setSynchronizationType(DatabaseConfiguration.SynchronizationType synchronizationType, String ... packagesString)
	{
		if (synchronizationType==null)
			throw new NullPointerException();
		if (packagesString==null)
			throw new NullPointerException();
		if (packagesString.length==0)
			throw new IllegalArgumentException();
		pushQuery((t) -> {
			final Reference<Boolean> changed=new Reference<>(false);
			for (String p : packagesString)
			{
				configurations.getDatabaseConfigurations().forEach ((c) -> {

					if (c.getDatabaseSchema().getPackage().getName().equals(p) && c.setSynchronizationType(synchronizationType)) {
						changed.set(true);
						try {
							if (wrapper.isDatabaseLoaded(c))
							{
								for (Class<? extends Table<?>> clazz : c.getDatabaseSchema().getTableClasses())
								{
									Table<?> table=wrapper.getTableInstance(clazz);
									table.updateSupportSynchronizationWithOtherPeersStep1();
								}
								for (Class<? extends Table<?>> clazz : c.getDatabaseSchema().getTableClasses())
								{
									Table<?> table=wrapper.getTableInstance(clazz);
									table.updateSupportSynchronizationWithOtherPeersStep2();
								}
							}
						} catch (DatabaseException e) {
							e.printStackTrace();
						}
					}
				});
			}
			if (changed.get()) {
				t.updateConfigurationPersistence();
				t.checkDatabaseToSynchronize();
				t.checkNewConnexions();
			}
		});
		return this;
	}


	DatabaseConfigurationsBuilder synchronizeDistantPeersWithGivenAdditionalPackages(boolean checkNewConnexion, Collection<DecentralizedValue> distantPeers, String ... packagesString)
	{
		if (packagesString==null)
			throw new NullPointerException();
		if (packagesString.length==0)
			throw new IllegalArgumentException();
		if (Arrays.stream(packagesString).anyMatch(Objects::isNull))
			throw new NullPointerException();
		if (protectedSignatureProfileProviderForAuthenticatedP2PMessages ==null)
			throw new IllegalArgumentException("Cannot set local peer without protected encryption profile provider for authenticated P2P messages");
		pushQuery((t) -> {
			t.checkNotRemovedIDs(distantPeers);
			boolean changed=false;
			for (String p : packagesString)
			{
				changed|=configurations.synchronizeAdditionalDistantPeersWithGivenPackage(p, distantPeers);
			}
			if (changed) {
				t.updateConfigurationPersistence();
				t.propagate|=checkNewConnexion;
				t.checkDatabaseToSynchronize();
				if (checkNewConnexion)
					t.checkNewConnexions();
				t.checkPeersToAdd();

			}
		});
		return this;
	}
	@SuppressWarnings("UnusedReturnValue")
	public DatabaseConfigurationsBuilder addDistantPeer(DecentralizedValue distantPeer, boolean volatilePeer) {
		return addDistantPeer(true, distantPeer, volatilePeer);
	}
	DatabaseConfigurationsBuilder addDistantPeer(@SuppressWarnings("SameParameterValue") boolean checkNewConnexion, DecentralizedValue distantPeer, boolean volatilePeer) {
		if (distantPeer==null)
			throw new NullPointerException();
		if (protectedSignatureProfileProviderForAuthenticatedP2PMessages ==null)
			throw new IllegalArgumentException("Cannot set local peer without protected encryption profile provider for authenticated P2P messages");
		pushQuery((t) -> {
			t.checkNotRemovedIDs(Collections.singleton(distantPeer));
			boolean changed=configurations.addDistantPeer(distantPeer, volatilePeer);
			if (changed) {
				if (checkNewConnexion)
					t.checkNewConnexions();
				t.checkPeersToAdd();

			}
		});
		return this;
	}
	public DatabaseConfigurationsBuilder desynchronizeDistantPeerWithGivenAdditionalPackages(DecentralizedValue distantPeer, String ... packagesString)
	{
		return desynchronizeDistantPeersWithGivenAdditionalPackages(Collections.singleton(distantPeer), packagesString);
	}
	public DatabaseConfigurationsBuilder desynchronizeDistantPeersWithGivenAdditionalPackages(Collection<DecentralizedValue> distantPeers, String ... packagesString)
	{
		return desynchronizeDistantPeersWithGivenAdditionalPackages(true, distantPeers, packagesString);
	}
	DatabaseConfigurationsBuilder desynchronizeDistantPeersWithGivenAdditionalPackages(boolean propagate, Collection<DecentralizedValue> distantPeers, String ... packagesString)
	{
		if (packagesString==null)
			throw new NullPointerException();
		if (packagesString.length==0)
			throw new IllegalArgumentException();
		if (Arrays.stream(packagesString).anyMatch(Objects::isNull))
			throw new NullPointerException();
		if (protectedSignatureProfileProviderForAuthenticatedP2PMessages ==null)
			throw new IllegalArgumentException("Cannot set local peer without protected encryption profile provider for authenticated P2P messages");
		pushQuery((t) -> {
			t.checkNotRemovedIDs(distantPeers);
			boolean changed=false;
			for (String p : packagesString)
			{
				changed|=configurations.desynchronizeAdditionalDistantPeersWithGivenPackage(p, distantPeers);
			}
			if (changed) {
				t.propagate|=propagate;
				t.checkConnexionsToDesynchronize();
				t.updateConfigurationPersistence();
				t.checkInitLocalPeer();

			}
		});
		return this;
	}
	public DatabaseConfigurationsBuilder removeDistantPeer(DecentralizedValue distantPeer)
	{
		return removeDistantPeers(Collections.singleton(distantPeer));
	}
	public DatabaseConfigurationsBuilder removeDistantPeers(Collection<DecentralizedValue> distantPeers)
	{
		return removeDistantPeers(true, distantPeers);
	}
	DatabaseConfigurationsBuilder removeDistantPeers(boolean propagate, Collection<DecentralizedValue> distantPeers)
	{
		if (protectedSignatureProfileProviderForAuthenticatedP2PMessages ==null)
			throw new IllegalArgumentException("Cannot set local peer without protected encryption profile provider for authenticated P2P messages");
		pushQuery((t) -> {
			HashSet<DecentralizedValue> dps=new HashSet<>(distantPeers);
			if (configurations.removeDistantPeers(dps))
			{

				if (propagate)
					t.addIDsToRemove(dps);

				t.checkDisconnections();
				t.checkConnexionsToDesynchronize();
				t.updateConfigurationPersistence();
				t.checkInitLocalPeer();
			}
		});
		return this;
	}

	public DatabaseConfigurationsBuilder setDistantPeersWithGivenAdditionalPackages(String packageString, Collection<DecentralizedValue> distantPeers)
	{
		if (packageString==null)
			throw new NullPointerException();
		if (distantPeers==null)
			throw new NullPointerException();
		if (configurations.getLocalPeer()!=null && distantPeers.contains(configurations.getLocalPeer()))
			throw new IllegalArgumentException();
		if (protectedSignatureProfileProviderForAuthenticatedP2PMessages ==null)
			throw new IllegalArgumentException("Cannot set local peer without protected encryption profile provider for authenticated P2P messages");
		pushQuery((t) -> {
			t.checkNotRemovedIDs(distantPeers);
			boolean changed=configurations.setDistantPeersWithGivenPackage(packageString, distantPeers);
			if (changed) {
				t.updateConfigurationPersistence();
				t.checkNewConnexions();
				t.checkConnexionsToDesynchronize();
				t.checkDatabaseToSynchronize();
				t.checkPeersToAdd();
			}
		});
		return this;
	}

	public DatabaseLifeCycles getLifeCycles() {
		return lifeCycles;
	}

	public DatabaseConfigurationsBuilder resetSynchronizerAndRemoveAllHosts()
	{
		pushQuery((t) -> {
			wrapper.getSynchronizer().resetSynchronizerAndRemoveAllHosts();
			configurations.removeDistantPeers(configurations.getDistantPeers());
			configurations.setLocalPeer(null);
			t.checkDatabaseLoading(null);
			t.checkNewConnexions();
			t.checkDatabaseToSynchronize();
		});
		return this;
	}

	public DatabaseConfigurationsBuilder setCentralDatabaseBackupCertificate(CentralDatabaseBackupCertificate centralDatabaseBackupCertificate)
	{
		pushQuery((t) -> {
			if (centralDatabaseBackupCertificate==null && configurations.getDatabaseConfigurations().stream().anyMatch(DatabaseConfiguration::isSynchronizedWithCentralBackupDatabase))
				throw new NullPointerException("You cannot set a null certificate when using a least one database configuration that is synchronized with central database backup");
			configurations.setCentralDatabaseBackupCertificate(centralDatabaseBackupCertificate);
			if (wrapper.getSynchronizer().isInitialized())
				wrapper.getSynchronizer().disconnectAllHooksFromThereBackups();
			currentTransaction.checkInitCentralDatabaseBackup();

		});
		return this;
	}
	public DatabaseConfigurationsBuilder restoreGivenDatabasesToOldVersion(Set<Package> concernedDatabases, long timeUTCInMs)
	{
		return restoreGivenDatabasesToOldVersion(concernedDatabases, timeUTCInMs, false, false);
	}
	public DatabaseConfigurationsBuilder restoreGivenDatabasesToOldVersion(Set<Package> concernedDatabases, long timeUTCInMs, boolean preferOtherChannelThanLocalChannelIfAvailable, boolean chooseNearestBackupIfNoBackupMatch)
	{
		if (concernedDatabases==null)
			throw new NullPointerException();
		return restoreDatabaseToOldVersion(timeUTCInMs, preferOtherChannelThanLocalChannelIfAvailable, chooseNearestBackupIfNoBackupMatch, dc -> concernedDatabases.contains(dc.getDatabaseSchema().getPackage()));
	}
	public DatabaseConfigurationsBuilder restoreGivenDatabaseStringToOldVersion(String concernedDatabase, long timeUTCInMs)
	{
		return restoreGivenDatabaseStringToOldVersion(concernedDatabase, timeUTCInMs, false, false);
	}
	public DatabaseConfigurationsBuilder restoreGivenDatabaseStringToOldVersion(String concernedDatabase, long timeUTCInMs, boolean preferOtherChannelThanLocalChannelIfAvailable, boolean chooseNearestBackupIfNoBackupMatch)
	{
		if (concernedDatabase==null)
			throw new NullPointerException();
		return restoreDatabaseToOldVersion(timeUTCInMs, preferOtherChannelThanLocalChannelIfAvailable, chooseNearestBackupIfNoBackupMatch, dc -> concernedDatabase.equals(dc.getDatabaseSchema().getPackage().getName()), true);
	}
	public DatabaseConfigurationsBuilder restoreGivenDatabaseToOldVersion(Package concernedDatabase, long timeUTCInMs)
	{
		return restoreGivenDatabaseToOldVersion(concernedDatabase, timeUTCInMs, false, false);
	}
	public DatabaseConfigurationsBuilder restoreGivenDatabaseToOldVersion(Package concernedDatabase, long timeUTCInMs, boolean preferOtherChannelThanLocalChannelIfAvailable, boolean chooseNearestBackupIfNoBackupMatch)
	{
		if (concernedDatabase==null)
			throw new NullPointerException();
		return restoreGivenDatabaseStringToOldVersion(concernedDatabase.getName(), timeUTCInMs, preferOtherChannelThanLocalChannelIfAvailable, chooseNearestBackupIfNoBackupMatch);
	}
	public DatabaseConfigurationsBuilder restoreGivenDatabasesStringToOldVersion(Set<String> concernedDatabases, long timeUTCInMs)
	{
		return restoreGivenDatabasesStringToOldVersion(concernedDatabases, timeUTCInMs, false, false);
	}
	public DatabaseConfigurationsBuilder restoreGivenDatabasesStringToOldVersion(Set<String> concernedDatabases, long timeUTCInMs, boolean preferOtherChannelThanLocalChannelIfAvailable, boolean chooseNearestBackupIfNoBackupMatch)
	{
		if (concernedDatabases==null)
			throw new NullPointerException();
		return restoreDatabaseToOldVersion(timeUTCInMs, preferOtherChannelThanLocalChannelIfAvailable, chooseNearestBackupIfNoBackupMatch, dc -> concernedDatabases.contains(dc.getDatabaseSchema().getPackage().getName()));
	}
	private DatabaseConfigurationsBuilder restoreDatabaseToOldVersion(long timeUTCInMs, boolean preferOtherChannelThanLocalChannelIfAvailable, boolean chooseNearestBackupIfNoBackupMatch, Predicate<DatabaseConfiguration> predicate)
	{
		return restoreDatabaseToOldVersion(timeUTCInMs, preferOtherChannelThanLocalChannelIfAvailable, chooseNearestBackupIfNoBackupMatch, predicate, true);
	}
	private DatabaseConfigurationsBuilder restoreDatabaseToOldVersion(long timeUTCInMs, boolean preferOtherChannelThanLocalChannelIfAvailable, boolean chooseNearestBackupIfNoBackupMatch, Predicate<DatabaseConfiguration> predicate, @SuppressWarnings("SameParameterValue") final boolean notifyOtherPeers)
	{
		pushQuery((p) -> {
			boolean changed=false;
			for (DatabaseConfiguration c : configurations.getDatabaseConfigurations()) {
				if (predicate.test(c)) {
					if (c.restoreDatabaseToOldVersion(timeUTCInMs, preferOtherChannelThanLocalChannelIfAvailable || c.getBackupConfiguration()==null, chooseNearestBackupIfNoBackupMatch, notifyOtherPeers)) {

						boolean localRestoration=true;
						if (preferOtherChannelThanLocalChannelIfAvailable || c.getBackupConfiguration() == null) {
							localRestoration=false;
							if (c.isSynchronizedWithCentralBackupDatabase()) {
								//download database from other database's channel
								wrapper.prepareDatabaseRestorationFromCentralDatabaseBackupChannel(c.getDatabaseSchema().getPackage());
								changed = true;

							} else {
								if (wrapper.getDatabaseHooksTable().hasRecords(new Filter<>() {
									@Override
									public boolean nextRecord(DatabaseHooksTable.Record _record) {
										return !_record.concernsLocalDatabaseHost() && _record.getDatabasePackageNames().contains(c.getDatabaseSchema().getPackage().getName())
												&& !_record.getDatabasePackageNamesThatDoNotUseExternalBackup().contains(c.getDatabaseSchema().getPackage().getName());
									}
								}))
								{
									//send message to distant peer in order to ask him to restore database to old state
									wrapper.checkNotAskForDatabaseBackupPartDestinedToCentralDatabaseBackup(c.getDatabaseSchema().getPackage());
									changed = true;
								}
								else if (c.getBackupConfiguration()==null)
									throw new DatabaseException("No peer and no local backup was found to restore the database "+c.getDatabaseSchema().getPackage().getName());
							}
						}
						if (localRestoration){
							//restore database from local backup database
							if (!wrapper.prepareDatabaseRestorationFromInternalBackupChannel(c.getDatabaseSchema().getPackage()))
								changed = true;
						}
					}
				}
			}
			if (changed)
			{
				p.updateConfigurationPersistence();
			}
		});
		return this;
	}

	public boolean isRestorationToOldVersionInProgress(Package databasePackage)
	{
		return isRestorationToOldVersionInProgress(databasePackage.getName());
	}

	public boolean isRestorationToOldVersionInProgress(String databasePackage)
	{
		synchronized (this) {
			DatabaseConfiguration dc = getDatabaseConfiguration(databasePackage);
			if (dc == null)
				throw new IllegalArgumentException("The database " + databasePackage + " does not exists !");
			return dc.isRestorationToOldVersionInProgress();
		}
	}

	boolean applyRestorationIfNecessary(DatabaseWrapper.Database database) throws DatabaseException {

		synchronized (this)
		{
			wrapper.finalizer.lockWrite();
			try {
				for (DatabaseConfiguration c : configurations.getDatabaseConfigurations()) {
					if (c.getDatabaseSchema().getPackage().equals(database.getConfiguration().getDatabaseSchema().getPackage())) {
						Long timeUTCInMs = c.getTimeUTCInMsForRestoringDatabaseToOldVersion();
						Logger l=wrapper.getDatabaseLogger();
						if (l!=null)
							l.info("Try to apply restoration : timeUTCInMs="+timeUTCInMs+", isPreferOtherChannelThanLocalChannelIfAvailableDuringRestoration="+c.isPreferOtherChannelThanLocalChannelIfAvailableDuringRestoration()+", isSynchronizedWithCentralBackupDatabase="+c.isSynchronizedWithCentralBackupDatabase()+", isCurrentDatabaseInInitialSynchronizationProcessFromCentralDatabaseBackup="+database.isCurrentDatabaseInInitialSynchronizationProcessFromCentralDatabaseBackup());
						if (timeUTCInMs != null && !database.isCurrentDatabaseInInitialSynchronizationProcessFromCentralDatabaseBackup()) {


							if (c.isPreferOtherChannelThanLocalChannelIfAvailableDuringRestoration()) {
								if (c.isSynchronizedWithCentralBackupDatabase()) {
									database.temporaryBackupRestoreManagerComingFromDistantBackupManager.restoreDatabaseToDateUTC(timeUTCInMs, c.isChooseNearestBackupIfNoBackupMatch(), true);

									database.cancelCurrentDatabaseRestorationProcessFromCentralDatabaseBackup();
								}
								else {
									wrapper.runSynchronizedTransaction(new SynchronizedTransaction<>() {
										@Override
										public Object run() throws Exception {

											Reference<DecentralizedValue> hostThatApplyRestoration = new Reference<>(null);
											wrapper.getDatabaseHooksTable().getRecords(new Filter<>() {
												@Override
												public boolean nextRecord(DatabaseHooksTable.Record _record) {
													if (_record.getDatabasePackageNames().contains(c.getDatabaseSchema().getPackage().getName())
															&& !_record.getDatabasePackageNamesThatDoNotUseExternalBackup().contains(c.getDatabaseSchema().getPackage().getName())) {
														hostThatApplyRestoration.set(_record.getHostID());
														stopTableParsing();
													}
													return false;
												}
											}, "concernsDatabaseHost=%c", "c", false);
											if (hostThatApplyRestoration.get() != null)
												wrapper.getSynchronizer().notifyOtherPeersThatDatabaseRestorationWasDone(c.getDatabaseSchema().getPackage(), timeUTCInMs, System.currentTimeMillis(), hostThatApplyRestoration.get(), c.isChooseNearestBackupIfNoBackupMatch());

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

							}
							else
							{
								BackupRestoreManager b = database.backupRestoreManager;
								assert b != null;
								b.restoreDatabaseToDateUTC(timeUTCInMs, c.isChooseNearestBackupIfNoBackupMatch(), c.isNotifyOtherPeers());
								wrapper.restorationFromInternalDatabaseBackupFinished(c.getDatabaseSchema().getPackage());
							}
							c.disableDatabaseRestorationToOldVersion();
							if (lifeCycles != null)
								lifeCycles.saveDatabaseConfigurations(configurations);

						}
						else if (database.isEmpty() && database.isCurrentDatabaseInInitialSynchronizationProcessFromCentralDatabaseBackup())
						{

							if (database.backupRestoreManager.getLastTransactionID()>-1) {
								database.backupRestoreManager.restoreDatabaseToLastKnownBackupFromEmptyDatabase(database.getSynchronizationPlanMessageComingFromCentralDatabaseBackup(), timeUTCInMs);
								database.terminateCurrentDatabaseInitialSynchronizationProcessFromCentralDatabaseBackup();
								if (timeUTCInMs!=null)
									c.disableDatabaseRestorationToOldVersion();

								return true;
							}
						}

						break;
					}
				}
				return false;
			}
			finally {
				wrapper.finalizer.unlockWrite();
			}
		}
	}

}
