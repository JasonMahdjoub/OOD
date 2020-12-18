package com.distrimind.ood.database;

import com.distrimind.ood.database.centraldatabaseapi.CentralDatabaseBackupCertificate;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.EncryptionProfileProvider;

import java.util.*;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 3.0.0
 */
public class DatabaseConfigurationsBuilder {
	private final DatabaseConfigurations configurations;
	private final DatabaseWrapper wrapper;
	private final DatabaseLifeCycles lifeCycles;
	private final EncryptionProfileProvider encryptionProfileProviderForCentralDatabaseBackup, protectedEncryptionProfileProviderForAuthenticatedP2PMessages;
	private final AbstractSecureRandom secureRandom;


	DatabaseConfigurationsBuilder(DatabaseConfigurations configurations,
								  DatabaseWrapper wrapper,
								  DatabaseLifeCycles lifeCycles,
								  EncryptionProfileProvider encryptionProfileProviderForCentralDatabaseBackup,
								  EncryptionProfileProvider protectedEncryptionProfileProviderForAuthenticatedP2PMessages,
								  AbstractSecureRandom secureRandom,
								  boolean createDatabasesIfNecessaryAndCheckIt) throws DatabaseException {
		if (configurations==null)
			configurations=new DatabaseConfigurations();
		if (wrapper==null)
			throw new NullPointerException();
		if (encryptionProfileProviderForCentralDatabaseBackup ==null && configurations.useCentralBackupDatabase())
			throw new NullPointerException();
		if (protectedEncryptionProfileProviderForAuthenticatedP2PMessages ==null && configurations.isDecentralized())
			throw new NullPointerException();
		if (secureRandom==null && (protectedEncryptionProfileProviderForAuthenticatedP2PMessages!=null || encryptionProfileProviderForCentralDatabaseBackup!=null))
			throw new NullPointerException();

		this.configurations = configurations;
		this.wrapper = wrapper;
		this.lifeCycles = lifeCycles;
		this.encryptionProfileProviderForCentralDatabaseBackup = encryptionProfileProviderForCentralDatabaseBackup;
		this.protectedEncryptionProfileProviderForAuthenticatedP2PMessages = protectedEncryptionProfileProviderForAuthenticatedP2PMessages;
		this.secureRandom=secureRandom;
		boolean save=configurations.checkDistantPeers();
		configurations.setCreateDatabasesIfNecessaryAndCheckIt(createDatabasesIfNecessaryAndCheckIt);

		if (save && lifeCycles!=null)
		{
			lifeCycles.saveDatabaseConfigurations(configurations);
		}

		checkInitLocalPeer();
	}


	private static class Transaction {
		final ArrayList<ConfigurationQuery> queries =new ArrayList<>();
		private boolean checkDatabaseToSynchronize=false;
		private boolean checkDatabaseUnload=false;
		private boolean checkDisconnexions=false;
		private boolean updateConfigurationPersistence=false;
		private boolean checkNewConnexions=false;
		private boolean checkDatabaseToDesynchronize =false;
		private boolean checkDatabaseLoading=false;
		private boolean checkInitCentralDatabaseBackup=false;
		private Set<DecentralizedValue> removedPeersID=null;

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
  		void checkDatabaseLoading()
		{
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
		void checkDisconnexions()
		{
			checkDisconnexions=true;
		}
		void checkDatabaseUnload()
		{
			checkDatabaseUnload=true;
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

	DatabaseConfigurations getConfigurations() {
		return configurations;
	}

	private void pushQuery(ConfigurationQuery query) {
		synchronized (this) {
			if (currentTransaction==null)
				currentTransaction=new Transaction();
			currentTransaction.queries.add(query);
		}
	}


	public void commit() throws DatabaseException {
		synchronized (this) {
			if (currentTransaction==null)
				throw new DatabaseException("No query was added ! Nothing to commit !");
			wrapper.lockWrite();
			try {
				for (ConfigurationQuery q : currentTransaction.queries)
					q.execute(currentTransaction);
				if (currentTransaction.updateConfigurationPersistence)
					lifeCycles.saveDatabaseConfigurations(configurations);
				if (currentTransaction.checkDatabaseLoading)
					checkDatabaseLoading();
				if (currentTransaction.checkDatabaseToDesynchronize)
					currentTransaction.checkDatabaseUnload|=checkDatabaseToDesynchronize();
				if (currentTransaction.removedPeersID!=null)
					currentTransaction.checkDisconnexions|=checkConnexionsToRemove();
				if (currentTransaction.checkDatabaseUnload)
					checkDatabaseLoading();
				if (currentTransaction.checkDatabaseToSynchronize)
					currentTransaction.checkNewConnexions|=checkDatabaseToSynchronize();
				if (currentTransaction.checkNewConnexions) {
					checkConnexions();
				}
				if (currentTransaction.checkDisconnexions) {
					checkDisconnections();
				}
				if (currentTransaction.checkInitCentralDatabaseBackup)
					checkInitCentralDatabaseBackup();


			}
			finally {
				currentTransaction = null;
				wrapper.unlockWrite();
			}
		}
	}

	private void checkInitCentralDatabaseBackup() throws DatabaseException {
		if (wrapper.getSynchronizer().centralBackupAvailable)
			wrapper.getSynchronizer().centralDatabaseBackupAvailable();
	}

	public void rollBack()
	{
		synchronized (this)
		{
			currentTransaction=null;
		}
	}


	public EncryptionProfileProvider getEncryptionProfileProviderForCentralDatabaseBackup() {
		return encryptionProfileProviderForCentralDatabaseBackup;
	}

	public EncryptionProfileProvider getProtectedEncryptionProfileProviderForAuthenticatedP2PMessages() {
		return protectedEncryptionProfileProviderForAuthenticatedP2PMessages;
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
		if (configuration.isSynchronizedWithCentralBackupDatabase() && encryptionProfileProviderForCentralDatabaseBackup==null)
			throw new IllegalArgumentException("Cannot add configuration without encryption profile provider for central backup database");
		if (configuration.isDecentralized() && protectedEncryptionProfileProviderForAuthenticatedP2PMessages ==null)
			throw new IllegalArgumentException("Cannot add configuration without protected encryption profile provider for authenticated P2P messages");
		pushQuery((t) -> {
			for (DatabaseConfiguration dc : this.configurations.getConfigurations())
			{
				if (dc.getDatabaseSchema().getPackage().equals(configuration.getDatabaseSchema().getPackage()))
					throw new IllegalArgumentException();
			}
			configuration.setCreateDatabaseIfNecessaryAndCheckItDuringCurrentSession(createDatabaseIfNecessaryAndCheckItDuringCurrentSession);
			configurations.addConfiguration(configuration, makeConfigurationLoadingPersistent);
			if (makeConfigurationLoadingPersistent)
				t.updateConfigurationPersistence();
			t.checkDatabaseLoading();

		});
		return this;
	}
	public DatabaseConfigurationsBuilder removeConfiguration(DatabaseConfiguration databaseConfiguration)
	{
		return removeConfiguration(databaseConfiguration.getDatabaseSchema().getPackage());
	}
	public DatabaseConfigurationsBuilder removeConfiguration(Package databasePackage)
	{
		return removeConfiguration(databasePackage.getName());
	}
	public DatabaseConfigurationsBuilder removeConfiguration(String databasePackage)
	{
		if (databasePackage==null)
			throw new NullPointerException();

		pushQuery((t) -> {
			if (configurations.removeConfiguration(databasePackage))
			{
				t.updateConfigurationPersistence();
				t.checkDatabaseUnload();
				t.checkDisconnexions();
				t.checkConnexionsToDesynchronize();
			}

		});
		return this;
	}


	private void checkInitLocalPeer() throws DatabaseException {

		wrapper.getSynchronizer().checkInitLocalPeer();
	}

	private void checkConnexions() throws DatabaseException {
		//checkInitLocalPeer();
		wrapper.getSynchronizer().checkConnexionsToInit();
	}
	private void checkDisconnections() throws DatabaseException {
		wrapper.getSynchronizer().checkDisconnections();
	}
	private boolean checkDatabaseToSynchronize() throws DatabaseException {
		return wrapper.runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {
			@Override
			public Boolean run() throws Exception {
				Set<DatabaseConfiguration> packagesToSynchronize=new HashSet<>();
				for (DatabaseConfiguration c : configurations.getConfigurations()) {
					if (c.isDecentralized()) {

						wrapper.getDatabaseHooksTable().getRecords(new Filter<DatabaseHooksTable.Record>() {
							@Override
							public boolean nextRecord(DatabaseHooksTable.Record _record)  {

								if (!currentTransaction.removedPeersID.contains(_record.getHostID())) {
									Set<DecentralizedValue> sps = c.getDistantPeersThatCanBeSynchronizedWithThisDatabase();
									if (sps != null && sps.contains(_record.getHostID()) &&
											!_record.isConcernedByDatabasePackage(c.getDatabaseSchema().getPackage().getName())) {
										packagesToSynchronize.add(c);
										stopTableParsing();
									}

								}
								return false;
							}
						}, "concernsDatabaseHost=%cdh", "cdh", false);
					}

					//TODO check if must change state
				}

				wrapper.getDatabaseHooksTable().updateRecords(new AlterRecordFilter<DatabaseHooksTable.Record>() {
					@Override
					public void nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException {
						for (DatabaseConfiguration c : packagesToSynchronize)
						{
							Map<String, Boolean> hm=new HashMap<>();
							hm.put(c.getDatabaseSchema().getPackage().getName(), DatabaseConfigurationsBuilder.this.lifeCycles.replaceDistantConflictualRecordsWhenDistributedDatabaseIsResynchronized(c));

							_record.offerNewAuthenticatedP2PMessage(wrapper, new HookSynchronizeRequest(configurations.getLocalPeer(), _record.getHostID(), hm, c.getDistantPeersThatCanBeSynchronizedWithThisDatabase()), protectedEncryptionProfileProviderForAuthenticatedP2PMessages, this);
						}
					}
				}, "concernsDatabaseHost=%cdh", "cdh", false);
				for (DatabaseConfiguration c : packagesToSynchronize)
				{
					Map<String, Boolean> hm=new HashMap<>();
					hm.put(c.getDatabaseSchema().getPackage().getName(), DatabaseConfigurationsBuilder.this.lifeCycles.replaceDistantConflictualRecordsWhenDistributedDatabaseIsResynchronized(c));
					wrapper.getSynchronizer().receivedHookSynchronizeRequest(new HookSynchronizeRequest(configurations.getLocalPeer(), configurations.getLocalPeer(), hm, c.getDistantPeersThatCanBeSynchronizedWithThisDatabase()));
				}
				return packagesToSynchronize.size()>0;
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

		return wrapper.runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {
			@Override
			public Boolean run() throws Exception {
				HashMap<Set<String>, Set<DecentralizedValue>> packagesToUnsynchronize=new HashMap<>();
				wrapper.getDatabaseHooksTable().getRecords(new Filter<DatabaseHooksTable.Record>() {
					@Override
					public boolean nextRecord(DatabaseHooksTable.Record _record) {

						if (!currentTransaction.removedPeersID.contains(_record.getHostID()))
						{
							Set<String> ptu=new HashSet<>();
							for (String p : _record.getDatabasePackageNames())
							{
								Optional<DatabaseConfiguration> o=configurations.getConfigurations().stream().filter(c -> c.getDatabaseSchema().getPackage().getName().equals(p)).findAny();
								if (!o.isPresent() || !o.get().isDecentralized() || o.get().getDistantPeersThatCanBeSynchronizedWithThisDatabase()==null || o.get().getDistantPeersThatCanBeSynchronizedWithThisDatabase().contains(_record.getHostID()))
								{
									ptu.add(p);
								}
							}
							if (ptu.size()>0) {
								Set<DecentralizedValue> dvs = packagesToUnsynchronize.computeIfAbsent(ptu, k -> new HashSet<>());
								dvs.add(_record.getHostID());
							}
							//TODO check if must change state

						}
						return false;
					}
				}, "concernsDatabaseHost=%cdh", "cdh", false);


				wrapper.getDatabaseHooksTable().updateRecords(new AlterRecordFilter<DatabaseHooksTable.Record>() {
					@Override
					public void nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException {
						for (Map.Entry<Set<String>, Set<DecentralizedValue>> e : packagesToUnsynchronize.entrySet())
						{
							_record.offerNewAuthenticatedP2PMessage(wrapper, new HookUnsynchronizeRequest(configurations.getLocalPeer(), _record.getHostID(), e.getKey(), e.getValue()), protectedEncryptionProfileProviderForAuthenticatedP2PMessages, this);
						}
					}
				}, "concernsDatabaseHost=%cdh", "cdh", false);
				for (Map.Entry<Set<String>, Set<DecentralizedValue>> e : packagesToUnsynchronize.entrySet()) {
					wrapper.getSynchronizer().receivedHookUnsynchronizeRequest(new HookUnsynchronizeRequest(configurations.getLocalPeer(), configurations.getLocalPeer(), e.getKey(), e.getValue()));
				}
				return packagesToUnsynchronize.size()>0;
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
		return wrapper.runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {
			@Override
			public Boolean run() throws Exception {
				if (currentTransaction.removedPeersID!=null) {
					for (DecentralizedValue peerIDToRemove : currentTransaction.removedPeersID) {
						wrapper.getDatabaseHooksTable().updateRecords(new AlterRecordFilter<DatabaseHooksTable.Record>() {
							@Override
							public void nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException {
								_record.offerNewAuthenticatedP2PMessage(wrapper, new HookRemoveRequest(configurations.getLocalPeer(), _record.getHostID(), peerIDToRemove), protectedEncryptionProfileProviderForAuthenticatedP2PMessages, this);
							}
						}, "concernsDatabaseHost=%cdh", "cdh", false);
					}
					return currentTransaction.removedPeersID.size()>0;
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




	private void checkDatabaseLoading() throws DatabaseException {
		ArrayList<DatabaseConfiguration> dls=new ArrayList<>();
		for (DatabaseConfiguration dc : configurations.getConfigurations())
		{
			if (!wrapper.isDatabaseLoaded(dc)) {
				dls.add(dc);
			}
		}
		wrapper.loadDatabase(dls, lifeCycles);
		//TOTO revisit this part : take account of the restoration and time of restoration
	}

	public void setLocalPeerIdentifier(DecentralizedValue localPeerId, boolean permitIndirectSynchronizationBetweenPeers, boolean replace) {
		if (localPeerId==null)
			throw new NullPointerException();
		if (protectedEncryptionProfileProviderForAuthenticatedP2PMessages ==null)
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
				t.checkDisconnexions();
				t.checkNewConnexions();
				t.checkDatabaseToSynchronize();
				if (removedHostID!=null)
					t.addIDToRemove(removedHostID);

			}

		});

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
		if (packagesString==null)
			throw new NullPointerException();
		if (packagesString.length==0)
			throw new IllegalArgumentException();
		if (Arrays.stream(packagesString).anyMatch(Objects::isNull))
			throw new NullPointerException();
		if (protectedEncryptionProfileProviderForAuthenticatedP2PMessages ==null)
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
				t.checkDatabaseToSynchronize();
				t.checkNewConnexions();

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
		if (packagesString==null)
			throw new NullPointerException();
		if (packagesString.length==0)
			throw new IllegalArgumentException();
		if (Arrays.stream(packagesString).anyMatch(Objects::isNull))
			throw new NullPointerException();
		if (protectedEncryptionProfileProviderForAuthenticatedP2PMessages ==null)
			throw new IllegalArgumentException("Cannot set local peer without protected encryption profile provider for authenticated P2P messages");
		pushQuery((t) -> {
			t.checkNotRemovedIDs(distantPeers);
			boolean changed=false;
			for (String p : packagesString)
			{
				changed|=configurations.desynchronizeAdditionalDistantPeersWithGivenPackage(p, distantPeers);
			}
			if (changed) {
				t.checkConnexionsToDesynchronize();
				t.updateConfigurationPersistence();

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
		if (protectedEncryptionProfileProviderForAuthenticatedP2PMessages ==null)
			throw new IllegalArgumentException("Cannot set local peer without protected encryption profile provider for authenticated P2P messages");
		pushQuery((t) -> {
			if (configurations.removeDistantPeers(distantPeers))
			{
				t.addIDsToRemove(distantPeers);
				t.checkDisconnexions();
				t.checkConnexionsToDesynchronize();
				t.updateConfigurationPersistence();
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
		if (protectedEncryptionProfileProviderForAuthenticatedP2PMessages ==null)
			throw new IllegalArgumentException("Cannot set local peer without protected encryption profile provider for authenticated P2P messages");
		pushQuery((t) -> {
			t.checkNotRemovedIDs(distantPeers);
			boolean changed=configurations.setDistantPeersWithGivenPackage(packageString, distantPeers);
			if (changed) {
				t.updateConfigurationPersistence();
				t.checkNewConnexions();
				t.checkConnexionsToDesynchronize();
				t.checkDatabaseToSynchronize();
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
			t.checkDatabaseLoading();
			t.checkNewConnexions();
			t.checkDatabaseToSynchronize();
		});
		return this;
	}

	public DatabaseConfigurationsBuilder setCentralDatabaseBackupCertificate(CentralDatabaseBackupCertificate centralDatabaseBackupCertificate)
	{
		pushQuery((t) -> {
			configurations.centralDatabaseBackupCertificate=centralDatabaseBackupCertificate;
			wrapper.getSynchronizer().disconnectAllHooksFromThereBackups();
			currentTransaction.checkInitCentralDatabaseBackup();

		});
		return this;
	}


}
