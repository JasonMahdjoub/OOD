package com.distrimind.ood.database;

import com.distrimind.ood.database.centraldatabaseapi.CentralDatabaseBackupCertificate;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.EncryptionProfileProvider;

import java.io.IOException;
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
	private boolean commitInProgress=false;

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
			if (commitInProgress)
				throw new IllegalAccessError();
			commitInProgress=true;
			wrapper.lockWrite();
			try {
				for (ConfigurationQuery q : currentTransaction.queries)
					q.execute(currentTransaction);
				if (currentTransaction.updateConfigurationPersistence && lifeCycles!=null)
					lifeCycles.saveDatabaseConfigurations(configurations);
				if (currentTransaction.checkDatabaseLoading)
					checkDatabaseLoading();
				if (currentTransaction.checkDatabaseToDesynchronize)
					currentTransaction.checkDatabaseUnload|=checkDatabaseToDesynchronize();
				if (currentTransaction.removedPeersID!=null)
					currentTransaction.checkDisconnexions|=checkConnexionsToRemove();
				if (currentTransaction.checkDatabaseUnload)
					checkDatabaseToUnload();
				if (currentTransaction.checkPeersToAdd) {
					boolean b=checkPeersToAdd();
					currentTransaction.checkDatabaseToSynchronize |=b;
				}
				if (currentTransaction.checkInitLocalPeer) {
					checkInitLocalPeer();
				}
				if (currentTransaction.checkDatabaseToSynchronize)
					checkDatabaseToSynchronize();
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
				commitInProgress=false;
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
			if (commitInProgress)
				throw new IllegalAccessError();
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

			configuration.setCreateDatabaseIfNecessaryAndCheckItDuringCurrentSession(createDatabaseIfNecessaryAndCheckItDuringCurrentSession);
			configurations.addConfiguration(configuration, makeConfigurationLoadingPersistent);
			if (makeConfigurationLoadingPersistent)
				t.updateConfigurationPersistence();
			t.checkDatabaseLoading();
			t.checkNewConnexions();
			t.checkDatabaseToSynchronize();
			t.checkPeersToAdd();

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
	private boolean checkPeersToAdd() throws DatabaseException {
		return wrapper.runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {
			@Override
			public Boolean run() throws Exception {
				Set<DecentralizedValue> peersID=configurations.getDistantPeers();
				boolean res=false;
				if (peersID!=null && peersID.size()>0)
				{
					for (DecentralizedValue dv : peersID)
					{
						List<DatabaseHooksTable.Record> l=wrapper.getDatabaseHooksTable().getRecords("concernsDatabaseHost=%cdh and hostID=%h", "cdh", false, "h", dv);
						if (l.size() == 0)
						{
							wrapper.getDatabaseHooksTable().initDistantHook(dv);
							res=true;
						}
					}
				}
				return res;
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
		return wrapper.runSynchronizedTransaction(new SynchronizedTransaction<Boolean>() {
			@Override
			public Boolean run() throws Exception {

				Set<DatabaseConfiguration> packagesToSynchronize=new HashSet<>();
				for (DatabaseConfiguration c : configurations.getConfigurations()) {
					if (c.isDecentralized()) {
						Set<DecentralizedValue> sps = c.getDistantPeersThatCanBeSynchronizedWithThisDatabase();
						wrapper.getDatabaseHooksTable().getRecords(new Filter<DatabaseHooksTable.Record>() {
							@Override
							public boolean nextRecord(DatabaseHooksTable.Record _record)  {

								if (currentTransaction.removedPeersID==null || !currentTransaction.removedPeersID.contains(_record.getHostID())) {

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
							hm.put(c.getDatabaseSchema().getPackage().getName(), DatabaseConfigurationsBuilder.this.lifeCycles != null && DatabaseConfigurationsBuilder.this.lifeCycles.replaceDistantConflictualRecordsWhenDistributedDatabaseIsResynchronized(c));
							try {
								_record.offerNewAuthenticatedP2PMessage(wrapper, new HookSynchronizeRequest(configurations.getLocalPeer(), _record.getHostID(), hm, c.getDistantPeersThatCanBeSynchronizedWithThisDatabase(), secureRandom, protectedEncryptionProfileProviderForAuthenticatedP2PMessages), protectedEncryptionProfileProviderForAuthenticatedP2PMessages, this);
							} catch (IOException e) {
								throw DatabaseException.getDatabaseException(e);
							}
						}
					}
				}, "concernsDatabaseHost=%cdh", "cdh", false);

				for (DatabaseConfiguration c : packagesToSynchronize)
				{
					Map<String, Boolean> hm=new HashMap<>();
					hm.put(c.getDatabaseSchema().getPackage().getName(), DatabaseConfigurationsBuilder.this.lifeCycles != null && DatabaseConfigurationsBuilder.this.lifeCycles.replaceDistantConflictualRecordsWhenDistributedDatabaseIsResynchronized(c));
					wrapper.getSynchronizer().receivedHookSynchronizeRequest(new HookSynchronizeRequest(configurations.getLocalPeer(), configurations.getLocalPeer(), hm, c.getDistantPeersThatCanBeSynchronizedWithThisDatabase(), secureRandom, protectedEncryptionProfileProviderForAuthenticatedP2PMessages));
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

						if (currentTransaction.removedPeersID==null || !currentTransaction.removedPeersID.contains(_record.getHostID()))
						{

							Set<String> packages=_record.getDatabasePackageNames();
							if (packages!=null && packages.size()>0) {
								Set<String> ptu=new HashSet<>();
								for (String p : packages) {
									Optional<DatabaseConfiguration> o = configurations.getConfigurations().stream().filter(c -> c.getDatabaseSchema().getPackage().getName().equals(p)).findAny();
									if (!o.isPresent() || !o.get().isDecentralized() || o.get().getDistantPeersThatCanBeSynchronizedWithThisDatabase() == null || o.get().getDistantPeersThatCanBeSynchronizedWithThisDatabase().contains(_record.getHostID())) {
										ptu.add(p);
									}
								}
								if (ptu.size() > 0) {
									Set<DecentralizedValue> dvs = packagesToUnsynchronize.computeIfAbsent(ptu, k -> new HashSet<>());
									dvs.add(_record.getHostID());
								}
							}
							//TODO check if must change state

						}
						return false;
					}
				}, "concernsDatabaseHost=%cdh", "cdh", false);

				if (currentTransaction.propagate)
				{
					wrapper.getDatabaseHooksTable().updateRecords(new AlterRecordFilter<DatabaseHooksTable.Record>() {
						@Override
						public void nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException {
							for (Map.Entry<Set<String>, Set<DecentralizedValue>> e : packagesToUnsynchronize.entrySet()) {
								try {
									_record.offerNewAuthenticatedP2PMessage(wrapper, new HookDesynchronizeRequest(configurations.getLocalPeer(), _record.getHostID(), e.getKey(), e.getValue(), secureRandom, protectedEncryptionProfileProviderForAuthenticatedP2PMessages), protectedEncryptionProfileProviderForAuthenticatedP2PMessages, this);
								} catch (IOException ioException) {
									throw DatabaseException.getDatabaseException(ioException);
								}
							}
						}
					}, "concernsDatabaseHost=%cdh", "cdh", false);

					for (Map.Entry<Set<String>, Set<DecentralizedValue>> e : packagesToUnsynchronize.entrySet()) {
						wrapper.getSynchronizer().receivedHookDesynchronizeRequest(new HookDesynchronizeRequest(configurations.getLocalPeer(), configurations.getLocalPeer(), e.getKey(), e.getValue(), secureRandom, protectedEncryptionProfileProviderForAuthenticatedP2PMessages));
					}
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
								try {
									_record.offerNewAuthenticatedP2PMessage(wrapper, new HookRemoveRequest(configurations.getLocalPeer(), _record.getHostID(), peerIDToRemove, secureRandom, protectedEncryptionProfileProviderForAuthenticatedP2PMessages), protectedEncryptionProfileProviderForAuthenticatedP2PMessages, this);
								} catch (IOException e) {
									throw DatabaseException.getDatabaseException(e);
								}
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


	private void checkDatabaseToUnload() throws DatabaseException {
		//TODO complete
	}

	private void checkDatabaseLoading() throws DatabaseException {
		ArrayList<DatabaseConfiguration> dls=new ArrayList<>();
		for (DatabaseConfiguration dc : configurations.getConfigurations())
		{
			if (!wrapper.isDatabaseLoaded(dc)) {
				dls.add(dc);
			}
		}
		if (!dls.isEmpty())
			wrapper.loadDatabase(dls, lifeCycles);
		//TOTO revisit this part : take account of the restoration and time of restoration
	}

	public DatabaseConfigurationsBuilder setLocalPeerIdentifier(DecentralizedValue localPeerId, boolean permitIndirectSynchronizationBetweenPeers, boolean replace) {
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
	DatabaseConfigurationsBuilder synchronizeDistantPeersWithGivenAdditionalPackages(boolean checkNewConnexion, Collection<DecentralizedValue> distantPeers, String ... packagesString)
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
		if (protectedEncryptionProfileProviderForAuthenticatedP2PMessages ==null)
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
				t.propagate=propagate;
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
		if (protectedEncryptionProfileProviderForAuthenticatedP2PMessages ==null)
			throw new IllegalArgumentException("Cannot set local peer without protected encryption profile provider for authenticated P2P messages");
		pushQuery((t) -> {
			HashSet<DecentralizedValue> dps=new HashSet<>(distantPeers);
			if (configurations.removeDistantPeers(dps))
			{

				if (propagate)
					t.addIDsToRemove(dps);

				t.checkDisconnexions();
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

	public void removeDatabaseConfiguration(String packageString, boolean removeData)
	{
		//TODO complete
	}

}
