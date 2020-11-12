package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.Reference;
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
	private final EncryptionProfileProvider encryptionProfileProvider;
	private final AbstractSecureRandom secureRandom;


	DatabaseConfigurationsBuilder(DatabaseConfigurations configurations,
								  DatabaseWrapper wrapper,
								  DatabaseLifeCycles lifeCycles,
								  EncryptionProfileProvider encryptionProfileProvider,
								  AbstractSecureRandom secureRandom,
								  boolean createDatabasesIfNecessaryAndCheckIt) throws DatabaseException {
		if (configurations==null)
			throw new NullPointerException();
		if (wrapper==null)
			throw new NullPointerException();
		if (encryptionProfileProvider==null)
			throw new NullPointerException();
		if (secureRandom==null)
			throw new NullPointerException();

		this.configurations = configurations;
		this.wrapper = wrapper;
		this.lifeCycles = lifeCycles;
		this.encryptionProfileProvider=encryptionProfileProvider;
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
		private boolean updateConfigurationPersistence=false;

		void updateConfigurationPersistence() {
			updateConfigurationPersistence = true;
		}

	}
	private interface ConfigurationQuery
	{
		void execute(Transaction transaction) throws DatabaseException;
	}

	private Transaction currentTransaction=null;

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

			}
			finally {
				currentTransaction = null;
				wrapper.unlockWrite();
			}
		}
	}
	public void rollBack()
	{
		synchronized (this)
		{
			currentTransaction=null;
		}
	}


	public EncryptionProfileProvider getEncryptionProfileProvider() {
		return encryptionProfileProvider;
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
			checkConfigurationLoading();
		});
		return this;
	}

	private void checkInitLocalPeer() throws DatabaseException {

		if (configurations.getLocalPeer() != null) {
			if (!wrapper.getSynchronizer().isInitialized(configurations.getLocalPeer()))
				wrapper.getSynchronizer().initLocalHostID(configurations.getLocalPeer(), configurations.isPermitIndirectSynchronizationBetweenPeers());
		}
	}

	private void checkNewConnexions() throws DatabaseException {
		checkInitLocalPeer();
		//TODO complete
	}

	private void checkConfigurationLoading() throws DatabaseException {
		checkDatabaseLoading();
		checkNewConnexions();
	}

	private void checkDatabaseLoading() throws DatabaseException {
		for (DatabaseConfiguration dc : configurations.getConfigurations())
		{
			if (!wrapper.isDatabaseLoaded(dc)) {
				wrapper.loadDatabase(dc, dc.isCreateDatabaseIfNecessaryAndCheckItDuringCurrentSession());
				//TOTO revisit this part : take account of the restoration and time of restoration
			}
		}
	}

	public void setLocalPeerIdentifier(DecentralizedValue localPeerId, boolean permitIndirectSynchronizationBetweenPeers, boolean replace) {
		if (localPeerId==null)
			throw new NullPointerException();
		pushQuery((t)-> {
			if (configurations.getLocalPeer() != null) {
				if (configurations.getLocalPeer().equals(localPeerId)) {
					if (configurations.isPermitIndirectSynchronizationBetweenPeers() != permitIndirectSynchronizationBetweenPeers) {
						if (replace) {
							wrapper.getSynchronizer().disconnectAll();
							configurations.setPermitIndirectSynchronizationBetweenPeers(permitIndirectSynchronizationBetweenPeers);
							t.updateConfigurationPersistence();
							checkNewConnexions();
						} else
							throw new DatabaseException("Local peer identifier is already configured !");
					}
				} else {
					final Reference<DecentralizedValue> removedHostID=new Reference<>();
					if (replace) {
						if (wrapper.getSynchronizer().isInitialized(configurations.getLocalPeer())) {
							wrapper.getSynchronizer().disconnectAll();
							removedHostID.set(wrapper.getSynchronizer().getLocalHostID());
							wrapper.getSynchronizer().removeHook(removedHostID.get());

						}
					} else
						throw new DatabaseException("Local peer identifier is already configured !");
					configurations.setPermitIndirectSynchronizationBetweenPeers(permitIndirectSynchronizationBetweenPeers);
					configurations.setLocalPeer(localPeerId);
					t.updateConfigurationPersistence();
					checkNewConnexions();
					if (removedHostID.get()!=null) {
						wrapper.getDatabaseHooksTable().updateRecords(new AlterRecordFilter<DatabaseHooksTable.Record>() {
							@Override
							public void nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException {
								if (!_record.concernsLocalDatabaseHost()) {
									_record.offerNewAuthenticatedP2PMessage(new HookRemoveRequest(localPeerId, _record.getHostID(), removedHostID.get(), encryptionProfileProvider), this);

								}
							}
						});
					}
				}

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
		return synchronizeDistantPeerWithGivenAdditionalPackages(Collections.singletonList(distantPeer), packagesString);
	}
	public DatabaseConfigurationsBuilder synchronizeDistantPeerWithGivenAdditionalPackages(Collection<DecentralizedValue> distantPeers, String ... packagesString)
	{
		if (packagesString==null)
			throw new NullPointerException();
		if (packagesString.length==0)
			throw new IllegalArgumentException();
		if (Arrays.stream(packagesString).anyMatch(Objects::isNull))
			throw new NullPointerException();
		pushQuery((t) -> {
			boolean changed=false;
			for (String p : packagesString)
			{
				changed|=configurations.synchronizeAdditionalDistantPeersWithGivenPackage(p, distantPeers);
			}
			if (changed) {
				t.updateConfigurationPersistence();
				checkNewConnexions();
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
		pushQuery((t) -> {
			boolean changed=configurations.setDistantPeersWithGivenPackage(packageString, distantPeers);
			if (changed) {
				t.updateConfigurationPersistence();
				checkNewConnexions();
			}
		});
		return this;
	}


}
