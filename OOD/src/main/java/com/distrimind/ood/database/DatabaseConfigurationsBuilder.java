package com.distrimind.ood.database;

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
		private boolean checkNewConnexions=false;
		private boolean checkConnexionsToRemove=false;
		private boolean checkDatabaseLoading=false;
		private List<DecentralizedValue> removedPeersID=null;

		void updateConfigurationPersistence() {
			updateConfigurationPersistence = true;
		}

		void checkNewConnexions() {
			checkNewConnexions=true;
		}

		void checkConnexionsToRemove() {
			checkConnexionsToRemove=true;
		}
		void addIDToRemove(DecentralizedValue dv)
		{
			if (dv==null)
				throw new NullPointerException();
			if (removedPeersID==null)
				removedPeersID=new ArrayList<>();
			else if (removedPeersID.contains(dv))
				return;

			removedPeersID.add(dv);
		}
  		void checkDatabaseLoading()
		{
			checkDatabaseLoading=true;
		}
		void checkConfigurationLoading() {
			checkDatabaseLoading();
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
				if (currentTransaction.checkDatabaseLoading)
					checkDatabaseLoading();
				if (currentTransaction.checkConnexionsToRemove)
					checkConnexionsToRemove();
				if (currentTransaction.checkNewConnexions)
					checkNewConnexions();
				if (currentTransaction.removedPeersID!=null)
				{

					for (DecentralizedValue peerIDToRemove : currentTransaction.removedPeersID) {
						wrapper.getDatabaseHooksTable().updateRecords(new AlterRecordFilter<DatabaseHooksTable.Record>() {
							@Override
							public void nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException {
								if (!_record.concernsLocalDatabaseHost()) {
									_record.offerNewAuthenticatedP2PMessage(wrapper, new HookRemoveRequest(configurations.getLocalPeer(), _record.getHostID(), peerIDToRemove), encryptionProfileProvider, this);

								}
							}
						});
					}
				}

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
			t.checkConfigurationLoading();
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
	private void checkConnexionsToRemove() throws DatabaseException {
		final ArrayList<DatabaseHooksTable.Record> deletedRecords=new ArrayList<>();

		wrapper.getDatabaseHooksTable().updateRecords(new AlterRecordFilter<DatabaseHooksTable.Record>() {
			@Override
			public void nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException {
				if (!_record.concernsLocalDatabaseHost()) {
					if (!configurations.getDistantPeers().contains(_record.getHostID()))
					{
						_record.setDatabasePackageNames(null, this);
						//TODO check if must change state
						_record.offerNewAuthenticatedP2PMessage(wrapper, new HookRemoveRequest(configurations.getLocalPeer(), _record.getHostID(), _record.getHostID()), encryptionProfileProvider, this);
					}
					deletedRecords.add(_record);
				}
			}
		});


		wrapper.getDatabaseHooksTable().updateRecords(new AlterRecordFilter<DatabaseHooksTable.Record>() {
			@Override
			public void nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException {
				if (!_record.concernsLocalDatabaseHost()) {
					if (!deletedRecords.contains(_record))
					{
						//TODO check if must change state
						for (DatabaseHooksTable.Record r : deletedRecords)
							_record.offerNewAuthenticatedP2PMessage(wrapper, new HookRemoveRequest(configurations.getLocalPeer(), _record.getHostID(), r.getHostID()),encryptionProfileProvider, this);
					}
				}
			}
		});

		for (DecentralizedValue peerIDToRemove : currentTransaction.removedPeersID) {
			wrapper.getDatabaseHooksTable().updateRecords(new AlterRecordFilter<DatabaseHooksTable.Record>() {
				@Override
				public void nextRecord(DatabaseHooksTable.Record _record) throws DatabaseException {
					if (!_record.concernsLocalDatabaseHost()) {
						_record.offerNewAuthenticatedP2PMessage(wrapper, new HookRemoveRequest(configurations.getLocalPeer(), _record.getHostID(), peerIDToRemove), encryptionProfileProvider, this);

					}
				}
			});
		}
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
		wrapper.loadDatabase(dls, lifeCycles);
		//TOTO revisit this part : take account of the restoration and time of restoration
	}

	public void setLocalPeerIdentifier(DecentralizedValue localPeerId, boolean permitIndirectSynchronizationBetweenPeers, boolean replace) {
		if (localPeerId==null)
			throw new NullPointerException();
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
				t.checkNewConnexions();
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
			t.checkNotRemovedIDs(distantPeers);
			boolean changed=false;
			for (String p : packagesString)
			{
				changed|=configurations.synchronizeAdditionalDistantPeersWithGivenPackage(p, distantPeers);
			}
			if (changed) {
				t.updateConfigurationPersistence();
				t.checkNewConnexions();
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
			t.checkNotRemovedIDs(distantPeers);
			boolean changed=configurations.setDistantPeersWithGivenPackage(packageString, distantPeers);
			if (changed) {
				t.updateConfigurationPersistence();
				t.checkNewConnexions();
				t.checkConnexionsToRemove();
			}
		});
		return this;
	}


}
