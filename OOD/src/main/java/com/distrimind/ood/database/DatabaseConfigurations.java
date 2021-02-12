/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

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

import com.distrimind.ood.database.centraldatabaseapi.CentralDatabaseBackupCertificate;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.properties.MultiFormatProperties;
import com.distrimind.util.properties.PropertiesParseException;
import org.w3c.dom.Document;

import java.io.IOException;
import java.io.Reader;
import java.util.*;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
@SuppressWarnings("FieldMayBeFinal")
public class DatabaseConfigurations extends MultiFormatProperties {
	private transient final Set<DatabaseConfiguration> volatileConfigurations=new HashSet<>();
	private transient final Set<DatabaseConfiguration> allConfigurations=new HashSet<>();
	private transient final Set<DatabaseConfiguration> allConfigurationsReadOnly= Collections.unmodifiableSet(allConfigurations);
	private transient final Set<DecentralizedValue> volatileDistantPeers=new HashSet<>();
	private transient final Set<DecentralizedValue> allDistantPeers=new HashSet<>();
	private transient final Set<DecentralizedValue> allDistantPeersReadOnly=Collections.unmodifiableSet(allDistantPeers);
	private Set<DatabaseConfiguration> configurations;

	private Set<DecentralizedValue> distantPeers;
	private DecentralizedValue localPeer;
	private boolean permitIndirectSynchronizationBetweenPeers;
	CentralDatabaseBackupCertificate centralDatabaseBackupCertificate=null;



	public DatabaseConfigurations(Set<DatabaseConfiguration> configurations) throws DatabaseException {
		this(configurations,null, null, false);
	}
	public DatabaseConfigurations() throws DatabaseException {
		this(new HashSet<>());
	}

	public CentralDatabaseBackupCertificate getCentralDatabaseBackupCertificate() {
		return centralDatabaseBackupCertificate;
	}

	void setCentralDatabaseBackupCertificate(CentralDatabaseBackupCertificate centralDatabaseBackupCertificate) {
		this.centralDatabaseBackupCertificate = centralDatabaseBackupCertificate;
	}

	public DatabaseConfigurations(Set<DatabaseConfiguration> configurations, Set<DecentralizedValue> distantPeers, DecentralizedValue localPeer, boolean permitIndirectSynchronizationBetweenPeers) throws DatabaseException {
		super(null);
		if (configurations ==null)
			throw new NullPointerException();
		if (configurations.contains(null))
			throw new NullPointerException();

		this.configurations = new HashSet<>(configurations);
		this.permitIndirectSynchronizationBetweenPeers=permitIndirectSynchronizationBetweenPeers;
		if (distantPeers==null)
			this.distantPeers=new HashSet<>();
		else {
			if (distantPeers.contains(null))
				throw new NullPointerException();
			if (localPeer==null)
				throw new NullPointerException();
			if (distantPeers.contains(localPeer))
				throw new IllegalArgumentException("The local peer "+localPeer+ " can't be contained into the list of distant peers");
			this.distantPeers = new HashSet<>(distantPeers);
		}
		this.localPeer = localPeer;
		checkConfigurations();
		checkDistantPeers();

	}


	boolean checkDistantPeers() throws DatabaseException {
		boolean save=false;
		for (DatabaseConfiguration dc : configurations)
		{
			if (dc.getDistantPeersThatCanBeSynchronizedWithThisDatabase()!=null) {
				save|=distantPeers.addAll(dc.getDistantPeersThatCanBeSynchronizedWithThisDatabase());
				allDistantPeers.addAll(dc.getDistantPeersThatCanBeSynchronizedWithThisDatabase());
			}
		}
		for (DatabaseConfiguration dc : volatileConfigurations)
		{
			if (dc.getDistantPeersThatCanBeSynchronizedWithThisDatabase()!=null) {
				save|=volatileDistantPeers.addAll(dc.getDistantPeersThatCanBeSynchronizedWithThisDatabase());
				allDistantPeers.addAll(dc.getDistantPeersThatCanBeSynchronizedWithThisDatabase());
			}
		}

		checkForMaxDistantPeersReached();
		checkLocalPeerNull();

		return save;
	}

	private void checkForMaxDistantPeersReached() throws DatabaseException {
		if (allDistantPeers.size()>DatabaseWrapper.MAX_DISTANT_PEERS)
			throw new DatabaseException("The number of distant peers "+allDistantPeers.size()+" has reached the maximum number of peers "+DatabaseWrapper.MAX_DISTANT_PEERS);
	}

	public boolean isPermitIndirectSynchronizationBetweenPeers() {
		return permitIndirectSynchronizationBetweenPeers;
	}

	void setPermitIndirectSynchronizationBetweenPeers(boolean permitIndirectSynchronizationBetweenPeers) {
		this.permitIndirectSynchronizationBetweenPeers = permitIndirectSynchronizationBetweenPeers;
	}

	void setLocalPeer(DecentralizedValue localPeer) {
		for (DatabaseConfiguration dc : configurations)
			if (dc.getDistantPeersThatCanBeSynchronizedWithThisDatabase()!=null && dc.getDistantPeersThatCanBeSynchronizedWithThisDatabase().contains(localPeer))
				throw new IllegalArgumentException();
		this.localPeer = localPeer;
	}

	private void checkLocalPeerNull() throws DatabaseException {
		checkLocalPeerNull(allDistantPeers);
	}
	private void checkLocalPeerNull(Collection<DecentralizedValue> allDistantPeers) throws DatabaseException {
		if (localPeer==null) {
			boolean e=allDistantPeers.size()>0;
			if (!e)
			{
				for (DatabaseConfiguration dc : configurations)
				{
					if (dc.getSynchronizationType()== DatabaseConfiguration.SynchronizationType.DECENTRALIZED_SYNCHRONIZATION_AND_SYNCHRONIZATION_WITH_CENTRAL_BACKUP_DATABASE) {
						e = true;
						break;
					}
				}
			}
			if (e)
				throw new DatabaseException("Local peer must be defined !");
		}
		else
		{
			if (allDistantPeers.contains(localPeer))
				throw new DatabaseException("Local peer cannot be a distant peer");
		}
	}
	private void checkConfigurations() throws DatabaseException {
		try {
			for (DatabaseConfiguration dc : configurations)
			{
				checkConfiguration(dc);
			}
		}
		catch (NullPointerException | IllegalArgumentException e)
		{
			throw DatabaseException.getDatabaseException(e);
		}

	}
	private void checkConfiguration(DatabaseConfiguration configuration)  {
		if (configuration==null)
			throw new NullPointerException();
		if (configuration.getDatabaseSchema().getPackage().equals(this.getClass().getPackage()))
			throw new IllegalArgumentException("The package "+this.getClass().getPackage()+" cannot be included into the list of database");

		if (localPeer!=null)
		{
			Collection<DecentralizedValue> c=configuration.getDistantPeersThatCanBeSynchronizedWithThisDatabase();
			if (c!=null && c.contains(localPeer))
				throw new IllegalArgumentException("Impossible to synchronize the database between one peer and it self : "+localPeer);
		}
	}

	void addConfiguration(DatabaseConfiguration configuration, boolean makeConfigurationLoadingPersistent ) throws DatabaseException {
		checkConfiguration(configuration);
		if (makeConfigurationLoadingPersistent) {
			configurations.add(configuration);
			if (configuration.getDistantPeersThatCanBeSynchronizedWithThisDatabase()!=null) {
				distantPeers.addAll(configuration.getDistantPeersThatCanBeSynchronizedWithThisDatabase());
			}
		}
		else {
			volatileConfigurations.add(configuration);
			if (configuration.getDistantPeersThatCanBeSynchronizedWithThisDatabase()!=null) {
				volatileDistantPeers.addAll(configuration.getDistantPeersThatCanBeSynchronizedWithThisDatabase());
			}
		}
		if (configuration.getDistantPeersThatCanBeSynchronizedWithThisDatabase()!=null) {
			allDistantPeers.addAll(configuration.getDistantPeersThatCanBeSynchronizedWithThisDatabase());
			checkForMaxDistantPeersReached();
		}
		allConfigurations.add(configuration);
	}
	boolean removeConfiguration(String databasePackage) {
		if (databasePackage==null)
			throw new NullPointerException();
		boolean changed=volatileConfigurations.removeIf(c -> c.getDatabaseSchema().getPackage().getName().equals(databasePackage));
		changed|=configurations.removeIf(c -> c.getDatabaseSchema().getPackage().getName().equals(databasePackage));
		changed|=allConfigurations.removeIf(c -> c.getDatabaseSchema().getPackage().getName().equals(databasePackage));
		return changed;
	}

	@Override
	public void loadXML(Document document) throws PropertiesParseException {
		super.loadXML(document);
		try {
			reloadAllConfigurations();
		} catch (DatabaseException e) {
			throw new IllegalArgumentException(e);
		}
	}

	private void reloadAllConfigurations() throws DatabaseException {
		checkConfigurations();
		allConfigurations.clear();
		allConfigurations.addAll(volatileConfigurations);
		allConfigurations.addAll(configurations);
		allDistantPeers.clear();
		checkDistantPeers();

	}


	@Override
	public void loadFromProperties(Properties properties) throws IllegalArgumentException {
		super.loadFromProperties(properties);
		try {
			reloadAllConfigurations();
		} catch (DatabaseException e) {
			throw new IllegalArgumentException(e);
		}
	}


	@Override
	public void loadYAML(Reader reader) throws IOException {
		super.loadYAML(reader);
		try {
			reloadAllConfigurations();
		} catch (DatabaseException e) {
			throw new IllegalArgumentException(e);
		}
	}



	Set<DatabaseConfiguration> getConfigurations() {
		return allConfigurationsReadOnly;
	}

	Set<DecentralizedValue> getDistantPeers() {
		return allDistantPeersReadOnly;
	}

	boolean addDistantPeer(DecentralizedValue distantPeer, boolean volatilePeer) throws DatabaseException {
		if (distantPeer==null)
			throw new NullPointerException();
		checkLocalPeerNull(Collections.singleton(distantPeer));
		boolean changed;
		if (volatilePeer)
			changed = this.volatileDistantPeers.add(distantPeer);
		else
			changed = this.distantPeers.add(distantPeer);
		allDistantPeers.add(distantPeer);
		checkForMaxDistantPeersReached();
		return changed;
	}
	boolean synchronizeAdditionalDistantPeersWithGivenPackage(String packageString, Collection<DecentralizedValue> distantPeers) throws DatabaseException {
		return synchronizeAdditionalDistantPeersWithGivenPackage(packageString, distantPeers, false);
	}

	private boolean synchronizeAdditionalDistantPeersWithGivenPackage(String packageString, Collection<DecentralizedValue> distantPeers, boolean replaceExistant) throws DatabaseException {
		boolean volatileConf=true;
		DatabaseConfiguration foundDC=getVolatileDatabaseConfiguration(packageString);
		if (foundDC==null)
		{
			foundDC=getPersistentDatabaseConfiguration(packageString);
			if (foundDC==null)
				throw new DatabaseException("Database configuration not found for package "+packageString);
			volatileConf=false;
		}
		if (distantPeers==null)
			throw new NullPointerException();
		checkLocalPeerNull(distantPeers);
		boolean changed;
		try {
			if (replaceExistant)
				changed = foundDC.setDistantPeersThatCanBeSynchronizedWithThisDatabase(distantPeers);
			else
				changed = foundDC.addDistantPeersThatCanBeSynchronizedWithThisDatabase(distantPeers);
		} catch (IllegalAccessException e) {
			throw DatabaseException.getDatabaseException(e);
		}
		if (volatileConf) {
			volatileDistantPeers.clear();
			for (DatabaseConfiguration dc : volatileConfigurations)
			{
				Set<DecentralizedValue> s=dc.getDistantPeersThatCanBeSynchronizedWithThisDatabase();
				if (s!=null)
					volatileDistantPeers.addAll(s);
			}
		}
		else {
			changed |= this.distantPeers.addAll(distantPeers);
		}
		allDistantPeers.addAll(distantPeers);
		checkForMaxDistantPeersReached();
		return changed;
	}
	boolean setDistantPeersWithGivenPackage(String packageString, Collection<DecentralizedValue> distantPeers) throws DatabaseException {
		return synchronizeAdditionalDistantPeersWithGivenPackage(packageString, distantPeers, true);
	}
	boolean desynchronizeAdditionalDistantPeersWithGivenPackage(String packageString, Collection<DecentralizedValue> distantPeers) throws DatabaseException {
		DatabaseConfiguration foundDC=getVolatileDatabaseConfiguration(packageString);
		if (foundDC==null)
		{
			foundDC=getPersistentDatabaseConfiguration(packageString);
			if (foundDC==null)
				throw new DatabaseException("Database configuration not found for package "+packageString);
		}
		if (distantPeers==null)
			throw new NullPointerException();
		boolean changed;


		try {
			if (localPeer!=null && distantPeers.contains(localPeer))
			{
				changed = foundDC.removeDistantPeersThatCanBeSynchronizedWithThisDatabase(foundDC.getDistantPeersThatCanBeSynchronizedWithThisDatabase());
				foundDC.setSynchronizationType(DatabaseConfiguration.SynchronizationType.NO_SYNCHRONIZATION);
			}
			else {
				checkLocalPeerNull(distantPeers);
				changed = foundDC.removeDistantPeersThatCanBeSynchronizedWithThisDatabase(distantPeers);
			}
		} catch (IllegalAccessException e) {
			throw DatabaseException.getDatabaseException(e);
		}
		return changed;
	}

	boolean removeDistantPeers(Collection<DecentralizedValue> distantPeers) throws DatabaseException {

		boolean changed=false;
		try {
			boolean allNoSync=false;
			if (distantPeers.contains(localPeer)) {
				distantPeers.clear();
				distantPeers.addAll(allDistantPeers);
				allNoSync=true;
			}
			for (DatabaseConfiguration dc : volatileConfigurations)
			{
				changed|=dc.removeDistantPeersThatCanBeSynchronizedWithThisDatabase(distantPeers);
			}
			for (DatabaseConfiguration dc : configurations)
			{
				changed|=dc.removeDistantPeersThatCanBeSynchronizedWithThisDatabase(distantPeers);
			}
			changed|=this.distantPeers.removeAll(distantPeers);
			changed|=volatileDistantPeers.removeAll(distantPeers);
			changed|=allDistantPeers.removeAll(distantPeers);
			if (allNoSync)
				for (DatabaseConfiguration c : allConfigurations)
					c.setSynchronizationType(DatabaseConfiguration.SynchronizationType.NO_SYNCHRONIZATION);
			return changed;
		} catch (IllegalAccessException e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}


	public DatabaseConfiguration getDatabaseConfiguration(Package packageString)
	{
		return getDatabaseConfiguration(packageString.getName());
	}
	public DatabaseConfiguration getDatabaseConfiguration(String packageString)
	{
		for (DatabaseConfiguration dc : getConfigurations())
		{
			if (dc.getDatabaseSchema().getPackage().getName().equals(packageString))
				return dc;
		}
		return null;
	}
	public DatabaseConfiguration getVolatileDatabaseConfiguration(Package packageString)
	{
		return getDatabaseConfiguration(packageString.getName());
	}
	public DatabaseConfiguration getVolatileDatabaseConfiguration(String packageString)
	{
		for (DatabaseConfiguration dc : volatileConfigurations)
		{
			if (dc.getDatabaseSchema().getPackage().getName().equals(packageString))
				return dc;
		}
		return null;
	}
	public DatabaseConfiguration getPersistentDatabaseConfiguration(Package packageString)
	{
		return getDatabaseConfiguration(packageString.getName());
	}
	public DatabaseConfiguration getPersistentDatabaseConfiguration(String packageString)
	{
		for (DatabaseConfiguration dc : configurations)
		{
			if (dc.getDatabaseSchema().getPackage().getName().equals(packageString))
				return dc;
		}
		return null;
	}
	DecentralizedValue getLocalPeer() {
		return localPeer;
	}

	void setCreateDatabasesIfNecessaryAndCheckIt(boolean createDatabasesIfNecessaryAndCheckIt) {
		for (DatabaseConfiguration dc : configurations)
			dc.setCreateDatabaseIfNecessaryAndCheckItDuringCurrentSession(createDatabasesIfNecessaryAndCheckIt);
	}
	public boolean isDecentralized()
	{
		return localPeer!=null || configurations.stream().anyMatch(DatabaseConfiguration::isDecentralized);
	}
	public boolean useCentralBackupDatabase()
	{
		return configurations.stream().anyMatch(DatabaseConfiguration::isSynchronizedWithCentralBackupDatabase);
	}


}
