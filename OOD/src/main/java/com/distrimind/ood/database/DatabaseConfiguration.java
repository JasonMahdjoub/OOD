
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

import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.properties.MultiFormatProperties;
import com.distrimind.util.properties.PropertiesParseException;
import org.w3c.dom.Document;

import java.io.IOException;
import java.io.Reader;
import java.util.*;

/**
 * Describe a database configuration. A database is defined by its package, and
 * its classes. All classes must be stored into the given package. Others are
 * not taken into account.
 * 
 * @author Jason Mahdjoub
 * @version 2.0.0
 * @since OOD 2.0
 * @see DatabaseLifeCycles
 */
@SuppressWarnings("FieldMayBeFinal")
public class DatabaseConfiguration extends MultiFormatProperties {
	public enum SynchronizationType
	{
		NO_SYNCHRONIZATION,
		DECENTRALIZED_SYNCHRONIZATION,
		DECENTRALIZED_SYNCHRONIZATION_AND_SYNCHRONIZATION_WITH_CENTRAL_BACKUP_DATABASE
	}

	private DatabaseSchema databaseSchema;
	private SynchronizationType synchronizationType;
	private Set<DecentralizedValue> distantPeersThatCanBeSynchronizedWithThisDatabase;
	private BackupConfiguration backupConfiguration;
	private transient boolean createDatabaseIfNecessaryAndCheckItDuringCurrentSession=true;
	private boolean createDatabaseIfNecessaryAndCheckItDuringLoading;

	public DatabaseConfiguration(DatabaseSchema databaseSchema) {
		this(databaseSchema, SynchronizationType.NO_SYNCHRONIZATION, null, null);
	}

	boolean isCreateDatabaseIfNecessaryAndCheckItDuringCurrentSession() {
		return createDatabaseIfNecessaryAndCheckItDuringCurrentSession;
	}

	void setCreateDatabaseIfNecessaryAndCheckItDuringCurrentSession(boolean createDatabaseIfNecessaryAndCheckItDuringCurrentSession) {
		this.createDatabaseIfNecessaryAndCheckItDuringCurrentSession = createDatabaseIfNecessaryAndCheckItDuringCurrentSession;
	}

	public DatabaseConfiguration(DatabaseSchema databaseSchema, BackupConfiguration backupConfiguration) {
		this(databaseSchema, SynchronizationType.NO_SYNCHRONIZATION, null, backupConfiguration);
	}
	public DatabaseConfiguration(DatabaseSchema databaseSchema, SynchronizationType synchronizationType, Collection<DecentralizedValue> distantPeersThatCanBeSynchronizedWithThisDatabase, BackupConfiguration backupConfiguration) {
		this(databaseSchema, SynchronizationType.NO_SYNCHRONIZATION, distantPeersThatCanBeSynchronizedWithThisDatabase, backupConfiguration, true);
	}

	public DatabaseConfiguration(DatabaseSchema databaseSchema, SynchronizationType synchronizationType, Collection<DecentralizedValue> distantPeersThatCanBeSynchronizedWithThisDatabase, BackupConfiguration backupConfiguration, boolean createDatabaseIfNecessaryAndCheckItDuringLoading) {
		super(null);
		if (databaseSchema == null)
			throw new NullPointerException("_package");
		if (synchronizationType == null)
			throw new NullPointerException();
		if (synchronizationType==SynchronizationType.DECENTRALIZED_SYNCHRONIZATION_AND_SYNCHRONIZATION_WITH_CENTRAL_BACKUP_DATABASE && backupConfiguration==null)
			throw new NullPointerException("Backup configuration cannot be null when "+synchronizationType+" is activated");
		this.databaseSchema = databaseSchema;
		this.synchronizationType=synchronizationType;
		this.backupConfiguration=backupConfiguration;
		try {
			setDistantPeersThatCanBeSynchronizedWithThisDatabase(distantPeersThatCanBeSynchronizedWithThisDatabase);
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException();
		}

	}
	public BackupConfiguration getBackupConfiguration() {
		return backupConfiguration;
	}

	public boolean isCreateDatabaseIfNecessaryAndCheckItDuringLoading() {
		return createDatabaseIfNecessaryAndCheckItDuringLoading;
	}

	public void setCreateDatabaseIfNecessaryAndCheckItDuringLoading(boolean createDatabaseIfNecessaryAndCheckItDuringLoading) {
		this.createDatabaseIfNecessaryAndCheckItDuringLoading = createDatabaseIfNecessaryAndCheckItDuringLoading;
	}

	public Set<DecentralizedValue> getDistantPeersThatCanBeSynchronizedWithThisDatabase() {
		return distantPeersThatCanBeSynchronizedWithThisDatabase;
	}

	boolean setDistantPeersThatCanBeSynchronizedWithThisDatabase(Collection<DecentralizedValue> distantPeersThatCanBeSynchronizedWithThisDatabase) throws IllegalAccessException {
		if (distantPeersThatCanBeSynchronizedWithThisDatabase!=null && distantPeersThatCanBeSynchronizedWithThisDatabase.size()>0) {
			if (distantPeersThatCanBeSynchronizedWithThisDatabase.stream().anyMatch(Objects::isNull))
				throw new NullPointerException();
			if (synchronizationType==SynchronizationType.NO_SYNCHRONIZATION)
				throw new IllegalAccessException();
			if (this.distantPeersThatCanBeSynchronizedWithThisDatabase!=null && this.distantPeersThatCanBeSynchronizedWithThisDatabase.equals(distantPeersThatCanBeSynchronizedWithThisDatabase))
				return false;
			this.distantPeersThatCanBeSynchronizedWithThisDatabase = new HashSet<>(distantPeersThatCanBeSynchronizedWithThisDatabase);
		}
		else {
			if (this.distantPeersThatCanBeSynchronizedWithThisDatabase==null)
				return false;
			this.distantPeersThatCanBeSynchronizedWithThisDatabase = null;
		}
		return true;
	}
	boolean addDistantPeersThatCanBeSynchronizedWithThisDatabase(Collection<DecentralizedValue> distantPeersThatCanBeSynchronizedWithThisDatabase) throws IllegalAccessException {
		if (distantPeersThatCanBeSynchronizedWithThisDatabase==null)
			throw new NullPointerException();

		if (synchronizationType==SynchronizationType.NO_SYNCHRONIZATION)
			throw new IllegalAccessException();
		if (this.distantPeersThatCanBeSynchronizedWithThisDatabase==null)
			this.distantPeersThatCanBeSynchronizedWithThisDatabase=new HashSet<>();
		return this.distantPeersThatCanBeSynchronizedWithThisDatabase.addAll(distantPeersThatCanBeSynchronizedWithThisDatabase);

	}


	public DatabaseSchema getDatabaseSchema() {
		return databaseSchema;
	}




	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;
		if (o == this)
			return true;
		if (o instanceof DatabaseConfiguration) {
			DatabaseConfiguration dt = (DatabaseConfiguration) o;
			return dt.databaseSchema.equals(databaseSchema);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return databaseSchema.hashCode();
	}

	public boolean isSynchronizedWithCentralBackupDatabase() {
		return backupConfiguration!=null && synchronizationType==SynchronizationType.DECENTRALIZED_SYNCHRONIZATION_AND_SYNCHRONIZATION_WITH_CENTRAL_BACKUP_DATABASE;
	}

	public boolean isDecentralized()
	{
		return synchronizationType==SynchronizationType.DECENTRALIZED_SYNCHRONIZATION ||
				synchronizationType==SynchronizationType.DECENTRALIZED_SYNCHRONIZATION_AND_SYNCHRONIZATION_WITH_CENTRAL_BACKUP_DATABASE;
	}

	public SynchronizationType getSynchronizationType() {
		return synchronizationType;
	}


	@Override
	public void loadXML(Document document) throws PropertiesParseException {
		super.loadXML(document);
		reloadedFromDocument();
	}

	private void reloadedFromDocument()
	{
		createDatabaseIfNecessaryAndCheckItDuringCurrentSession=createDatabaseIfNecessaryAndCheckItDuringLoading;
	}


	@Override
	public void loadFromProperties(Properties properties) throws IllegalArgumentException {
		super.loadFromProperties(properties);
		reloadedFromDocument();
	}


	@Override
	public void loadYAML(Reader reader) throws IOException {
		super.loadYAML(reader);
		reloadedFromDocument();
	}
}
