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

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.properties.MultiFormatProperties;

import javax.security.auth.login.Configuration;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class DatabaseConfigurations extends MultiFormatProperties {
	private HashSet<DatabaseConfiguration> configurations;
	private HashSet<DecentralizedValue> distantPeers;
	private DecentralizedValue localPeer;
	private transient DatabaseWrapper wrapper=null;
	private transient DatabaseLifeCycles lifeCycles=null;
	public DatabaseConfigurations(Set<DatabaseConfiguration> configurations) {
		this(configurations,null, null);
	}
	public DatabaseConfigurations() {
		this(new HashSet<>());
	}
	public DatabaseConfigurations(Set<DatabaseConfiguration> configurations, Set<DecentralizedValue> distantPeers, DecentralizedValue localPeer) {
		super(null);
		if (configurations ==null)
			throw new NullPointerException();
		if (configurations.contains(null))
			throw new NullPointerException();

		this.configurations = new HashSet<>(configurations);

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
		checkLocalPeerNull();

	}
	private void checkLocalPeerNull()
	{
		if (localPeer==null) {
			for (DatabaseConfiguration dc : configurations) {
				if (dc.getDistantPeersThatCanBeSynchronizedWithThisDatabase().size() > 0)
					throw new NullPointerException("The local peer must be defined");
			}
		}
	}

	void setWrapper(DatabaseWrapper wrapper, DatabaseLifeCycles lifeCycles) throws DatabaseException {
		if (wrapper==null)
			throw new NullPointerException();
		if (this.wrapper!=null)
			throw new DatabaseException("Cannot use the same database configuration for several wrappers");
		this.wrapper = wrapper;
		this.lifeCycles=lifeCycles;
		boolean save=false;
		for (DatabaseConfiguration dc : configurations)
		{
			for (DecentralizedValue dv : dc.getDistantPeersThatCanBeSynchronizedWithThisDatabase())
			{
				if (this.distantPeers == null)
					this.distantPeers = new HashSet<>();

				if (this.distantPeers.add(dv))
					save=true;
			}
		}
		if (save && lifeCycles!=null)
		{
			lifeCycles.saveDatabaseConfigurationParameters(this);
		}
	}

	DatabaseLifeCycles getLifeCycles() {
		return lifeCycles;
	}



	Set<DatabaseConfiguration> getConfigurations() {
		return configurations;
	}

	Set<DecentralizedValue> getDistantPeers() {
		return distantPeers;
	}

	DecentralizedValue getLocalPeer() {
		return localPeer;
	}
}
