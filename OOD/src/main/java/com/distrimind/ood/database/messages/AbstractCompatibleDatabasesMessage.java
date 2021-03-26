package com.distrimind.ood.database.messages;
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

import com.distrimind.ood.database.DatabaseEvent;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.SecureExternalizable;
import com.distrimind.util.io.SecuredObjectInputStream;
import com.distrimind.util.io.SecuredObjectOutputStream;
import com.distrimind.util.io.SerializationTools;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 3.0.0
 */
public abstract class AbstractCompatibleDatabasesMessage extends DatabaseEvent implements SecureExternalizable {

	private static final int MAX_PACKAGES_NUMBERS= DatabaseWrapper.MAX_PACKAGE_TO_SYNCHRONIZE;
	public static final int MAX_SIZE_OF_PACKAGES_NAMES_IN_BYTES=MAX_PACKAGES_NUMBERS*SerializationTools.MAX_CLASS_LENGTH*2+4;


	private Set<String> incompatibleDatabasesWithDestinationPeer;
	private Set<String> compatibleDatabasesWithDestinationPeer;
	private DecentralizedValue hostSource;

	protected AbstractCompatibleDatabasesMessage(Set<String> compatibleDatabases, Set<String> compatibleDatabasesWithDestinationPeer, DecentralizedValue hostSource) {
		if (hostSource==null)
			throw new NullPointerException();
		if (compatibleDatabases ==null)
			compatibleDatabases =new HashSet<>();
		if (compatibleDatabases.contains(null))
			throw new NullPointerException();
		if (compatibleDatabasesWithDestinationPeer ==null)
			compatibleDatabasesWithDestinationPeer =new HashSet<>();
		if (compatibleDatabasesWithDestinationPeer.contains(null))
			throw new NullPointerException();
		this.incompatibleDatabasesWithDestinationPeer = new HashSet<>(compatibleDatabases);
		this.incompatibleDatabasesWithDestinationPeer.removeAll(compatibleDatabasesWithDestinationPeer);
		this.compatibleDatabasesWithDestinationPeer = compatibleDatabasesWithDestinationPeer;
		this.hostSource=hostSource;
	}

	protected AbstractCompatibleDatabasesMessage() {
	}

	@Override
	public int getInternalSerializedSize() {
		return SerializationTools.getInternalSize(incompatibleDatabasesWithDestinationPeer, MAX_SIZE_OF_PACKAGES_NAMES_IN_BYTES)
				+ SerializationTools.getInternalSize(hostSource);
	}

	public DecentralizedValue getHostSource() {
		return hostSource;
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeCollection(compatibleDatabasesWithDestinationPeer, false, MAX_SIZE_OF_PACKAGES_NAMES_IN_BYTES, false);
		out.writeCollection(incompatibleDatabasesWithDestinationPeer, false, MAX_SIZE_OF_PACKAGES_NAMES_IN_BYTES, false);
		out.writeObject(hostSource, false);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		compatibleDatabasesWithDestinationPeer =in.readCollection(false, MAX_SIZE_OF_PACKAGES_NAMES_IN_BYTES, false, String.class);
		incompatibleDatabasesWithDestinationPeer =in.readCollection(false, MAX_SIZE_OF_PACKAGES_NAMES_IN_BYTES, false, String.class);
		hostSource=in.readObject(false);
	}

	public Set<String> getCompatibleDatabases() {
		Set<String> res=new HashSet<>(compatibleDatabasesWithDestinationPeer);
		res.addAll(incompatibleDatabasesWithDestinationPeer);
		return res;
	}

	public Set<String> getCompatibleDatabasesWithDestinationPeer() {
		return compatibleDatabasesWithDestinationPeer;
	}
}
