package com.distrimind.ood.database.messages;
/*
Copyright or © or Corp. Jason Mahdjoub (01/04/2013)

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
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.SecuredObjectInputStream;
import com.distrimind.util.io.SecuredObjectOutputStream;
import com.distrimind.util.io.SerializationTools;

import java.io.IOException;
import java.util.Set;

/**
 * Message that contains compatible databases between two peers
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 3.0.0
 */
public class CompatibleDatabasesP2PMessage extends AbstractCompatibleDatabasesMessage implements P2PDatabaseEventToSend {

	private DecentralizedValue hostDestination;

	public CompatibleDatabasesP2PMessage(Set<String> compatibleDatabases, Set<String> compatibleDatabasesWithDestinationPeer, DecentralizedValue hostSource, DecentralizedValue hostDestination, Set<String> databasesThatUseBackupRestoreManager) {
		super(compatibleDatabases, compatibleDatabasesWithDestinationPeer, hostSource, databasesThatUseBackupRestoreManager);
		if (hostDestination==null)
			throw new NullPointerException();
		this.hostDestination = hostDestination;
	}

	@SuppressWarnings("unused")
	private CompatibleDatabasesP2PMessage() {
		super();
	}

	@Override
	public boolean cannotBeMerged() {
		return false;
	}

	@Override
	public DecentralizedValue getHostDestination() throws DatabaseException {
		return hostDestination;
	}



	@Override
	public int getInternalSerializedSize() {
		return super.getInternalSerializedSize()
				+ SerializationTools.getInternalSize(hostDestination);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		super.writeExternal(out);

		out.writeObject(hostDestination, false );
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		super.readExternal(in);

		hostDestination=in.readObject(false);
	}

	@Override
	public MergeState mergeWithP2PDatabaseEventToSend(DatabaseEvent newEvent) {
		try {
			if (newEvent instanceof CompatibleDatabasesP2PMessage && ((CompatibleDatabasesP2PMessage) newEvent).getHostDestination().equals(getHostDestination()))
				return MergeState.DELETE_OLD;
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
		return MergeState.NO_FUSION;
	}

	@Override
	public String toString() {
		return "CompatibleDatabasesP2PMessage{" +
				"hostSource=" + DatabaseWrapper.toString(getHostSource()) +
				", hostDestination=" + DatabaseWrapper.toString(hostDestination) +
				", compatibleDatabases=" + getCompatibleDatabases() +
				"} ";
	}
}
