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
import com.distrimind.util.io.SecureExternalizable;
import com.distrimind.util.io.SecuredObjectInputStream;
import com.distrimind.util.io.SecuredObjectOutputStream;
import com.distrimind.util.io.SerializationTools;

import java.io.IOException;

/**
 * Initial connection message with other peer
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class P2PConnexionInitialization extends DatabaseEvent implements P2PDatabaseEventToSend, SecureExternalizable {

	private DecentralizedValue hostSource;
	private DecentralizedValue hostDestination;
	private long lastValidatedTransactionID;

	@SuppressWarnings("unused")
	private P2PConnexionInitialization()
	{

	}

	public P2PConnexionInitialization(DecentralizedValue hostSource, DecentralizedValue hostDestination, long lastValidatedTransactionID) {
		if (hostSource==null)
			throw new NullPointerException();
		if (hostDestination==null)
			throw new NullPointerException();
		if (hostSource.equals(hostDestination))
			throw new IllegalArgumentException();
		this.hostSource = hostSource;
		this.hostDestination = hostDestination;
		this.lastValidatedTransactionID = lastValidatedTransactionID;
	}

	public long getLastValidatedTransactionID() {
		return lastValidatedTransactionID;
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
	public DecentralizedValue getHostSource() throws DatabaseException {
		return hostSource;
	}

	@Override
	public int getInternalSerializedSize() {
		return SerializationTools.getInternalSize(hostDestination, 0)
				+SerializationTools.getInternalSize(hostSource, 0)
				+9;
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostSource, false );
		out.writeObject(hostDestination, false );
		out.writeLong(lastValidatedTransactionID);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		hostSource=in.readObject(false);
		hostDestination=in.readObject(false);
		lastValidatedTransactionID=in.readLong();
	}

	@Override
	public String toString() {
		return "P2PConnexionInitialization{" +
				"hostSource=" + DatabaseWrapper.toString(hostSource) +
				", hostDestination=" + DatabaseWrapper.toString(hostDestination) +
				", lastValidatedTransactionID=" + lastValidatedTransactionID +
				'}';
	}
}
