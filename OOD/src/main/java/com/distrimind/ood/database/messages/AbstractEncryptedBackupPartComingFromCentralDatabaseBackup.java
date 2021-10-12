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

import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.EncryptedDatabaseBackupMetaDataPerFile;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.SecuredObjectInputStream;
import com.distrimind.util.io.SecuredObjectOutputStream;
import com.distrimind.util.io.SerializationTools;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 3.1.0
 */
public abstract class AbstractEncryptedBackupPartComingFromCentralDatabaseBackup extends AbstractEncryptedBackupPart implements MessageComingFromCentralDatabaseBackup{
	private DecentralizedValue channelHost;
	private DecentralizedValue hostDestination;
	public AbstractEncryptedBackupPartComingFromCentralDatabaseBackup(DecentralizedValue hostSource, DecentralizedValue hostDestination, EncryptedDatabaseBackupMetaDataPerFile metaData, RandomInputStream partInputStream, DecentralizedValue channelHost) {
		super(hostSource, metaData, partInputStream);
		if (channelHost==null)
			throw new NullPointerException();
		if (hostDestination==null)
			throw new NullPointerException();
		if (hostDestination!=hostSource && hostDestination.equals(hostSource))
			this.hostDestination=hostSource;
		else
			this.hostDestination=hostDestination;
		this.channelHost=channelHost;
	}
	@Override
	public DecentralizedValue getHostDestination() {
		return hostDestination;
	}
	protected AbstractEncryptedBackupPartComingFromCentralDatabaseBackup() {
		super();
	}

	public DecentralizedValue getChannelHost() {
		return channelHost;
	}

	@Override
	public int getInternalSerializedSize() {
		return super.getInternalSerializedSize()+ SerializationTools.getInternalSize(hostDestination)+SerializationTools.getInternalSize(channelHost);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		super.writeExternal(out);
		assert hostDestination!=null;
		out.writeObject(hostDestination==getHostSource()?null:hostDestination, true);
		out.writeObject(channelHost, false);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		hostDestination=in.readObject(true, DecentralizedValue.class);
		if (hostDestination==null)
			hostDestination=getHostSource();
		channelHost=in.readObject(false);
	}
	@Override
	public boolean cannotBeMerged() {
		return true;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName()+"{" +
				"hostSource=" + DatabaseWrapper.toString(getHostSource()) +
				", hostDestination=" + DatabaseWrapper.toString(hostDestination) +
				", channelHost=" + DatabaseWrapper.toString(channelHost) +
				", metaData=" + getMetaData() +
				'}';
	}
}
