package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.DatabaseEvent;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.SecureExternalizable;
import com.distrimind.util.io.SecuredObjectInputStream;
import com.distrimind.util.io.SecuredObjectOutputStream;
import com.distrimind.util.io.SerializationTools;

import java.io.IOException;

/**
 * Disconnection message from central database backup
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class DisconnectCentralDatabaseBackup extends DatabaseEvent implements MessageDestinedToCentralDatabaseBackup, SecureExternalizable {
	private DecentralizedValue hostSource;

	@SuppressWarnings("unused")
	private DisconnectCentralDatabaseBackup() {
	}

	public DisconnectCentralDatabaseBackup(DecentralizedValue hostSource) {
		if (hostSource==null)
			throw new NullPointerException();
		this.hostSource = hostSource;
	}

	@Override
	public DecentralizedValue getHostSource() {
		return hostSource;
	}

	@Override
	public boolean cannotBeMerged() {
		return true;
	}

	@Override
	public int getInternalSerializedSize() {
		return SerializationTools.getInternalSize(hostSource);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostSource, false);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		hostSource=in.readObject(false, DecentralizedValue.class);
	}

	@Override
	public String toString() {
		return "DisconnectCentralDatabaseBackup{" +
				"hostSource=" + DatabaseWrapper.toString(hostSource) +
				'}';
	}
}
