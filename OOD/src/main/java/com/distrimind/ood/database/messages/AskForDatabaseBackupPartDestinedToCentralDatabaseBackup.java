package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.DatabaseEvent;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.SecureExternalizable;
import com.distrimind.util.io.SecuredObjectInputStream;
import com.distrimind.util.io.SecuredObjectOutputStream;
import com.distrimind.util.io.SerializationTools;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 3.0.0
 */
public class AskForDatabaseBackupPartDestinedToCentralDatabaseBackup extends DatabaseEvent implements MessageDestinedToCentralDatabaseBackup, SecureExternalizable {

	private DecentralizedValue hostSource;
	private long lastFileTimestampUTCToNotInclude;

	@SuppressWarnings("unused")
	private AskForDatabaseBackupPartDestinedToCentralDatabaseBackup() {
	}

	public AskForDatabaseBackupPartDestinedToCentralDatabaseBackup(DecentralizedValue hostSource, long lastFileTimestampUTCToNotInclude) {
		if (hostSource==null)
			throw new NullPointerException();
		this.hostSource = hostSource;
		this.lastFileTimestampUTCToNotInclude = lastFileTimestampUTCToNotInclude;
	}

	@Override
	public DecentralizedValue getHostSource() {
		return hostSource;
	}

	public long getLastFileTimestampUTCToNotInclude() {
		return lastFileTimestampUTCToNotInclude;
	}

	@Override
	public int getInternalSerializedSize() {
		return 8+ SerializationTools.getInternalSize((SecureExternalizable)hostSource);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostSource, false);
		out.writeLong(lastFileTimestampUTCToNotInclude);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		hostSource=in.readObject(false, DecentralizedValue.class);
		lastFileTimestampUTCToNotInclude =in.readLong();
	}
}
