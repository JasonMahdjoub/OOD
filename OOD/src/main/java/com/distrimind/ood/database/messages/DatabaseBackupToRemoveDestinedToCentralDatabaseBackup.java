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
 * @since OOD 3.0.0
 */
public class DatabaseBackupToRemoveDestinedToCentralDatabaseBackup extends DatabaseEvent implements MessageDestinedToCentralDatabaseBackup, SecureExternalizable
{
	private String packageName;
	private DecentralizedValue hostSource;


	@SuppressWarnings("unused")
	private DatabaseBackupToRemoveDestinedToCentralDatabaseBackup() {
	}

	public DatabaseBackupToRemoveDestinedToCentralDatabaseBackup(DecentralizedValue hostSource, String packageName) {
		this.packageName = packageName;
		this.hostSource=hostSource;
	}

	public String getPackageName() {
		return packageName;
	}


	@Override
	public DecentralizedValue getHostSource() {
		return hostSource;
	}

	@Override
	public int getInternalSerializedSize() {
		return SerializationTools.getInternalSize(packageName, SerializationTools.MAX_CLASS_LENGTH)+SerializationTools.getInternalSize((SecureExternalizable)hostSource);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeString(packageName, false, SerializationTools.MAX_CLASS_LENGTH);
		out.writeObject(hostSource, false);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		packageName=in.readString(false, SerializationTools.MAX_CLASS_LENGTH);
		hostSource=in.readObject(false, DecentralizedValue.class);
	}
}
