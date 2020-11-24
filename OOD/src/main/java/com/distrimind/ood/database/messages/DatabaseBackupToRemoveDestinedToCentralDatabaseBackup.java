package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.DatabaseEvent;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.*;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class DatabaseBackupToRemoveDestinedToCentralDatabaseBackup extends DatabaseEvent implements MessageDestinedToCentralDatabaseBackup, SecureExternalizable
{
	private String packageString;
	private DecentralizedValue hostSource;


	@SuppressWarnings("unused")
	private DatabaseBackupToRemoveDestinedToCentralDatabaseBackup() {
	}

	public DatabaseBackupToRemoveDestinedToCentralDatabaseBackup(DecentralizedValue hostSource, String packageString) {
		if (hostSource==null)
			throw new NullPointerException();
		if (packageString==null)
			throw new NullPointerException();
		if (packageString.trim().length()==0)
			throw new IllegalArgumentException();
		this.packageString = packageString;
		this.hostSource=hostSource;
	}

	public String getPackageString() {
		return packageString;
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
		return SerializationTools.getInternalSize(packageString, SerializationTools.MAX_CLASS_LENGTH)+SerializationTools.getInternalSize((SecureExternalizable)hostSource);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeString(packageString, false, SerializationTools.MAX_CLASS_LENGTH);
		out.writeObject(hostSource, false);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		packageString =in.readString(false, SerializationTools.MAX_CLASS_LENGTH);
		if (packageString.trim().length()==0)
			throw new MessageExternalizationException(Integrity.FAIL);
		hostSource=in.readObject(false, DecentralizedValue.class);

	}
}
