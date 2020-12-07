package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.centraldatabaseapi.CentralDatabaseBackupCertificate;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.*;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class DatabaseBackupToRemoveDestinedToCentralDatabaseBackup extends AuthenticatedMessageDestinedToCentralDatabaseBackup
{
	private String packageString;


	@SuppressWarnings("unused")
	private DatabaseBackupToRemoveDestinedToCentralDatabaseBackup() {
	}

	public DatabaseBackupToRemoveDestinedToCentralDatabaseBackup(DecentralizedValue hostSource, String packageString, CentralDatabaseBackupCertificate certificate) {
		super(hostSource, certificate);
		if (packageString==null)
			throw new NullPointerException();
		if (packageString.trim().length()==0)
			throw new IllegalArgumentException();
		this.packageString = packageString;
	}

	public String getPackageString() {
		return packageString;
	}


	@Override
	public boolean cannotBeMerged() {
		return true;
	}

	@Override
	public int getInternalSerializedSize() {
		return SerializationTools.getInternalSize(packageString, SerializationTools.MAX_CLASS_LENGTH)+super.getInternalSerializedSize();
	}

	@Override
	public void writeExternalWithoutSignature(SecuredObjectOutputStream out) throws IOException {
		super.writeExternalWithoutSignature(out);
		out.writeString(packageString, false, SerializationTools.MAX_CLASS_LENGTH);
	}

	@Override
	public void readExternalWithoutSignature(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		super.readExternalWithoutSignature(in);
		packageString =in.readString(false, SerializationTools.MAX_CLASS_LENGTH);
		if (packageString.trim().length()==0)
			throw new MessageExternalizationException(Integrity.FAIL);
	}
}
