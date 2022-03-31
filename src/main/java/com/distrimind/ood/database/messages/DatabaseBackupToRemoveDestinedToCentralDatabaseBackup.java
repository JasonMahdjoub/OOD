package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.Table;
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

	public DatabaseBackupToRemoveDestinedToCentralDatabaseBackup(DecentralizedValue hostSource, String packageString, CentralDatabaseBackupCertificate certificate){
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
	public int getInternalSerializedSizeWithoutSignatures() {
		return SerializationTools.getInternalSize(packageString, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH)+super.getInternalSerializedSizeWithoutSignatures();
	}

	@Override
	public void writeExternalWithoutSignatures(SecuredObjectOutputStream out) throws IOException {
		super.writeExternalWithoutSignatures(out);
		out.writeString(packageString, false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
	}

	@Override
	public void readExternalWithoutSignatures(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		super.readExternalWithoutSignatures(in);
		packageString =in.readString(false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
		if (packageString.trim().length()==0)
			throw new MessageExternalizationException(Integrity.FAIL);
	}

	@Override
	public String toString() {
		return "DatabaseBackupToRemoveDestinedToCentralDatabaseBackup{" +
				"packageString='" + packageString + '\'' +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (o==this)
			return true;
		if (o instanceof DatabaseBackupToRemoveDestinedToCentralDatabaseBackup)
		{
			return ((DatabaseBackupToRemoveDestinedToCentralDatabaseBackup) o).packageString.equals(this.packageString);
		}
		else
			return false;
	}
}
