package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.centraldatabaseapi.CentralDatabaseBackupCertificate;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.SecuredObjectInputStream;
import com.distrimind.util.io.SecuredObjectOutputStream;
import com.distrimind.util.io.SerializationTools;

import java.io.IOException;
import java.util.Set;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.1.0
 */
public class AskForInitialSynchronizationPlanMessageDestinedToCentralDatabaseBackup extends AuthenticatedMessageDestinedToCentralDatabaseBackup {
	static final int MAX_SIZE_IN_BYTES_OF_PEERS_COLLECTION=4+DatabaseWrapper.MAX_DISTANT_PEERS*DatabaseWrapper.MAX_ACCEPTED_SIZE_IN_BYTES_OF_DECENTRALIZED_VALUE;
	private String packageString;
	private Set<DecentralizedValue> acceptedDataSources;


	public AskForInitialSynchronizationPlanMessageDestinedToCentralDatabaseBackup() {
	}

	public AskForInitialSynchronizationPlanMessageDestinedToCentralDatabaseBackup(DecentralizedValue hostSource, CentralDatabaseBackupCertificate certificate, String packageString, Set<DecentralizedValue> acceptedDataSources) {
		super(hostSource, certificate);
		if (packageString==null)
			throw new NullPointerException();
		if (acceptedDataSources==null)
			throw new NullPointerException();
		if (acceptedDataSources.size()==0)
			throw new IllegalArgumentException();
		this.packageString = packageString;
		this.acceptedDataSources = acceptedDataSources;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof AskForInitialSynchronizationPlanMessageDestinedToCentralDatabaseBackup) {
			AskForInitialSynchronizationPlanMessageDestinedToCentralDatabaseBackup a=(AskForInitialSynchronizationPlanMessageDestinedToCentralDatabaseBackup)o;
			return packageString.equals(a.packageString)
					&& getHostSource().equals(a.getHostSource());
		}
		return false;
	}

	@Override
	public boolean cannotBeMerged() {
		return false;
	}

	public String getPackageString() {
		return packageString;
	}

	public Set<DecentralizedValue> getAcceptedDataSources() {
		return acceptedDataSources;
	}

	@Override
	public void writeExternalWithoutSignatures(SecuredObjectOutputStream out) throws IOException {
		super.writeExternalWithoutSignatures(out);
		out.writeString(packageString, false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
		out.writeCollection(acceptedDataSources, false, MAX_SIZE_IN_BYTES_OF_PEERS_COLLECTION);
	}

	@Override
	public void readExternalWithoutSignatures(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		super.readExternalWithoutSignatures(in);
		packageString=in.readString(false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
		acceptedDataSources=in.readCollection(false, MAX_SIZE_IN_BYTES_OF_PEERS_COLLECTION, DecentralizedValue.class);
	}

	@Override
	public int getInternalSerializedSizeWithoutSignatures() {
		return super.getInternalSerializedSizeWithoutSignatures()
				+ SerializationTools.getInternalSize(packageString, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH)
				+ SerializationTools.getInternalSize(acceptedDataSources, MAX_SIZE_IN_BYTES_OF_PEERS_COLLECTION);
	}

	@Override
	public String toString() {
		return "AskForInitialSynchronizationPlanMessageDestinedToCentralDatabaseBackup{" +
				"hostSource=" + getHostSource()+
				", packageString='" + packageString + '\'' +
				", acceptedDataSources=" + acceptedDataSources +
				'}';
	}
}
