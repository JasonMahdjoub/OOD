package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.Table;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.crypto.EncryptionProfileProvider;
import com.distrimind.util.io.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.1.0
 */
public class SynchronizationPlanMessageComingFromCentralDatabaseBackup implements MessageComingFromCentralDatabaseBackup {

	private DecentralizedValue hostDestination;
	private String packageString;
	private DecentralizedValue sourceChannel;
	private long firstBackupPartTimeUTC, lastBackupPartUTC;
	private Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost;
	private Map<String, Long> lastValidatedTransactionsUTCForDestinationHost;

	@SuppressWarnings("unused")
	private SynchronizationPlanMessageComingFromCentralDatabaseBackup() {
	}

	public SynchronizationPlanMessageComingFromCentralDatabaseBackup(DecentralizedValue hostDestination,
																	 String packageString,
																	 DecentralizedValue sourceChannel,
																	 long firstBackupPartTimeUTC,
																	 long lastBackupPartUTC,
																	 Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost,
																	 Map<String, Long> lastValidatedTransactionsUTCForDestinationHost) {
		if (hostDestination==null)
			throw new NullPointerException();
		if (packageString==null)
			throw new NullPointerException();
		if (sourceChannel==null)
			throw new NullPointerException();
		if (hostDestination.equals(sourceChannel))
			throw new IllegalArgumentException();
		InitialMessageComingFromCentralBackup.checkLastValidatedLocalAndDistantEncryptedIDs(lastValidatedAndEncryptedIDsPerHost);
		InitialMessageComingFromCentralBackup.checkLastValidatedTransactionsUTCForDestinationHost(lastValidatedTransactionsUTCForDestinationHost);
		if (firstBackupPartTimeUTC==Long.MIN_VALUE)
			throw new IllegalArgumentException();
		if (lastBackupPartUTC==Long.MAX_VALUE)
			throw new IllegalArgumentException();
		if (firstBackupPartTimeUTC>lastBackupPartUTC)
			throw new IllegalArgumentException();
		this.packageString = packageString;
		this.hostDestination=hostDestination;
		this.sourceChannel = sourceChannel;
		this.firstBackupPartTimeUTC = firstBackupPartTimeUTC;
		this.lastBackupPartUTC = lastBackupPartUTC;
		this.lastValidatedAndEncryptedIDsPerHost = lastValidatedAndEncryptedIDsPerHost;
		this.lastValidatedTransactionsUTCForDestinationHost = lastValidatedTransactionsUTCForDestinationHost;
	}

	@Override
	public boolean cannotBeMerged() {
		return false;
	}

	@Override
	public DecentralizedValue getHostDestination() {
		return hostDestination;
	}

	@Override
	public int getInternalSerializedSize() {
		return SerializationTools.getInternalSize(hostDestination)
				+SerializationTools.getInternalSize(sourceChannel)
				+SerializationTools.getInternalSize(packageString, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH)
				+8
				+InitialMessageComingFromCentralBackup.getInternalSerializedSize(lastValidatedAndEncryptedIDsPerHost, null)
				+InitialMessageComingFromCentralBackup.getInternalSerializedSize(lastValidatedTransactionsUTCForDestinationHost);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostDestination, false);
		out.writeObject(sourceChannel, false);
		out.writeString(packageString, false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
		out.writeLong(firstBackupPartTimeUTC);
		out.writeLong(lastBackupPartUTC);

		InitialMessageComingFromCentralBackup.write(out, lastValidatedAndEncryptedIDsPerHost, null);
		InitialMessageComingFromCentralBackup.write(out, lastValidatedTransactionsUTCForDestinationHost);
	}


	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		lastValidatedAndEncryptedIDsPerHost=new HashMap<>();
		lastValidatedTransactionsUTCForDestinationHost=new HashMap<>();
		hostDestination=in.readObject(false);
		sourceChannel=in.readObject(false);
		packageString=in.readString(false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
		firstBackupPartTimeUTC=in.readLong();
		lastBackupPartUTC=in.readLong();
		if (firstBackupPartTimeUTC==Long.MIN_VALUE)
			throw new MessageExternalizationException(Integrity.FAIL);
		if (lastBackupPartUTC==Long.MAX_VALUE)
			throw new MessageExternalizationException(Integrity.FAIL);
		if (firstBackupPartTimeUTC>lastBackupPartUTC)
			throw new MessageExternalizationException(Integrity.FAIL);
		InitialMessageComingFromCentralBackup.read(in, lastValidatedAndEncryptedIDsPerHost, null);
		InitialMessageComingFromCentralBackup.read(in, lastValidatedTransactionsUTCForDestinationHost);
	}

	public String getPackageString() {
		return packageString;
	}

	public DecentralizedValue getSourceChannel() {
		return sourceChannel;
	}

	public long getFirstBackupPartTimeUTC() {
		return firstBackupPartTimeUTC;
	}

	public long getLastBackupPartUTC() {
		return lastBackupPartUTC;
	}

	public Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> getLastValidatedAndEncryptedIDsPerHost() {
		return lastValidatedAndEncryptedIDsPerHost;
	}

	public Map<String, Long> getLastValidatedTransactionsUTCForDestinationHost() {
		return lastValidatedTransactionsUTCForDestinationHost;
	}
	public Map<DecentralizedValue, LastValidatedLocalAndDistantID> getLastValidatedIDsPerHost(EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		return InitialMessageComingFromCentralBackup.getLastValidatedIDsPerHost(encryptionProfileProvider, lastValidatedAndEncryptedIDsPerHost);
	}

	@Override
	public String toString() {
		return "SynchronizationPlanMessageComingFromCentralDatabaseBackup{" +
				"hostDestination=" + hostDestination +
				", packageString='" + packageString + '\'' +
				", sourceChannel=" + sourceChannel +
				", firstBackupPartTimeUTC=" + firstBackupPartTimeUTC +
				", lastBackupPartUTC=" + lastBackupPartUTC +
				'}';
	}
}
