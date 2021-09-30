package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.EncryptionTools;
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

	static final int MAX_SIZE_IN_BYTES_OF_VALIDATED_AND_ENCRYPTED_IDS= DatabaseWrapper.MAX_DISTANT_PEERS* (EncryptionTools.MAX_ENCRYPTED_ID_SIZE+DatabaseWrapper.MAX_ACCEPTED_SIZE_IN_BYTES_OF_DECENTRALIZED_VALUE);
	private DecentralizedValue hostDestination;
	private String packageString;
	private DecentralizedValue sourceChannel;
	private long firstBackupPartTimeUTC, lastBackupPartUTC;
	//private Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost;
	private Map<DecentralizedValue, byte[]> lastValidatedAndEncryptedDistantIdsPerHost;
	private transient Map<DecentralizedValue, Long> lastValidatedDistantIdsPerHost=null;
	private Map<String, Long> lastValidatedTransactionsUTCForDestinationHost;

	@SuppressWarnings("unused")
	private SynchronizationPlanMessageComingFromCentralDatabaseBackup() {
	}

	public SynchronizationPlanMessageComingFromCentralDatabaseBackup(DecentralizedValue hostDestination,
																	 String packageString,
																	 DecentralizedValue sourceChannel,
																	 long firstBackupPartTimeUTC,
																	 long lastBackupPartUTC,
																	 Map<DecentralizedValue, byte[]> lastValidatedAndEncryptedDistantIdsPerHost,
																	 //Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> lastValidatedAndEncryptedIDsPerHost,
																	 Map<String, Long> lastValidatedTransactionsUTCForDestinationHost) {
		if (hostDestination==null)
			throw new NullPointerException();
		if (packageString==null)
			throw new NullPointerException();
		if (sourceChannel==null)
			throw new NullPointerException();
		if (lastValidatedAndEncryptedDistantIdsPerHost==null)
			throw new NullPointerException();
		//InitialMessageComingFromCentralBackup.checkLastValidatedLocalAndDistantEncryptedIDs(lastValidatedAndEncryptedIDsPerHost);
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
		this.lastValidatedAndEncryptedDistantIdsPerHost = lastValidatedAndEncryptedDistantIdsPerHost;
		this.lastValidatedTransactionsUTCForDestinationHost = lastValidatedTransactionsUTCForDestinationHost;
	}

	public SynchronizationPlanMessageComingFromCentralDatabaseBackup(DecentralizedValue hostDestination,
																	 String packageString) {
		if (hostDestination==null)
			throw new NullPointerException();
		if (packageString==null)
			throw new NullPointerException();

		this.packageString = packageString;
		this.hostDestination=hostDestination;
		this.sourceChannel=null;
		sourceChannelNull();
	}
	private void sourceChannelNull()
	{
		this.firstBackupPartTimeUTC = -1;
		this.lastBackupPartUTC = -1;
		this.lastValidatedAndEncryptedDistantIdsPerHost = null;
		this.lastValidatedTransactionsUTCForDestinationHost = null;
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
		int res=SerializationTools.getInternalSize(hostDestination)
				+SerializationTools.getInternalSize(sourceChannel)
				+SerializationTools.getInternalSize(packageString, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
		if (sourceChannel!=null)
			res+=16
				+SerializationTools.getInternalSize(lastValidatedAndEncryptedDistantIdsPerHost, MAX_SIZE_IN_BYTES_OF_VALIDATED_AND_ENCRYPTED_IDS)
				//+InitialMessageComingFromCentralBackup.getInternalSerializedSize(lastValidatedAndEncryptedIDsPerHost, null)
				+InitialMessageComingFromCentralBackup.getInternalSerializedSize(lastValidatedTransactionsUTCForDestinationHost);
		return res;
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostDestination, false);
		out.writeString(packageString, false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
		out.writeObject(sourceChannel, true);
		if (sourceChannel!=null) {
			out.writeLong(firstBackupPartTimeUTC);
			out.writeLong(lastBackupPartUTC);
			out.writeMap(lastValidatedAndEncryptedDistantIdsPerHost, false, MAX_SIZE_IN_BYTES_OF_VALIDATED_AND_ENCRYPTED_IDS, false, false);
			//InitialMessageComingFromCentralBackup.write(out, lastValidatedAndEncryptedIDsPerHost, null);
			InitialMessageComingFromCentralBackup.write(out, lastValidatedTransactionsUTCForDestinationHost);
		}
	}


	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		lastValidatedDistantIdsPerHost=null;
		//lastValidatedAndEncryptedIDsPerHost=new HashMap<>();
		lastValidatedTransactionsUTCForDestinationHost=new HashMap<>();
		hostDestination=in.readObject(false);
		packageString=in.readString(false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
		sourceChannel=in.readObject(true);
		if (sourceChannel!=null) {
			firstBackupPartTimeUTC = in.readLong();
			lastBackupPartUTC = in.readLong();
			if (firstBackupPartTimeUTC == Long.MIN_VALUE)
				throw new MessageExternalizationException(Integrity.FAIL);
			if (lastBackupPartUTC == Long.MAX_VALUE)
				throw new MessageExternalizationException(Integrity.FAIL);
			if (firstBackupPartTimeUTC > lastBackupPartUTC)
				throw new MessageExternalizationException(Integrity.FAIL);
			//InitialMessageComingFromCentralBackup.read(in, lastValidatedAndEncryptedIDsPerHost, null);
			lastValidatedAndEncryptedDistantIdsPerHost = in.readMap(false, MAX_SIZE_IN_BYTES_OF_VALIDATED_AND_ENCRYPTED_IDS, DecentralizedValue.class, byte[].class);
			InitialMessageComingFromCentralBackup.read(in, lastValidatedTransactionsUTCForDestinationHost);
		}
		else
			sourceChannelNull();
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

	/*public Map<DecentralizedValue, LastValidatedLocalAndDistantEncryptedID> getLastValidatedAndEncryptedIDsPerHost() {
		return lastValidatedAndEncryptedIDsPerHost;
	}*/

	public Map<DecentralizedValue, byte[]> getLastValidatedAndEncryptedDistantIdsPerHost() {
		return lastValidatedAndEncryptedDistantIdsPerHost;
	}

	public Map<String, Long> getLastValidatedTransactionsUTCForDestinationHost() {
		return lastValidatedTransactionsUTCForDestinationHost;
	}
	/*public Map<DecentralizedValue, LastValidatedLocalAndDistantID> getLastValidatedIDsPerHost(EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		return InitialMessageComingFromCentralBackup.getLastValidatedIDsPerHost(encryptionProfileProvider, lastValidatedAndEncryptedIDsPerHost);
	}*/

	public Map<DecentralizedValue, Long> getLastValidatedIDsPerHost(EncryptionProfileProvider encryptionProfileProvider) throws IOException {
		if (lastValidatedDistantIdsPerHost==null)
		{
			Map<DecentralizedValue, Long> lastValidatedDistantIdsPerHost=new HashMap<>();
			for (Map.Entry<DecentralizedValue, byte[]> e : lastValidatedAndEncryptedDistantIdsPerHost.entrySet())
			{
				lastValidatedDistantIdsPerHost.put(e.getKey(), EncryptionTools.decryptID(encryptionProfileProvider, e.getValue()));
			}
			this.lastValidatedDistantIdsPerHost=lastValidatedDistantIdsPerHost;
		}
		return this.lastValidatedDistantIdsPerHost;
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

	public boolean isEmptyPlan() {
		return sourceChannel==null;
	}
}
