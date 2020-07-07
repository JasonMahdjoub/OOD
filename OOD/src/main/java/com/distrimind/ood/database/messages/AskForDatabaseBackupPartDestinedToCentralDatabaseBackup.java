package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.DatabaseEvent;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.*;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 3.0.0
 */
public class AskForDatabaseBackupPartDestinedToCentralDatabaseBackup extends DatabaseEvent implements ChannelMessageDestinedToCentralDatabaseBackup, SecureExternalizable {

	private DecentralizedValue hostSource;
	private DecentralizedValue channelHost;
	private long lastFileTimestampUTCToNotInclude;
	private String packageString;

	@SuppressWarnings("unused")
	private AskForDatabaseBackupPartDestinedToCentralDatabaseBackup() {
	}
	public AskForDatabaseBackupPartDestinedToCentralDatabaseBackup(String packageString, DecentralizedValue hostSource, long lastFileTimestampUTCToNotInclude) {
		if (hostSource==null)
			throw new NullPointerException();
		if (packageString==null)
			throw new NullPointerException();
		if (packageString.trim().length()==0)
			throw new IllegalArgumentException();
		this.hostSource = hostSource;
		this.channelHost =hostSource;
		this.lastFileTimestampUTCToNotInclude = lastFileTimestampUTCToNotInclude;
		this.packageString=packageString;
	}
	public AskForDatabaseBackupPartDestinedToCentralDatabaseBackup(String packageString, DecentralizedValue hostSource, DecentralizedValue channelHost, long lastFileTimestampUTCToNotInclude) {
		this(packageString, hostSource, lastFileTimestampUTCToNotInclude);
		if (channelHost ==null)
			throw new NullPointerException();
		this.channelHost = channelHost;
	}

	@Override
	public DecentralizedValue getChannelHost() {
		return channelHost;
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
		return 8+ SerializationTools.getInternalSize((SecureExternalizable)hostSource)
				+SerializationTools.getInternalSize((SecureExternalizable) channelHost)
				+SerializationTools.getInternalSize(packageString, SerializationTools.MAX_CLASS_LENGTH);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostSource, false);
		assert channelHost !=null;
		out.writeObject(channelHost ==hostSource?null: channelHost, true);
		out.writeLong(lastFileTimestampUTCToNotInclude);
		out.writeString(packageString, false, SerializationTools.MAX_CLASS_LENGTH);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		hostSource=in.readObject(false, DecentralizedValue.class);
		channelHost =in.readObject(true, DecentralizedValue.class);
		if (channelHost ==null)
			channelHost =hostSource;
		lastFileTimestampUTCToNotInclude =in.readLong();
		packageString=in.readString(false, SerializationTools.MAX_CLASS_LENGTH);
		if (packageString.trim().length()==0)
			throw new MessageExternalizationException(Integrity.FAIL);
	}

	public String getPackageString() {
		return packageString;
	}
}
