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
	private FileCoordinate fileCoordinate;
	private String packageString;
	private boolean forRestoration;

	@SuppressWarnings("unused")
	private AskForDatabaseBackupPartDestinedToCentralDatabaseBackup() {
	}
	public AskForDatabaseBackupPartDestinedToCentralDatabaseBackup(String packageString, DecentralizedValue hostSource, FileCoordinate fileCoordinate, boolean forRestoration) {
		if (hostSource==null)
			throw new NullPointerException();
		if (packageString==null)
			throw new NullPointerException();
		if (packageString.trim().length()==0)
			throw new IllegalArgumentException();
		if (fileCoordinate==null)
			throw new NullPointerException();
		this.hostSource = hostSource;
		this.channelHost =hostSource;
		this.fileCoordinate = fileCoordinate;
		this.packageString=packageString;
		this.forRestoration=forRestoration;
	}
	public AskForDatabaseBackupPartDestinedToCentralDatabaseBackup(String packageString, DecentralizedValue hostSource, DecentralizedValue channelHost, FileCoordinate fileCoordinate, boolean forRestoration) {
		this(packageString, hostSource, fileCoordinate, forRestoration);
		if (channelHost ==null && !forRestoration)
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


	public boolean isForRestoration() {
		return forRestoration;
	}

	public FileCoordinate getFileCoordinate() {
		return fileCoordinate;
	}

	@Override
	public int getInternalSerializedSize() {
		return 11+ SerializationTools.getInternalSize((SecureExternalizable)hostSource)
				+SerializationTools.getInternalSize((SecureExternalizable) channelHost)
				+SerializationTools.getInternalSize(packageString, SerializationTools.MAX_CLASS_LENGTH);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeBoolean(forRestoration);
		out.writeObject(hostSource, false);
		assert channelHost !=null || forRestoration;
		if (channelHost==null)
			out.writeBoolean(false);
		else {
			out.writeBoolean(true);
			out.writeObject(channelHost == hostSource ? null : channelHost, true);
		}
		fileCoordinate.write(out);
		out.writeString(packageString, false, SerializationTools.MAX_CLASS_LENGTH);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		forRestoration=in.readBoolean();
		hostSource=in.readObject(false, DecentralizedValue.class);
		if (in.readBoolean()) {
			channelHost = in.readObject(true, DecentralizedValue.class);
			if (channelHost == null)
				channelHost = hostSource;
		}
		else
		{
			channelHost=null;
			if (!forRestoration)
				throw new MessageExternalizationException(Integrity.FAIL);
		}
		fileCoordinate=FileCoordinate.read(in);
		if (fileCoordinate==null)
			throw new MessageExternalizationException(Integrity.FAIL);
		packageString=in.readString(false, SerializationTools.MAX_CLASS_LENGTH);
		if (packageString.trim().length()==0)
			throw new MessageExternalizationException(Integrity.FAIL);
	}

	public String getPackageString() {
		return packageString;
	}

	@Override
	public String toString() {
		return "AskForDatabaseBackupPartDestinedToCentralDatabaseBackup{" +
				"hostSource=" + hostSource +
				", channelHost=" + channelHost +
				", fileCoordinate=" + fileCoordinate +
				", packageString='" + packageString + '\'' +
				'}';
	}
}
