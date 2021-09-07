package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.DatabaseEvent;
import com.distrimind.ood.database.Table;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.*;

import java.io.IOException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since MaDKitLanEdition 3.0.0
 */
public class AskForDatabaseBackupPartDestinedToCentralDatabaseBackup extends DatabaseEvent implements ChannelMessageDestinedToCentralDatabaseBackup, SecureExternalizable {
	public enum Context
	{
		SYNCHRONIZATION,
		RESTORATION,
		INITIAL_SYNCHRONIZATION
	}
	private DecentralizedValue hostSource;
	private DecentralizedValue channelHost;
	private FileCoordinate fileCoordinate;
	private String packageString;
	private Context context;

	@SuppressWarnings("unused")
	private AskForDatabaseBackupPartDestinedToCentralDatabaseBackup() {
	}
	public AskForDatabaseBackupPartDestinedToCentralDatabaseBackup(String packageString, DecentralizedValue hostSource, FileCoordinate fileCoordinate, Context context) {
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
		this.context=context;
	}
	public AskForDatabaseBackupPartDestinedToCentralDatabaseBackup(String packageString, DecentralizedValue hostSource, DecentralizedValue channelHost, FileCoordinate fileCoordinate, Context context) {
		this(packageString, hostSource, fileCoordinate, context);
		if (channelHost ==null && context!=Context.RESTORATION)
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

	public Context getContext() {
		return context;
	}

	public FileCoordinate getFileCoordinate() {
		return fileCoordinate;
	}

	@Override
	public int getInternalSerializedSize() {
		return 10+ SerializationTools.getInternalSize(hostSource)
				+SerializationTools.getInternalSize(channelHost)
				+SerializationTools.getInternalSize(packageString, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH)
				+SerializationTools.getInternalSize(context);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeEnum(context, false);
		out.writeObject(hostSource, false);
		assert channelHost !=null || context==Context.RESTORATION;
		if (channelHost==null)
			out.writeBoolean(false);
		else {
			out.writeBoolean(true);
			out.writeObject(channelHost == hostSource ? null : channelHost, true);
		}
		fileCoordinate.write(out);
		out.writeString(packageString, false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		context=in.readEnum(false);
		hostSource=in.readObject(false, DecentralizedValue.class);
		if (in.readBoolean()) {
			channelHost = in.readObject(true, DecentralizedValue.class);
			if (channelHost == null)
				channelHost = hostSource;
		}
		else
		{
			channelHost=null;
			if (context!=Context.RESTORATION)
				throw new MessageExternalizationException(Integrity.FAIL);
		}
		fileCoordinate=FileCoordinate.read(in);
		if (fileCoordinate==null)
			throw new MessageExternalizationException(Integrity.FAIL);
		packageString=in.readString(false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
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
				", context='"+context+'\''+
				'}';
	}
}
