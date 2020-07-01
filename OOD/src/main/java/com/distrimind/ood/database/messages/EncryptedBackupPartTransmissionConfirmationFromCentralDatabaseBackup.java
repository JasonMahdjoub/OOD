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
public class EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup extends DatabaseEvent implements MessageComingFromCentralDatabaseBackup, SecureExternalizable {

	private long fileUTC;
	private long lastTransactionUTC;
	private DecentralizedValue hostDestination;

	@SuppressWarnings("unused")
	private EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup()
	{

	}

	public EncryptedBackupPartTransmissionConfirmationFromCentralDatabaseBackup(DecentralizedValue hostDestination, long fileUTC, long lastTransactionUTC) {
		if (hostDestination==null)
			throw new NullPointerException();
		if (fileUTC>lastTransactionUTC)
			throw new IllegalArgumentException();
		this.fileUTC = fileUTC;
		this.lastTransactionUTC=lastTransactionUTC;
	}

	public long getFileUTC() {
		return fileUTC;
	}

	@Override
	public DecentralizedValue getHostDestination() {
		return hostDestination;
	}

	public long getLastTransactionUTC() {
		return lastTransactionUTC;
	}

	@Override
	public int getInternalSerializedSize() {
		return 16+ SerializationTools.getInternalSize((SecureExternalizable)hostDestination);
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostDestination, false);
		out.writeLong(fileUTC);
		out.writeLong(lastTransactionUTC);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		hostDestination=in.readObject(false, DecentralizedValue.class);
		fileUTC=in.readLong();
		lastTransactionUTC=in.readLong();
	}
}
