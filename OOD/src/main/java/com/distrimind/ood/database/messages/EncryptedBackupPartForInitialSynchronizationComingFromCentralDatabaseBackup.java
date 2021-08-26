package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.EncryptedDatabaseBackupMetaDataPerFile;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.RandomInputStream;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.1.0
 */
public class EncryptedBackupPartForInitialSynchronizationComingFromCentralDatabaseBackup extends AbstractEncryptedBackupPartComingFromCentralDatabaseBackup{
	public EncryptedBackupPartForInitialSynchronizationComingFromCentralDatabaseBackup(DecentralizedValue hostSource, DecentralizedValue hostDestination, EncryptedDatabaseBackupMetaDataPerFile metaData, RandomInputStream partInputStream, DecentralizedValue channelHost) {
		super(hostSource, hostDestination, metaData, partInputStream, channelHost);
	}

	public EncryptedBackupPartForInitialSynchronizationComingFromCentralDatabaseBackup() {
	}
}
