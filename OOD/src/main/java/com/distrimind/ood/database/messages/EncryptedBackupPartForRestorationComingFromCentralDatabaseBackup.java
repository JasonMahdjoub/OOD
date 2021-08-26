package com.distrimind.ood.database.messages;

import com.distrimind.ood.database.EncryptedDatabaseBackupMetaDataPerFile;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.RandomInputStream;

/**
 * @author Jason Mahdjoub
 * @version 2.0
 * @since OOD 3.0.0
 */
public class EncryptedBackupPartForRestorationComingFromCentralDatabaseBackup extends AbstractEncryptedBackupPartComingFromCentralDatabaseBackup{
	public EncryptedBackupPartForRestorationComingFromCentralDatabaseBackup(DecentralizedValue hostSource, DecentralizedValue hostDestination, EncryptedDatabaseBackupMetaDataPerFile metaData, RandomInputStream partInputStream, DecentralizedValue channelHost) {
		super(hostSource, hostDestination, metaData, partInputStream, channelHost);
	}

	protected EncryptedBackupPartForRestorationComingFromCentralDatabaseBackup() {
	}
}
