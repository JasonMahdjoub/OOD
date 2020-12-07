package com.distrimind.ood.database.centraldatabaseapi;

import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.messages.DistantBackupCenterConnexionInitialisation;
import com.distrimind.ood.database.messages.MessageDestinedToCentralDatabaseBackup;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.Integrity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public abstract class CentralDatabaseBackupReceiver {
	private final Map<DecentralizedValue, CentralDatabaseBackupReceiverPerPeer> receiversPerPeer=new HashMap<>();
	private final DatabaseWrapper wrapper;
	private final DecentralizedValue centralID;

	public CentralDatabaseBackupReceiver(DatabaseWrapper wrapper, DecentralizedValue centralID) {
		if (wrapper==null)
			throw new NullPointerException();
		if (centralID==null)
			throw new NullPointerException();
		this.wrapper = wrapper;
		this.centralID=centralID;
	}

	protected abstract CentralDatabaseBackupReceiverPerPeer newCentralDatabaseBackupReceiverPerPeerInstance(DatabaseWrapper wrapper);

	public Integrity received(MessageDestinedToCentralDatabaseBackup message) throws DatabaseException, IOException {
		CentralDatabaseBackupReceiverPerPeer r=receiversPerPeer.get(message.getHostSource());
		if (r==null)
		{
			if (message instanceof DistantBackupCenterConnexionInitialisation) {
				r = newCentralDatabaseBackupReceiverPerPeerInstance(wrapper);
				Integrity res=r.init((DistantBackupCenterConnexionInitialisation)message);
				if (r.isConnected())
					receiversPerPeer.put(message.getHostSource(), r);
				return res;
			}
			else
				return Integrity.FAIL_AND_CANDIDATE_TO_BAN;
		}
		Integrity res=r.received(message);
		if (!r.isConnected())
			receiversPerPeer.remove(message.getHostSource());
		return res;
	}
	public boolean isConnected(DecentralizedValue peerID)
	{
		CentralDatabaseBackupReceiverPerPeer r=receiversPerPeer.get(peerID);
		return r!=null && r.isConnected();
	}

	public DecentralizedValue getCentralID() {
		return centralID;
	}
}
