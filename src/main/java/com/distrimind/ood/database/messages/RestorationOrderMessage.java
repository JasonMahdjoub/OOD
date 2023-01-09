package com.distrimind.ood.database.messages;
/*
Copyright or © or Corp. Jason Mahdjoub (01/04/2013)

jason.mahdjoub@distri-mind.fr

This software (Object Oriented Database (OOD)) is a computer program 
whose purpose is to manage a local database with the object paradigm 
and the java language 

This software is governed by the CeCILL-C license under French law and
abiding by the rules of distribution of free software.  You can  use, 
modify and/ or redistribute the software under the terms of the CeCILL-C
license as circulated by CEA, CNRS and INRIA at the following URL
"http://www.cecill.info". 

As a counterpart to the access to the source code and  rights to copy,
modify and redistribute granted by the license, users are provided only
with a limited warranty  and the software's author,  the holder of the
economic rights,  and the successive licensors  have only  limited
liability. 

In this respect, the user's attention is drawn to the risks associated
with loading,  using,  modifying and/or developing or reproducing the
software by the user in light of its specific status of free software,
that may mean  that it is complicated to manipulate,  and  that  also
therefore means  that it is reserved for developers  and  experienced
professionals having in-depth computer knowledge. Users are therefore
encouraged to load and test the software's suitability as regards their
requirements in conditions enabling the security of their systems and/or 
data to be ensured and,  more generally, to use and operate it in the 
same conditions as regards security. 

The fact that you are presently reading this means that you have had
knowledge of the CeCILL-C license and that you accept its terms.
 */

import com.distrimind.ood.database.AuthenticatedP2PMessage;
import com.distrimind.ood.database.DatabaseEvent;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.io.SecuredObjectInputStream;
import com.distrimind.util.io.SecuredObjectOutputStream;
import com.distrimind.util.io.SerializationTools;

import java.io.IOException;

/**
 * Restoration order message
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class RestorationOrderMessage extends DatabaseEvent implements AuthenticatedP2PMessage {

	private DecentralizedValue hostSource, hostDestination, hostThatApplyRestoration;
	private long timeUTCOfRestorationInMs;
	private long messageID;
	private String databasePackage;
	private boolean chooseNearestBackupIfNoBackupMatch;
	private byte[] signatures;
	private long timeUTCInMsWhenRestorationIsDone;
	@SuppressWarnings("unused")
	private RestorationOrderMessage() {
	}

	public RestorationOrderMessage(DecentralizedValue hostSource, DecentralizedValue hostDestination, DecentralizedValue hostThatApplyRestoration, String databasePackage, long timeUTCOfRestorationInMs,boolean chooseNearestBackupIfNoBackupMatch, long timeUTCInMsWhenRestorationIsDone) {
		if (hostSource==null)
			throw new NullPointerException();
		if (hostDestination==null)
			throw new NullPointerException();
		if (hostThatApplyRestoration==null)
			throw new NullPointerException();
		if (databasePackage==null)
			throw new NullPointerException();

		this.hostSource = hostSource;
		this.hostDestination = hostDestination;
		this.hostThatApplyRestoration = hostThatApplyRestoration;
		this.timeUTCOfRestorationInMs = timeUTCOfRestorationInMs;
		this.messageID = -1;
		this.signatures = null;
		this.databasePackage=databasePackage;
		this.chooseNearestBackupIfNoBackupMatch=chooseNearestBackupIfNoBackupMatch;
		this.timeUTCInMsWhenRestorationIsDone = timeUTCInMsWhenRestorationIsDone;
	}

	public boolean isChooseNearestBackupIfNoBackupMatch() {
		return chooseNearestBackupIfNoBackupMatch;
	}

	@Override
	public long getMessageID() {
		return messageID;
	}

	@Override
	public void setMessageID(long messageID) {
		this.messageID=messageID;
	}

	@Override
	public DatabaseEvent.MergeState tryToMergeWithNewAuthenticatedMessage(DatabaseEvent newEvent) throws DatabaseException {
		if (newEvent instanceof RestorationOrderMessage)
		{
			RestorationOrderMessage m=(RestorationOrderMessage)newEvent;
			if (m.getHostDestination().equals(hostDestination) && m.getHostSource().equals(hostSource))
				return MergeState.DELETE_OLD;
		}

		return MergeState.NO_FUSION;
	}

	@Override
	public void writeExternalWithoutSignatures(SecuredObjectOutputStream out) throws IOException {
		out.writeObject(hostSource, false );
		out.writeObject(hostDestination, false );
		out.writeObject(hostThatApplyRestoration, false );
		out.writeLong(timeUTCOfRestorationInMs);
		out.writeString(databasePackage, false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
		out.writeLong(messageID);
		out.writeBoolean(chooseNearestBackupIfNoBackupMatch);
		out.writeLong(timeUTCInMsWhenRestorationIsDone);
	}

	@Override
	public void readExternalWithoutSignatures(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		hostSource=in.readObject(false);
		hostDestination=in.readObject(false);
		hostThatApplyRestoration=in.readObject(false);
		timeUTCOfRestorationInMs =in.readLong();
		databasePackage=in.readString(false, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH);
		messageID=in.readLong();
		chooseNearestBackupIfNoBackupMatch=in.readBoolean();
		timeUTCInMsWhenRestorationIsDone =in.readLong();
	}

	@Override
	public int getInternalSerializedSizeWithoutSignatures() {
		return 25+SerializationTools.getInternalSize(hostSource)+
				SerializationTools.getInternalSize(hostDestination)+
				SerializationTools.getInternalSize(hostThatApplyRestoration)+
				SerializationTools.getInternalSize(databasePackage, Table.MAX_DATABASE_PACKAGE_NAME_LENGTH)
				;
	}

	@Override
	public void setSignatures(byte[] signatures) {
		if (signatures==null)
			throw new NullPointerException();
		this.signatures=signatures;
	}

	@Override
	public byte[] getSignatures() {
		return signatures;
	}

	@Override
	public DecentralizedValue getHostDestination()  {
		return hostDestination;
	}

	@Override
	public DecentralizedValue getHostSource() {
		return hostSource;
	}

	public DecentralizedValue getHostThatApplyRestoration() {
		return hostThatApplyRestoration;
	}

	public long getTimeUTCOfRestorationInMs() {
		return timeUTCOfRestorationInMs;
	}

	public String getDatabasePackage() {
		return databasePackage;
	}

	public long getTimeUTCInMsWhenRestorationIsDone() {
		return timeUTCInMsWhenRestorationIsDone;
	}
}
