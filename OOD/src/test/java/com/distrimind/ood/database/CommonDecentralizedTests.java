package com.distrimind.ood.database;
/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

jason.mahdjoub@distri-mind.fr

This software (Object Oriented Database (OOD)) is a computer program 
whose purpose is to manage a local database with the object paradigm 
and the java langage 

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

import com.distrimind.ood.database.decentralizeddatabase.*;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.messages.*;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.DecentralizedIDGenerator;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.FileTools;
import com.distrimind.util.crypto.ASymmetricAuthenticatedSignatureType;
import com.distrimind.util.crypto.SecureRandomType;
import com.distrimind.util.io.RandomByteArrayInputStream;
import com.distrimind.util.io.RandomByteArrayOutputStream;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Jason
 * @version 1.0
 * @since OOD 2.5.0
 */
public abstract class CommonDecentralizedTests {

	public static class DistantDatabaseEvent {
		private final byte[] eventToSend;
		private final byte[] joinedData;
		private final DecentralizedValue hostDest;

		DistantDatabaseEvent(DatabaseWrapper wrapper, DBEventToSend eventToSend)
				throws Exception {
			try (RandomByteArrayOutputStream baos = new RandomByteArrayOutputStream()) {
				baos.writeObject(eventToSend, false);

				baos.flush();

				this.eventToSend = baos.getBytes();
			}
			if (eventToSend instanceof BigDatabaseEventToSend) {
				BigDatabaseEventToSend b = (BigDatabaseEventToSend) eventToSend;
				final AtomicReference<RandomByteArrayOutputStream> baos=new AtomicReference<>();
				try (OutputStreamGetter osg=new OutputStreamGetter() {

					@Override
					public RandomOutputStream initOrResetOutputStream() {
						if (baos.get()!=null)
							baos.get().close();
						baos.set(new RandomByteArrayOutputStream());
						return baos.get();
					}

					@Override
					public void close() {
						if (baos.get()!=null)
							baos.get().close();
					}
				}) {
					b.exportToOutputStream(wrapper, osg);
					baos.get().flush();
					this.joinedData = baos.get().getBytes();
				}
			} else
				this.joinedData = null;
			hostDest = eventToSend.getHostDestination();
		}

		public DBEventToSend getDatabaseEventToSend() throws IOException, ClassNotFoundException {
			try (RandomByteArrayInputStream bais = new RandomByteArrayInputStream(eventToSend)) {
				return bais.readObject(false, DBEventToSend.class);
			}
		}

		public RandomInputStream getInputStream() {
			return new RandomByteArrayInputStream(joinedData);
		}

		public DecentralizedValue getHostDestination() {
			return hostDest;
		}
	}
	static File centralDatabaseBackupDirectory=new File("centralDatabaseBackup");
	public class CentralDatabaseBackup
	{
		final Map<DecentralizedValue, DatabaseBackupMetaData> channels=new HashMap<>();
		final Map<DecentralizedValue, Long> lastAskedTransactionSynchronizationFromServer=new HashMap<>();
		final Map<DecentralizedValue, Map<DecentralizedValue, Long>> validatedDistantTransactionsIDPerHost=new HashMap<>();

		public File getDirectory(DecentralizedValue host, String packageString)
		{
			return new File(centralDatabaseBackupDirectory, host.encodeString()+File.separator+packageString.replace('.', File.separatorChar));
		}
		public File getFile(DecentralizedValue host, String packageString, File file)
		{
			return new File(getDirectory(host, packageString), file.getName());
		}
		//this function is called when client is connecting to server
		public void receiveMessage(DatabaseTransactionsIdentifiersToSynchronizeDestinedToCentralDatabaseBackup message) throws DatabaseException {
			for (Map.Entry<DecentralizedValue, Long> e : message.getLastDistantTransactionIdentifiers().entrySet())
			{
				DatabaseBackupMetaData m=channels.get(e.getKey());

				if (message.getHostSource().equals(e.getKey()))
				{
					Long lastTransactionUTC=null;
					if (m==null || (lastTransactionUTC=m.getLastUpdatedTransactionUTC())==null)
					{
						lastTransactionUTC=Long.MIN_VALUE;
					}
					else
					{
						if (!lastAskedTransactionSynchronizationFromServer.containsKey(message.getHostSource()))
						{
							if (lastTransactionUTC>=e.getValue())
								lastTransactionUTC=null;
							else {
								++lastTransactionUTC;
								lastAskedTransactionSynchronizationFromServer.put(e.getKey(), lastTransactionUTC);
							}
						}
						else
							lastTransactionUTC=null;
					}
					if (lastTransactionUTC!=null)
					{
						for (Database d2 : listDatabase)
						{
							if (d2.hostID.equals(message.getHostSource()))
							{
								d2.getReceivedCentralDatabaseBackupEvents().add(
										new AskComingFromDatabaseBackupCenterForSynchronization(message.getHostSource(), lastTransactionID ));
								break;
							}
						}
					}
				}
				else
				{
					Long lastTransactionID=null;
					if (m==null || (lastTransactionID=m.getLastUpdatedTransactionID())==null)
					{
						if (message.getHostSource().equals(e.getKey()))
						{
							lastTransactionID=Long.MIN_VALUE;
						}
					}
					else{

						Map<DecentralizedValue, Long> oldm=validatedDistantTransactionsIDPerHost.get(message.getHostSource());
						if (oldm!=null)
						{
							Long l=oldm.get(e.getKey());
							if (l!=null)
							{
								lastTransactionID=null;
							}
						}
						if (lastTransactionID!=null && lastTransactionID>e.getValue())
						{
							for (Database d2 : listDatabase)
							{
								if (d2.hostID.equals(e.getKey()))
								{
									if (d2.isConnected()) {
										d2.getReceivedCentralDatabaseBackupEvents().add(
												new AskComingFromDatabaseBackupCenterForSynchronization(d2.hostID, e.getValue() + 1));
									}
									break;
								}
							}
						}
						lastTransactionID=null;
					}
				}


			}
			validatedDistantTransactionsIDPerHost.put(message.getHostSource(), message.getLastDistantTransactionIdentifiers());

			//TODO check if next code is necessary
			for (Database d2 : listDatabase)
			{
				if (!d2.hostID.equals(message.getHostSource()))
				{
					if (!d2.dbwrapper.getSynchronizer().isInitializedWithCentralBackup(message.getHostSource())) {
						d2.getDbwrapper().getSynchronizer().initDistantBackupCenter(message.getHostSource());
					}
				}
			}
		}

		public void receiveMessage(AskDestinedToDatabaseBackupCenterForSynchronization message)
		{
			//TODO message
		}

		void disconnect(DecentralizedValue host)
		{
			validatedDistantTransactionsIDPerHost.remove(host);
		}

		//TODO continue from here


		//TODO remove DatabaseWrapper.LastIDCorrectionForCentralDatabaseBackup
		public void receiveMessage(DatabaseWrapper.LastIDCorrectionForCentralDatabaseBackup message)  {
			for (Database d : listDatabase) {
				if (d.hostID.equals(message.getHostDestination())) {
					if (d.getDbwrapper().getSynchronizer().isInitializedWithCentralBackup()) {
						d.getReceivedCentralDatabaseBackupEvents().add(message);
					}
					else
					{
						Map<DecentralizedValue, Long> m=lastCorrectedTransactionsID.get(message.getHostDestination());
						if (m==null)
							lastCorrectedTransactionsID.put(message.getHostDestination(), m=new HashMap<>());
						m.put(message.getHostSource(), message.getLastValidatedTransaction());
					}
					break;
				}
			}
		}




		/*final Map<DecentralizedValue, Map<DecentralizedValue, Long>> lastValidatedTransactionsID=new HashMap<>();
		final Map<DecentralizedValue, Map<DecentralizedValue, Long>> lastCorrectedTransactionsID=new HashMap<>();
		private final Map<String, Map<DecentralizedValue, Long>> lastBackupsTimeStamps=new HashMap<>();
		private final Map<String, Map<DecentralizedValue, ArrayList<DatabaseWrapper.TransactionsInterval>>> lastBackupsInterval=new HashMap<>();*/



		public void receiveMessage(DatabaseWrapper.DatabaseBackupToIncorporateFromCentralDatabaseBackup message) throws IOException {
			assert message!=null;
			FileTools.copy(message.getSourceFile(), getFile(message.getHostSource(),message.getConcernedPackage(), message.getSourceFile()), true);
			Map<DecentralizedValue, Long> map=lastBackupsTimeStamps.get(message.getConcernedPackage());
			if (map==null)
				lastBackupsTimeStamps.put(message.getConcernedPackage(), map=new HashMap<>());
			map.put(message.getHostSource(), message.getBackupTimeStamp());
			Map<DecentralizedValue, ArrayList<DatabaseWrapper.TransactionsInterval>> map2=lastBackupsInterval.get(message.getConcernedPackage());
			if (map2==null)
				lastBackupsInterval.put(message.getConcernedPackage(), map2=new HashMap<>());
			ArrayList<DatabaseWrapper.TransactionsInterval> l=map2.get(message.getHostSource());
			if (l==null)
				map2.put(message.getHostSource(), l=new ArrayList<>());
			l.add(message.getInterval());

			for (Database d : listDatabase)
			{
				if (!d.getDbwrapper().getSynchronizer().isInitializedWithCentralBackup())
					continue;
				if (d.hostID.equals(message.getHostSource()))
				{
					d.getReceivedCentralDatabaseBackupEvents().add(new DatabaseWrapper.TransactionInUTCConfirmationEventsWithCentralDatabaseBackupEvent(message.getHostSource(), message.getBackupTimeStamp(), message.getConcernedPackage()));
				}
				else
				{
					Long tid=getLastValidatedTransactionID(message.hostIDSource);
					if (tid!=null)
						d.getReceivedCentralDatabaseBackupEvents().add(new DatabaseWrapper.TransactionInIDConfirmationEventsWithCentralDatabaseBackupEvent(message.getHostSource(), d.hostID, tid));
				}
			}
		}

		public Long getLastValidatedTransactionID(DecentralizedValue hostID)
		{
			Long res=null;
			for (Map.Entry<String, Map<DecentralizedValue, ArrayList<DatabaseWrapper.TransactionsInterval>>> e : lastBackupsInterval.entrySet())
			{
				ArrayList<DatabaseWrapper.TransactionsInterval> l=e.getValue().get(hostID);
				if (l!=null)
				{
					for (int i=l.size()-1;i>=0;i--)
					{
						DatabaseWrapper.TransactionsInterval ti=l.get(i);
						if (ti!=null)
						{
							if (res==null)
								res=ti.getEndIncludedTransactionID();
							else
								res=Math.max(res, ti.getEndIncludedTransactionID());
						}
					}
				}
			}
			return res;

		}
		public void connectHooksBetweenThem() throws DatabaseException {
			for (Database d : listDatabase)
			{
				if (d.getDbwrapper().getSynchronizer().isInitializedWithCentralBackup())
				{
					for (Database d2 : listDatabase)
					{

						if (d2!=d && d2.getDbwrapper().getSynchronizer().isInitializedWithCentralBackup())
						{
							if (!d2.dbwrapper.getSynchronizer().isInitializedWithCentralBackup(d.getHostID())) {
								Map<DecentralizedValue, Long> m=lastValidatedTransactionsID.get(d.getHostID());
								if (m!=null) {
									d2.getDbwrapper().getSynchronizer().initDistantBackupCenter(d.getHostID(),
											m.get(d2.getHostID()));
								}
								else
									d2.getDbwrapper().getSynchronizer().initDistantBackupCenter(d.getHostID(),
											Long.MIN_VALUE);

							}
							if (!d.dbwrapper.getSynchronizer().isInitializedWithCentralBackup(d2.getHostID())) {
								Map<DecentralizedValue, Long> m=lastValidatedTransactionsID.get(d2.getHostID());
								if (m!=null) {
									d.getDbwrapper().getSynchronizer().initDistantBackupCenter(d2.getHostID(), m.get(d.getHostID()));
								}
								else
									d.getDbwrapper().getSynchronizer().initDistantBackupCenter(d2.getHostID(), Long.MIN_VALUE);

							}
						}
					}
				}
			}
		}
		public void connect(Database d) throws DatabaseException {
			Map<String, Long> map=new HashMap<>();
			for (Map.Entry<String, Map<DecentralizedValue, Long>> e : lastBackupsTimeStamps.entrySet())
			{
				for (Map.Entry<DecentralizedValue, Long> e2 : e.getValue().entrySet())
				{
					if (e2.getKey().equals(d.hostID))
					{
						map.put(e.getKey(), e2.getValue());
					}
				}
			}
			d.getDbwrapper().getSynchronizer().initDistantBackupCenterForThisHostWithStringPackages(map);
			Map<DecentralizedValue, Long> m=lastCorrectedTransactionsID.get(d.hostID);
			if (m!=null)
			{
				for (Map.Entry<DecentralizedValue, Long> e : m.entrySet())
				{
					d.getReceivedCentralDatabaseBackupEvents().add(new DatabaseWrapper.LastIDCorrectionForCentralDatabaseBackup(e.getKey(), d.hostID, e.getValue()));
				}
				lastCorrectedTransactionsID.remove(d.hostID);
			}
		}


		public void disconnect(Database d) throws DatabaseException {
			d.getDbwrapper().getSynchronizer().disconnectAllHooksFromThereBackups();
			/*for (Database d2 : listDatabase) {
				if (d2 != d && d2.isConnected()) {
					d2.getDbwrapper().getSynchronizer().disconnectHookFromItsBackup(d.hostID);
				}
			}*/
		}

		public void receiveMessage(DatabaseWrapper.DatabaseBackupToRemoveIntoCentralDatabaseBackup message)
		{
			this.lastBackupsInterval.remove(message.getPackageName());
			this.lastBackupsTimeStamps.remove(message.getPackageName());
		}


		public void receiveMessage(DatabaseWrapper.TransactionInIDConfirmationEventsWithCentralDatabaseBackupEvent message) {
			Map<DecentralizedValue, Long> m=lastValidatedTransactionsID.get(message.getHostSource());
			if (m==null)
				lastValidatedTransactionsID.put(message.getHostSource(), m=new HashMap<>());
			m.put(message.getHostDestination(), message.getLastValidatedTransaction());
			for (Database d : listDatabase) {
				if (d.hostID.equals(message.getHostDestination())) {
					if (d.getDbwrapper().getSynchronizer().isInitializedWithCentralBackup()) {
						d.getReceivedCentralDatabaseBackupEvents().add(message);
					}
					break;
				}
			}
		}

		public void receiveMessage(CentralDatabaseBackupEvent message) throws IOException, DatabaseException {
			if (message instanceof DatabaseWrapper.DatabaseBackupToIncorporateFromCentralDatabaseBackup)
			{
				receiveMessage((DatabaseWrapper.DatabaseBackupToIncorporateFromCentralDatabaseBackup)message);
			}
			else if (message instanceof DatabaseWrapper.DatabaseTransactionsIdentifiersToSynchronizeWithCentralDatabaseBackup)
			{
				receiveMessage((DatabaseWrapper.DatabaseTransactionsIdentifiersToSynchronizeWithCentralDatabaseBackup)message);
			}
			else if (message instanceof DatabaseWrapper.LastIDCorrectionForCentralDatabaseBackup)
			{
				receiveMessage((DatabaseWrapper.LastIDCorrectionForCentralDatabaseBackup)message);
			}
			else if (message instanceof DatabaseWrapper.TransactionInIDConfirmationEventsWithCentralDatabaseBackupEvent)
			{
				receiveMessage((DatabaseWrapper.TransactionInIDConfirmationEventsWithCentralDatabaseBackupEvent)message);
			}
			else if (message instanceof DatabaseWrapper.DatabaseBackupToRemoveIntoCentralDatabaseBackup)
			{
				receiveMessage((DatabaseWrapper.DatabaseBackupToRemoveIntoCentralDatabaseBackup)message);
			}
			else if(message instanceof DatabaseWrapper.AskToDatabaseBackupCenterForSynchronization)
			{
				receiveMessage((DatabaseWrapper.AskToDatabaseBackupCenterForSynchronization)message);
			}
		}
	}



	public static class Database implements AutoCloseable, DatabaseNotifier {
		private final DatabaseWrapper dbwrapper;
		private volatile boolean connected;
		private final AbstractDecentralizedID hostID;
		private final ArrayList<DatabaseEvent> localEvents;
		private final List<CommonDecentralizedTests.DistantDatabaseEvent> eventsReceivedStack;
		private TableAlone tableAlone;
		private TablePointed tablePointed;
		private TablePointing tablePointing;
		private UndecentralizableTableA1 undecentralizableTableA1;
		private UndecentralizableTableB1 undecentralizableTableB1;
		private volatile boolean newDatabaseEventDetected = false;
		private volatile boolean replaceWhenCollisionDetected = false;
		private volatile CommonDecentralizedTests.DetectedCollision collisionDetected = null;

		private volatile TablePointed.Record recordPointed = null;
		private TablePointing.Record recordPointingNull = null, recordPointingNotNull = null;
		private TableAlone.Record recordAlone = null;
		private final List<CommonDecentralizedTests.Anomaly> anomalies;
		private final List<MessageComingFromCentralDatabaseBackup> centralDatabaseBackupEvents=new ArrayList<>();

		Database(DatabaseWrapper dbwrapper, BackupConfiguration backupConfiguration) throws DatabaseException {
			this.dbwrapper = dbwrapper;
			connected = false;
			hostID = new DecentralizedIDGenerator();
			localEvents = new ArrayList<>();
			eventsReceivedStack = Collections.synchronizedList(new LinkedList<CommonDecentralizedTests.DistantDatabaseEvent>());

			getDbwrapper()
					.loadDatabase(new DatabaseConfiguration(TableAlone.class.getPackage(), new DatabaseLifeCycles() {


						@Override
						public void transferDatabaseFromOldVersion(DatabaseWrapper wrapper, DatabaseConfiguration oldDatabaseConfiguration, DatabaseConfiguration newDatabaseConfiguration) {

						}

						@Override
						public void afterDatabaseCreation(DatabaseWrapper wrapper, DatabaseConfiguration newDatabaseConfiguration) {

						}

						@Override
						public boolean hasToRemoveOldDatabase() {
							return false;
						}

						@Override
						public boolean replaceDistantConflictualRecordsWhenDistributedDatabaseIsResynchronized() {
							return false;
						}
					}, null, backupConfiguration), true);

			tableAlone = dbwrapper.getTableInstance(TableAlone.class);
			tablePointed = dbwrapper.getTableInstance(TablePointed.class);
			tablePointing = dbwrapper.getTableInstance(TablePointing.class);
			undecentralizableTableA1 = dbwrapper
					.getTableInstance(UndecentralizableTableA1.class);
			undecentralizableTableB1 = dbwrapper
					.getTableInstance(UndecentralizableTableB1.class);
			anomalies = Collections.synchronizedList(new ArrayList<CommonDecentralizedTests.Anomaly>());

			tableAlone.setDatabaseAnomaliesNotifier(new DatabaseAnomaliesNotifier<TableAlone.Record, Table<TableAlone.Record>>() {
				@Override
				public void anomalyDetected(DecentralizedValue distantPeerID, DecentralizedValue intermediatePeerID, DatabaseWrapper.SynchronizationAnomalyType type, Table<TableAlone.Record> concernedTable, Map<String, Object> primary_keys, TableAlone.Record record) {
					CommonDecentralizedTests.Database.this.anomalyDetected( distantPeerID, intermediatePeerID, type, concernedTable, primary_keys, record);
				}
			});
			tableAlone.setDatabaseCollisionsNotifier(new DatabaseCollisionsNotifier<TableAlone.Record, Table<TableAlone.Record>>() {
				@Override
				public boolean collisionDetected(DecentralizedValue distantPeerID, DecentralizedValue intermediatePeerID, DatabaseEventType type, Table<TableAlone.Record> concernedTable, HashMap<String, Object> keys, TableAlone.Record newValues, TableAlone.Record actualValues) {
					return CommonDecentralizedTests.Database.this.collisionDetected(distantPeerID, intermediatePeerID, type, concernedTable, keys, newValues, actualValues);
				}

				@Override
				public boolean areDuplicatedEventsNotConsideredAsCollisions() {
					return false;
				}
			});
			tablePointed.setDatabaseAnomaliesNotifier(new DatabaseAnomaliesNotifier<TablePointed.Record, Table<TablePointed.Record>>() {
				@Override
				public void anomalyDetected(DecentralizedValue distantPeerID, DecentralizedValue intermediatePeerID, DatabaseWrapper.SynchronizationAnomalyType type, Table<TablePointed.Record> concernedTable, Map<String, Object> primary_keys, TablePointed.Record record) {
					CommonDecentralizedTests.Database.this.anomalyDetected( distantPeerID, intermediatePeerID, type, concernedTable, primary_keys, record);
				}
			});
			tablePointed.setDatabaseCollisionsNotifier(new DatabaseCollisionsNotifier<TablePointed.Record, Table<TablePointed.Record>>() {
				@Override
				public boolean collisionDetected(DecentralizedValue distantPeerID, DecentralizedValue intermediatePeerID, DatabaseEventType type, Table<TablePointed.Record> concernedTable, HashMap<String, Object> keys, TablePointed.Record newValues, TablePointed.Record actualValues) {
					return CommonDecentralizedTests.Database.this.collisionDetected(distantPeerID, intermediatePeerID, type, concernedTable, keys, newValues, actualValues);
				}
				@Override
				public boolean areDuplicatedEventsNotConsideredAsCollisions() {
					return false;
				}

			});
			tablePointing.setDatabaseAnomaliesNotifier(new DatabaseAnomaliesNotifier<TablePointing.Record, Table<TablePointing.Record>>() {
				@Override
				public void anomalyDetected(DecentralizedValue distantPeerID, DecentralizedValue intermediatePeerID, DatabaseWrapper.SynchronizationAnomalyType type, Table<TablePointing.Record> concernedTable, Map<String, Object> primary_keys, TablePointing.Record record) {
					CommonDecentralizedTests.Database.this.anomalyDetected( distantPeerID, intermediatePeerID, type, concernedTable, primary_keys, record);
				}
			});
			tablePointing.setDatabaseCollisionsNotifier(new DatabaseCollisionsNotifier<TablePointing.Record, Table<TablePointing.Record>>() {
				@Override
				public boolean collisionDetected(DecentralizedValue distantPeerID, DecentralizedValue intermediatePeerID, DatabaseEventType type, Table<TablePointing.Record> concernedTable, HashMap<String, Object> keys, TablePointing.Record newValues, TablePointing.Record actualValues) {
					return CommonDecentralizedTests.Database.this.collisionDetected(distantPeerID, intermediatePeerID, type, concernedTable, keys, newValues, actualValues);

				}
				@Override
				public boolean areDuplicatedEventsNotConsideredAsCollisions() {
					return false;
				}

			});
		}

		public TableAlone getTableAlone() {
			return tableAlone;
		}

		public TablePointed getTablePointed() {
			return tablePointed;
		}

		public TablePointing getTablePointing() {
			return tablePointing;
		}

		public UndecentralizableTableA1 getUndecentralizableTableA1() {
			return undecentralizableTableA1;
		}

		public UndecentralizableTableB1 getUndecentralizableTableB1() {
			return undecentralizableTableB1;
		}

		public boolean isConnected() {
			return connected;
		}

		public void setConnected(boolean _connected) {
			connected = _connected;
		}

		public DatabaseWrapper getDbwrapper() {
			return dbwrapper;
		}

		@Override
		public void close() {
			dbwrapper.close();
		}

		public AbstractDecentralizedID getHostID() {
			return hostID;
		}

		public ArrayList<DatabaseEvent> getLocalEvents() {
			return localEvents;
		}

		public void clearPendingEvents() {
			synchronized (CommonDecentralizedTests.class) {
				localEvents.clear();
				this.eventsReceivedStack.clear();
				newDatabaseEventDetected = false;
				collisionDetected = null;
				anomalies.clear();
			}
		}

		public boolean isNewDatabaseEventDetected() {
			return newDatabaseEventDetected;
		}

		public void setNewDatabaseEventDetected(boolean newDatabaseEventDetected) {
			this.newDatabaseEventDetected = newDatabaseEventDetected;
		}

		public List<CommonDecentralizedTests.DistantDatabaseEvent> getReceivedDatabaseEvents() {
			return this.eventsReceivedStack;
		}

		public List<MessageComingFromCentralDatabaseBackup> getReceivedCentralDatabaseBackupEvents() {
			return this.centralDatabaseBackupEvents;
		}

		@Override
		public void newDatabaseEventDetected(DatabaseWrapper _wrapper) {
			newDatabaseEventDetected = true;
		}

		@Override
		public void startNewSynchronizationTransaction() {

		}

		@Override
		public void endSynchronizationTransaction() {

		}

		@Override
		public void hostDisconnected(DecentralizedValue hostID) {

		}

		@Override
		public void hostConnected(DecentralizedValue hostID) {

		}


		private boolean collisionDetected(DecentralizedValue _distantPeerID,
										  DecentralizedValue _intermediatePeer, DatabaseEventType _type, Table<?> _concernedTable,
										  HashMap<String, Object> _keys, DatabaseRecord _newValues, DatabaseRecord _actualValues) {
			synchronized (CommonDecentralizedTests.class) {
				collisionDetected = new CommonDecentralizedTests.DetectedCollision(_distantPeerID, _intermediatePeer, _type, _concernedTable,
						_keys, _newValues, _actualValues);
				return replaceWhenCollisionDetected;
			}
		}

		public void setReplaceWhenCollisionDetected(boolean _replaceWhenCollisionDetected) {
			replaceWhenCollisionDetected = _replaceWhenCollisionDetected;
		}

		public boolean isReplaceWhenCollisionDetected() {
			return replaceWhenCollisionDetected;
		}

		public CommonDecentralizedTests.DetectedCollision getDetectedCollision() {
			return collisionDetected;
		}

		public TablePointed.Record getRecordPointed() {
			return recordPointed;
		}

		public void setRecordPointed(TablePointed.Record _recordPointed) {
			recordPointed = _recordPointed;
		}

		public TablePointing.Record getRecordPointingNull() {
			return recordPointingNull;
		}

		public TablePointing.Record getRecordPointingNotNull() {
			return recordPointingNotNull;
		}

		public void setRecordPointingNull(TablePointing.Record _recordPointing) {
			recordPointingNull = _recordPointing;
		}

		public void setRecordPointingNotNull(TablePointing.Record _recordPointing) {
			recordPointingNotNull = _recordPointing;
		}

		public TableAlone.Record getRecordAlone() {
			return recordAlone;
		}

		public void setRecordAlone(TableAlone.Record _recordAlone) {
			recordAlone = _recordAlone;
		}

		public List<CommonDecentralizedTests.Anomaly> getAnomalies() {
			return anomalies;
		}


		private void anomalyDetected(DecentralizedValue distantPeerID, DecentralizedValue intermediatePeerID,
									 DatabaseWrapper.SynchronizationAnomalyType _type, Table<?> _concernedTable, Map<String, Object> _primary_keys,
									 DatabaseRecord _record) {
			anomalies.add(new CommonDecentralizedTests.Anomaly(distantPeerID, intermediatePeerID, _type, _concernedTable, _primary_keys, _record));
		}


	}

	public static class Anomaly {

		final DecentralizedValue distantPeerID;
		final DecentralizedValue intermediatePeerID;
		final Table<?> table;
		final Map<String, Object> keys;
		final DatabaseRecord record;
		final DatabaseWrapper.SynchronizationAnomalyType type;

		public Anomaly(DecentralizedValue _distantPeerID, DecentralizedValue _intermediatePeerID, DatabaseWrapper.SynchronizationAnomalyType type,
					   Table<?> _table, Map<String, Object> _keys, DatabaseRecord _record) {
			super();
			distantPeerID = _distantPeerID;
			intermediatePeerID = _intermediatePeerID;
			this.type=type;
			table = _table;
			keys = _keys;
			record = _record;
		}

		@Override
		public String toString()
		{
			return "Anomaly-"+type;
		}
	}

	public static class DetectedCollision {
		final DecentralizedValue distantPeerID;
		final DecentralizedValue intermediatePeer;
		final DatabaseEventType type;
		final Table<?> concernedTable;
		final HashMap<String, Object> keys;
		final DatabaseRecord newValues;
		final DatabaseRecord actualValues;

		public DetectedCollision(DecentralizedValue _distantPeerID, DecentralizedValue _intermediatePeer,
								 DatabaseEventType _type, Table<?> _concernedTable, HashMap<String, Object> _keys,
								 DatabaseRecord _newValues, DatabaseRecord _actualValues) {
			super();
			distantPeerID = _distantPeerID;
			intermediatePeer = _intermediatePeer;
			type = _type;
			concernedTable = _concernedTable;
			keys = _keys;
			newValues = _newValues;
			actualValues = _actualValues;
		}

	}



	protected volatile CommonDecentralizedTests.Database db1 = null, db2 = null, db3 = null, db4 = null;
	protected final ArrayList<CommonDecentralizedTests.Database> listDatabase = new ArrayList<>(3);
	protected final CentralDatabaseBackup centralDatabaseBackup=new CentralDatabaseBackup();

	public abstract DatabaseWrapper getDatabaseWrapperInstance1() throws IllegalArgumentException, DatabaseException;

	public abstract DatabaseWrapper getDatabaseWrapperInstance2() throws IllegalArgumentException, DatabaseException;

	public abstract DatabaseWrapper getDatabaseWrapperInstance3() throws IllegalArgumentException, DatabaseException;

	public abstract DatabaseWrapper getDatabaseWrapperInstance4() throws IllegalArgumentException, DatabaseException;

	public abstract void removeDatabaseFiles1();

	public abstract void removeDatabaseFiles2();

	public abstract void removeDatabaseFiles3();

	public abstract void removeDatabaseFiles4();

	public BackupConfiguration getBackupConfiguration()
	{
		return null;
	}
	public boolean canInitCentralBackup()
	{
		return false;
	}

	@BeforeClass
	public void loadDatabase() throws DatabaseException {
		unloadDatabase();
		db1 = new CommonDecentralizedTests.Database(getDatabaseWrapperInstance1(), getBackupConfiguration());
		db2 = new CommonDecentralizedTests.Database(getDatabaseWrapperInstance2(), getBackupConfiguration());
		db3 = new CommonDecentralizedTests.Database(getDatabaseWrapperInstance3(), getBackupConfiguration());
		listDatabase.add(db1);
		listDatabase.add(db2);
		listDatabase.add(db3);
		Assert.assertTrue(db1.getDbwrapper().getTableInstance(TableAlone.class).isLocallyDecentralizable());
		Assert.assertTrue(db1.getDbwrapper().getTableInstance(TablePointed.class).isLocallyDecentralizable());
		Assert.assertTrue(db1.getDbwrapper().getTableInstance(TablePointing.class).isLocallyDecentralizable());
		Assert.assertFalse(db1.getDbwrapper().getTableInstance(UndecentralizableTableA1.class).isLocallyDecentralizable());
		//Assert.assertFalse(db1.getDbwrapper().getTableInstance(UndecentralizableTableB1.class).isLocallyDecentralizable());
	}

	public void unloadDatabase1() {
		if (db1 != null) {
			try {
				db1.close();
			} finally {
				db1 = null;
			}
		}
		removeDatabaseFiles1();
	}

	public void unloadDatabase2() {
		if (db2 != null) {
			try {
				db2.close();
			} finally {
				db2 = null;
			}
		}
		removeDatabaseFiles2();
	}

	public void unloadDatabase3() {
		if (db3 != null) {
			try {
				db3.close();
			} finally {
				db3 = null;
			}
		}
		removeDatabaseFiles3();
	}

	public void unloadDatabase4() {
		if (db4 != null) {
			try {
				db4.close();
				listDatabase.remove(db4);
			} finally {
				db4 = null;
			}
		}
		removeDatabaseFiles4();
	}

	@AfterClass
	public void unloadDatabase() {
		try {
			unloadDatabase1();
		} finally {
			try {
				unloadDatabase2();
			} finally {
				try {
					unloadDatabase3();
				} finally {
					try {
						unloadDatabase4();
					} finally {
						listDatabase.clear();
						if (centralDatabaseBackupDirectory.exists())
							FileTools.deleteDirectory(centralDatabaseBackupDirectory);
					}
				}
			}
		}

	}

	@SuppressWarnings("deprecation")
	@Override
	public void finalize() {
		unloadDatabase();
	}

	@AfterMethod
	public void cleanPendedEvents() {
		synchronized (CommonDecentralizedTests.class) {
			for (CommonDecentralizedTests.Database db : listDatabase) {
				db.clearPendingEvents();
			}
		}
	}

	protected void sendCentralDatabaseBackupEvent(CommonDecentralizedTests.Database db, CentralDatabaseBackupEvent event) {
		if (db.isConnected() && db.dbwrapper.getSynchronizer().isInitializedWithCentralBackup())
			db.getReceivedCentralDatabaseBackupEvents().add(event);
		else
			Assert.fail();
	}

	protected void sendDistantDatabaseEvent(CommonDecentralizedTests.DistantDatabaseEvent event) {
		for (CommonDecentralizedTests.Database db : listDatabase) {
			if (db.getHostID().equals(event.getHostDestination())) {
				if (db.isConnected())
					db.getReceivedDatabaseEvents().add(event);
				else
					Assert.fail();
				break;
			}
		}
	}

	protected boolean checkMessages(CommonDecentralizedTests.Database db) throws Exception {
		boolean changed = false;
		while (!db.getReceivedDatabaseEvents().isEmpty()) {
			changed = true;
			final CommonDecentralizedTests.DistantDatabaseEvent dde = db.getReceivedDatabaseEvents().remove(0);

			DBEventToSend event = dde.getDatabaseEventToSend();

			if (event instanceof BigDatabaseEventToSend) {
				try (InputStreamGetter is = new InputStreamGetter() {

					private RandomInputStream actual=null;

					@Override
					public RandomInputStream initOrResetInputStream() throws IOException {
						if (actual!=null)
							actual.close();
						return actual=dde.getInputStream();
					}

					@Override
					public void close() throws IOException {
						if (actual!=null)
							actual.close();
					}
				})
				{
					db.getDbwrapper().getSynchronizer().received((BigDatabaseEventToSend) event, is);
				}
			} else {
				db.getDbwrapper().getSynchronizer().received(event);
			}
		}
		while (!db.getReceivedCentralDatabaseBackupEvents().isEmpty())
		{
			changed=true;
			CentralDatabaseBackupEvent event = db.getReceivedCentralDatabaseBackupEvents().remove(0);
			if (event instanceof BigDatabaseEventToSend) {
				try (InputStreamGetter is = new InputStreamGetter() {

					private RandomInputStream actual=null;

					@Override
					public RandomInputStream initOrResetInputStream() throws IOException {
						if (actual!=null)
							actual.close();
						return actual=dde.getInputStream();
					}

					@Override
					public void close() throws IOException {
						if (actual!=null)
							actual.close();
					}
				})
				{
					db.getDbwrapper().getSynchronizer().received((BigDatabaseEventToSend) event, is);
				}
			} else {
				db.getDbwrapper().getSynchronizer().received(event);
			}

			db.dbwrapper.getSynchronizer().received(event);
		}
		return changed;
	}

	protected void checkForNewDatabaseBackupBlocks() throws DatabaseException {
		for (Database d : listDatabase)
			if (d.dbwrapper.getSynchronizer().isInitializedWithCentralBackup())
				d.dbwrapper.getSynchronizer().checkForNewCentralBackupDatabaseEvent();
			else
				System.out.println("not initialized");
	}

	protected boolean checkMessages() throws Exception {
		synchronized (CommonDecentralizedTests.class) {
			boolean changed = false;
			for (CommonDecentralizedTests.Database db : listDatabase) {
				changed |= checkMessages(db);
			}
			return changed;
		}
	}

	protected void exchangeMessages() throws Exception {
		synchronized (CommonDecentralizedTests.class) {
			boolean loop = true;
			while (loop) {
				loop = false;
				for (CommonDecentralizedTests.Database db : listDatabase) {

					DatabaseEvent e = db.getDbwrapper().getSynchronizer().nextEvent();
					if (e != null) {

						loop = true;
						if (e instanceof CentralDatabaseBackupEvent)
						{
							System.out.println("new event : "+e);
							centralDatabaseBackup.receiveMessage((CentralDatabaseBackupEvent)e);
						}
						else if (e instanceof DBEventToSend) {
							DBEventToSend es = (DBEventToSend) e;

							Assert.assertEquals(es.getHostSource(), db.getHostID());
							Assert.assertNotEquals(es.getHostDestination(), db.getHostID());
							if (db.isConnected()) {
								sendDistantDatabaseEvent(new CommonDecentralizedTests.DistantDatabaseEvent(db.getDbwrapper(), es));
							} else
								Assert.fail();// TODO really ?
						} else {
							db.getLocalEvents().add(e);
						}
					}
				}
				loop |= checkMessages();
			}
		}
	}

	protected void checkAllDatabaseInternalDataUsedForSynchro() throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase) {
			checkDatabaseInternalDataUsedForSynchro(db);
		}
	}

	protected void checkDatabaseInternalDataUsedForSynchro(CommonDecentralizedTests.Database db) throws DatabaseException {
		synchronized (CommonDecentralizedTests.class) {
			Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionsPerHostTable().getRecords().size(), 0);

			Assert.assertEquals(db.getDbwrapper().getTransactionsTable().getRecords().size(), 0);
			Assert.assertEquals(db.getDbwrapper().getDatabaseEventsTable().getRecords().size(), 0);
			Assert.assertEquals(db.getDbwrapper().getHooksTransactionsTable().getRecords().size(), listDatabase.size());
			Assert.assertEquals(db.getDbwrapper().getDatabaseDistantTransactionEvent().getRecords().size(), 0);
			Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionEventsTable().getRecords().size(), 0);
		}

	}

	protected boolean sendIndirectTransactions()
	{
		return true;
	}

	protected void connectLocal(CommonDecentralizedTests.Database db) throws DatabaseException {
		synchronized (CommonDecentralizedTests.class) {
			if (!db.isConnected()) {
				db.getDbwrapper().getSynchronizer().initLocalHostID(db.getHostID(), sendIndirectTransactions());
				if (canInitCentralBackup())
					centralDatabaseBackup.connect(db);
				db.setConnected(true);
			}
		}
	}

	protected void connectDistant(CommonDecentralizedTests.Database db, CommonDecentralizedTests.Database... listDatabase) throws DatabaseException {

		synchronized (CommonDecentralizedTests.class) {
			if (db.isConnected()) {
				for (CommonDecentralizedTests.Database otherdb : listDatabase) {
					if (otherdb != db && otherdb.isConnected()) {
						db.getDbwrapper().getSynchronizer().initHook(otherdb.getHostID(), otherdb.getDbwrapper()
								.getSynchronizer().getLastValidatedSynchronization(db.getHostID()));
						// otherdb.getDbwrapper().getSynchronizer().initHook(db.getHostID(),
						// db.getDbwrapper().getSynchronizer().getLastValidatedSynchronization(otherdb.getHostID()));
					}
				}
			}
		}

	}

	protected void connectSelectedDatabase(CommonDecentralizedTests.Database... listDatabase)
			throws Exception {
		for (CommonDecentralizedTests.Database db : listDatabase) {
			connectLocal(db);
		}
		for (CommonDecentralizedTests.Database db : listDatabase) {
			connectDistant(db, listDatabase);
		}
		exchangeMessages();
	}

	protected void connectAllDatabase() throws Exception {
		CommonDecentralizedTests.Database[] dbs = new CommonDecentralizedTests.Database[listDatabase.size()];
		for (int i = 0; i < dbs.length; i++)
			dbs[i] = listDatabase.get(i);
		for (CommonDecentralizedTests.Database db : listDatabase) {
			connectLocal(db);
		}
		for (CommonDecentralizedTests.Database db : listDatabase) {
			connectDistant(db, dbs);
		}
		exchangeMessages();
	}

	protected void connectCentralDatabaseBackupWithConnectedDatabase() throws DatabaseException {
		for (Database d : listDatabase)
		{
			centralDatabaseBackup.connect(d);
		}
		centralDatabaseBackup.connectHooksBetweenThem();
	}

	protected void addDatabasePackageToSynchronizeWithCentralDatabaseBackup(Package _package) throws Exception {
		connectAllDatabase();
		listDatabase.get(0).getDbwrapper().getSynchronizer().addDatabasePackageToSynchronizeWithCentralBackup(_package);
		exchangeMessages();
	}

	protected void disconnectCentralDatabaseBakcup() throws DatabaseException {
		for (Database d : listDatabase)
		{
			if (d.dbwrapper.getSynchronizer().isInitializedWithCentralBackup())
				d.dbwrapper.getSynchronizer().disconnectAllHooksFromThereBackups();
		}
	}

	protected void disconnect(CommonDecentralizedTests.Database db, CommonDecentralizedTests.Database... listDatabase) throws DatabaseException {
		synchronized (CommonDecentralizedTests.class) {
			if (db.isConnected()) {
				db.setConnected(false);
				if (db.dbwrapper.getSynchronizer().isInitializedWithCentralBackup())
					db.dbwrapper.getSynchronizer().disconnectAllHooksFromThereBackups();
				db.getDbwrapper().getSynchronizer().disconnectHook(db.getHostID());
				for (CommonDecentralizedTests.Database dbother : listDatabase) {
					if (dbother != db && dbother.isConnected())
						dbother.getDbwrapper().getSynchronizer().disconnectHook(db.getHostID());
				}
				Assert.assertFalse(db.isConnected());
			}
		}
	}

	protected void disconnectSelectedDatabase(CommonDecentralizedTests.Database... listDatabase) throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase)
			disconnect(db, listDatabase);
	}

	protected void disconnectAllDatabase() throws DatabaseException {
		CommonDecentralizedTests.Database[] dbs = new CommonDecentralizedTests.Database[listDatabase.size()];
		for (int i = 0; i < dbs.length; i++)
			dbs[i] = listDatabase.get(i);
		disconnectSelectedDatabase(dbs);
	}

	protected TableAlone.Record generatesTableAloneRecord() throws DatabaseException {
		TableAlone.Record ralone = new TableAlone.Record();
		ralone.id = new DecentralizedIDGenerator();
		try {
			ralone.id2 = ASymmetricAuthenticatedSignatureType.BC_FIPS_Ed25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair().getASymmetricPublicKey();
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
		ralone.value = generateString();
		return ralone;

	}

	@SuppressWarnings("SameParameterValue")
	protected void addTableAloneRecord(CommonDecentralizedTests.Database db, boolean first) throws DatabaseException {
		TableAlone.Record ralone = generatesTableAloneRecord();

		db.getTableAlone().addRecord(((Table<TableAlone.Record>) db.getTableAlone()).getMap(ralone, true, true));
		if (first)
			db.setRecordAlone(ralone);
	}

	protected void addUndecentralizableTableA1Record(CommonDecentralizedTests.Database db) throws DatabaseException {
		UndecentralizableTableA1.Record record = new UndecentralizableTableA1.Record();
		StringBuilder sb=new StringBuilder(db.getHostID().toString());
		for (int i = 0; i < 10; i++) {
			sb.append('a' + ((int) (Math.random() * 52)));
		}
		record.value=sb.toString();
		db.getUndecentralizableTableA1().addRecord(record);

	}

	protected void addUndecentralizableTableB1Record(CommonDecentralizedTests.Database db) throws DatabaseException {
		UndecentralizableTableA1.Record record = new UndecentralizableTableA1.Record();
		StringBuilder sb=new StringBuilder(db.getHostID().toString());

		for (int i = 0; i < 10; i++) {
			sb.append( 'a' + ((int) (Math.random() * 52)));
		}
		record.value=sb.toString();
		record = db.getUndecentralizableTableA1().addRecord(record);
		UndecentralizableTableB1.Record record2 = new UndecentralizableTableB1.Record();
		record2.pointing = record;
		db.getUndecentralizableTableB1().addRecord(record2);
	}

	protected String generateString() {
		StringBuilder res = new StringBuilder();
		for (int i = 0; i < 10; i++) {
			res.append('a' + ((int) (Math.random() * 52)));
		}
		return res.toString();
	}

	protected TablePointed.Record generatesTablePointedRecord() {
		TablePointed.Record rpointed = new TablePointed.Record();
		rpointed.id = new DecentralizedIDGenerator();
		rpointed.value = generateString();
		return rpointed;
	}

	protected TablePointing.Record generatesTablePointingRecord(TablePointed.Record rpointed) throws DatabaseException {
		TablePointing.Record rpointing1 = new TablePointing.Record();
		try {
			rpointing1.id = ASymmetricAuthenticatedSignatureType.BC_FIPS_Ed25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair().getASymmetricPublicKey();
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
		rpointing1.table2 = Math.random() < 0.5 ? null : rpointed;
		return rpointing1;
	}

	@SuppressWarnings("SameParameterValue")
	protected void addTablePointedAndPointingRecords(CommonDecentralizedTests.Database db, boolean first) throws DatabaseException {
		TablePointed.Record rpointed = new TablePointed.Record();
		rpointed.id = new DecentralizedIDGenerator();
		rpointed.value = generateString();

		rpointed = db.getTablePointed().addRecord(rpointed);

		TablePointing.Record rpointing1 = new TablePointing.Record();
		try {
			rpointing1.id = ASymmetricAuthenticatedSignatureType.BC_FIPS_Ed25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair().getASymmetricPublicKey();
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
		}

		rpointing1.table2 = null;
		rpointing1 = db.getTablePointing().addRecord(rpointing1);
		TablePointing.Record rpointing2 = new TablePointing.Record();
		try {
			rpointing2.id = ASymmetricAuthenticatedSignatureType.BC_FIPS_Ed25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair().getASymmetricPublicKey();
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
		rpointing2.table2 = rpointed;
		rpointing2 = db.getTablePointing().addRecord(rpointing2);
		if (first) {
			db.setRecordPointingNull(rpointing1);
			db.setRecordPointingNotNull(rpointing2);
		}
	}

	protected void addElements(CommonDecentralizedTests.Database db) throws DatabaseException {
		addTableAloneRecord(db, true);
		addTablePointedAndPointingRecords(db, true);
		addUndecentralizableTableA1Record(db);
		addUndecentralizableTableB1Record(db);
	}

	protected void addElements() throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase)
			addElements(db);
	}


	@Test
	public void testAddFirstElements() throws DatabaseException {
		addElements();
	}

	@Test(dependsOnMethods = { "testAddFirstElements" })
	public void testInit() throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase) {
			db.getDbwrapper().getSynchronizer().setNotifier(db);
			db.getDbwrapper().setMaxTransactionsToSynchronizeAtTheSameTime(5);
			db.getDbwrapper().setMaxTransactionEventsKeepedIntoMemory(3);

			db.getDbwrapper().getSynchronizer().addHookForLocalDatabaseHost(db.getHostID(),
					TablePointed.class.getPackage());
			Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized());

		}

		for (CommonDecentralizedTests.Database db : listDatabase) {
			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					HookAddRequest har = db.getDbwrapper().getSynchronizer().askForHookAddingAndSynchronizeDatabase(
							other.getHostID(), false, TablePointed.class.getPackage());
					har = other.getDbwrapper().getSynchronizer().receivedHookAddRequest(har);
					db.getDbwrapper().getSynchronizer().receivedHookAddRequest(har);
				}
			}
		}

	}

	@Test(dependsOnMethods = { "testInit" })
	public void testAllConnect() throws Exception {

		connectAllDatabase();
		exchangeMessages();
		for (CommonDecentralizedTests.Database db : listDatabase) {
			for (CommonDecentralizedTests.Database other : listDatabase) {
				Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized(other.getHostID()));
			}
		}
	}

	@Test(dependsOnMethods = {"testAllConnect"})
	public void testOldElementsAddedBeforeAddingSynchroSynchronized()
			throws Exception {
		exchangeMessages();
		testSynchronisation();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();
	}

	@Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods = {
			"testOldElementsAddedBeforeAddingSynchroSynchronized" })
	public void testSynchroBetweenTwoPeers(boolean exceptionDuringTransaction, boolean generateDirectConflict,
										   boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenPeersImpl(2, exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dependsOnMethods = { "testSynchroBetweenTwoPeers" })
	public void testSynchroAfterTestsBetweenTwoPeers() throws DatabaseException {
		testSynchronisation();
	}



	@Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods = { "testSynchroBetweenTwoPeers" })
	public void testSynchroBetweenThreePeers(boolean exceptionDuringTransaction, boolean generateDirectConflict,
											 boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenPeersImpl(3, exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dependsOnMethods = { "testSynchroBetweenThreePeers" })
	public void testSynchroAfterTestsBetweenThreePeers() throws DatabaseException {
		testSynchronisation();
	}
	/*
	 * @Test(dependsOnMethods={"testAllConnect"}) public void
	 * testInitDatabaseNetwork() throws DatabaseException { for (Database db :
	 * listDatabase) {
	 *
	 * db.getDbwrapper().getSynchronizer().addHookForLocalDatabaseHost(db.getHostID(
	 * ), TablePointed.class.getPackage()); } }
	 */

	/*
	 * @Test(dependsOnMethods={"testInit"}) public void
	 * testAddSynchroBetweenDatabase() throws DatabaseException,
	 * ClassNotFoundException, IOException { for (Database db : listDatabase) { for
	 * (Database otherdb : listDatabase) { if (otherdb!=db) {
	 * db.getDbwrapper().getSynchronizer().addHookForDistantHost(otherdb.getHostID()
	 * ,TablePointed.class.getPackage());
	 * otherdb.getDbwrapper().getSynchronizer().addHookForDistantHost(db.getHostID()
	 * ,TablePointed.class.getPackage()); } } } exchangeMessages(); }
	 */

	protected void testSynchronisation(CommonDecentralizedTests.Database db) throws DatabaseException {

		for (TableAlone.Record r : db.getTableAlone().getRecords()) {

			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					TableAlone.Record otherR = other.getTableAlone().getRecord("id", r.id, "id2", r.id2);
					Assert.assertNotNull(otherR);
					Assert.assertEquals(otherR.value, r.value);
				}
			}
		}
		for (TablePointed.Record r : db.getTablePointed().getRecords()) {
			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					TablePointed.Record otherR = other.getTablePointed().getRecord("id", r.id);
					Assert.assertNotNull(otherR);
					Assert.assertEquals(otherR.value, r.value);
				}
			}
		}
		for (TablePointing.Record r : db.getTablePointing().getRecords()) {
			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					TablePointing.Record otherR = other.getTablePointing().getRecord("id", r.id);
					Assert.assertNotNull(otherR);
					if (r.table2 == null)
						Assert.assertNull(otherR.table2);
					else
						Assert.assertEquals(otherR.table2.value, r.table2.value);
				}
			}
		}
		for (UndecentralizableTableA1.Record r : db.getUndecentralizableTableA1().getRecords()) {
			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					UndecentralizableTableA1.Record otherR = other.getUndecentralizableTableA1().getRecord("id",
							r.id);
					if (otherR != null) {
						Assert.assertNotEquals(otherR.value, r.value);
					}
				}
			}
		}
		for (UndecentralizableTableB1.Record r : db.getUndecentralizableTableB1().getRecords()) {
			for (CommonDecentralizedTests.Database other : listDatabase) {
				if (other != db) {
					UndecentralizableTableB1.Record otherR = other.getUndecentralizableTableB1().getRecord("id", r.id);
					if (otherR != null) {
						if (r.pointing == null)
							Assert.assertNull(otherR.pointing);
						else
							Assert.assertNotEquals(otherR.pointing.value, r.pointing.value);
					}
				}
			}
		}
	}

	protected void testSynchronisation() throws DatabaseException {
		for (CommonDecentralizedTests.Database db : listDatabase)
			testSynchronisation(db);
	}


	protected ArrayList<TableEvent<DatabaseRecord>> provideTableEvents(int number) throws DatabaseException {
		ArrayList<TableEvent<DatabaseRecord>> res = new ArrayList<>();
		ArrayList<TablePointed.Record> pointedRecord = new ArrayList<>();
		ArrayList<DatabaseRecord> livingRecords = new ArrayList<>();

		while (number > 0) {
			TableEvent<DatabaseRecord> te = null;

			switch ((int) (Math.random() * 4)) {

				case 0: {

					DatabaseRecord record = null;
					switch ((int) (Math.random() * 3)) {
						case 0:
							record = generatesTableAloneRecord();
							break;
						case 1:
							record = generatesTablePointedRecord();
							break;
						case 2:
							if (pointedRecord.isEmpty())
								record = generatesTablePointedRecord();
							else
								record = generatesTablePointingRecord(
										pointedRecord.get((int) (Math.random() * pointedRecord.size())));
							break;
					}
					te = new TableEvent<>(-1, DatabaseEventType.ADD, null, record, null);
					livingRecords.add(record);

				}
				break;
				case 1: {
					if (livingRecords.isEmpty())
						continue;
					DatabaseRecord record = livingRecords.get((int) (Math.random() * livingRecords.size()));
					te = new TableEvent<>(-1, DatabaseEventType.REMOVE, record, null, null);
					Assert.assertTrue(livingRecords.remove(record));

					//noinspection SuspiciousMethodCalls
					pointedRecord.remove(record);
				}
				break;
				case 2: {
					if (livingRecords.isEmpty())
						continue;
					DatabaseRecord record = livingRecords.get((int) (Math.random() * livingRecords.size()));
					te = new TableEvent<>(-1, DatabaseEventType.REMOVE_WITH_CASCADE, record, null, null);
					Assert.assertTrue(livingRecords.remove(record));
					//noinspection SuspiciousMethodCalls
					pointedRecord.remove(record);
				}
				break;
				case 3: {
					if (livingRecords.isEmpty())
						continue;
					DatabaseRecord record = livingRecords.get((int) (Math.random() * livingRecords.size()));
					DatabaseRecord recordNew = null;
					if (record instanceof TableAlone.Record) {
						TableAlone.Record r = generatesTableAloneRecord();
						r.id = ((TableAlone.Record) record).id;
						r.id2=((TableAlone.Record)record).id2;
						recordNew = r;
					} else if (record instanceof TablePointed.Record) {
						TablePointed.Record r = generatesTablePointedRecord();
						r.id = ((TablePointed.Record) record).id;
						recordNew = r;
					} else if (record instanceof TablePointing.Record) {
						TablePointing.Record r = new TablePointing.Record();
						r.id = ((TablePointing.Record) record).id;
						if (pointedRecord.isEmpty())
							continue;
						r.table2 = pointedRecord.get((int) (Math.random() * pointedRecord.size()));
						recordNew = r;
					}
					te = new TableEvent<>(-1, DatabaseEventType.UPDATE, record, recordNew, null);
				}
				break;

			}
			if (te != null) {
				res.add(te);
				--number;
			}
		}

		return res;
	}

	@DataProvider(name = "provideDataForSynchroBetweenTwoPeers")
	public Object[][] provideDataForSynchroBetweenTwoPeers() throws DatabaseException {
		int numberEvents = 40;
		Object[][] res = new Object[numberEvents * 2 * 2 * 2][];
		int index = 0;
		for (boolean exceptionDuringTransaction : new boolean[] { false, true }) {
			boolean[] gdc = exceptionDuringTransaction ? new boolean[] { false } : new boolean[] { true, false };
			for (boolean generateDirectConflict : gdc) {
				for (boolean peersInitiallyConnected : new boolean[] { true, false }) {
					for (TableEvent<DatabaseRecord> te : provideTableEvents(numberEvents)) {
						res[index++] = new Object[] {exceptionDuringTransaction,
								generateDirectConflict, peersInitiallyConnected, te };
					}
				}
			}

		}
		return res;
	}

	protected void proceedEvent(final CommonDecentralizedTests.Database db, final boolean exceptionDuringTransaction,
							  final List<TableEvent<DatabaseRecord>> events) throws DatabaseException {
		proceedEvent(db, exceptionDuringTransaction, events, false);
	}

	protected void proceedEvent(final CommonDecentralizedTests.Database db, final boolean exceptionDuringTransaction,
							  final List<TableEvent<DatabaseRecord>> events, final boolean manualKeys) throws DatabaseException {
		db.getDbwrapper().runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

			@Override
			public Void run() throws Exception {
				int indexException = exceptionDuringTransaction ? ((int) (Math.random() * events.size())) : -1;
				for (int i = 0; i < events.size(); i++) {
					TableEvent<DatabaseRecord> te = events.get(i);
					proceedEvent(te.getTable(db.getDbwrapper()), te, indexException == i, manualKeys);
				}
				return null;
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_SERIALIZABLE;
			}

			@Override
			public boolean doesWriteData() {
				return true;
			}

			@Override
			public void initOrReset() {

			}
		});

	}

	protected void proceedEvent(final Table<DatabaseRecord> table, final TableEvent<DatabaseRecord> event,
								boolean exceptionDuringTransaction, boolean manualKeys) throws Exception {
		switch (event.getType()) {

			case ADD:

				table.addRecord(table.getMap(event.getNewDatabaseRecord(), true, true));
				break;
			case REMOVE:
				table.removeRecord(event.getOldDatabaseRecord());
				break;
			case REMOVE_WITH_CASCADE:
				table.removeRecordWithCascade(event.getOldDatabaseRecord());
				break;
			case UPDATE:
				table.updateRecord(event.getNewDatabaseRecord());
				break;
		}
		if (exceptionDuringTransaction)
			throw new Exception();

	}

	protected void testEventSynchronized(CommonDecentralizedTests.Database db, List<TableEvent<DatabaseRecord>> levents, boolean synchronizedOk)
			throws DatabaseException {
		ArrayList<TableEvent<DatabaseRecord>> l = new ArrayList<>(levents.size());
		for (TableEvent<DatabaseRecord> te : levents) {
			switch (te.getType()) {
				case ADD:
				case UPDATE:
				case REMOVE: {

					for (Iterator<TableEvent<DatabaseRecord>> it = l.iterator(); it.hasNext(); ) {
						TableEvent<DatabaseRecord> te2 = it.next();
						DatabaseRecord dr1 = te.getOldDatabaseRecord() == null ? te.getNewDatabaseRecord()
								: te.getOldDatabaseRecord();
						DatabaseRecord dr2 = te2.getOldDatabaseRecord() == null ? te2.getNewDatabaseRecord()
								: te2.getOldDatabaseRecord();
						if (dr1 == null)
							throw new IllegalAccessError();
						if (dr2 == null)
							throw new IllegalAccessError();
						Table<DatabaseRecord> table = te2.getTable(db.getDbwrapper());
						if (te.getTable(db.getDbwrapper()).getClass().getName().equals(table.getClass().getName())
								&& table.equals(dr1, dr2)) {
							it.remove();
							break;
						}
					}
					break;
				}
				case REMOVE_WITH_CASCADE: {

					Table<DatabaseRecord> tablePointed = null;
					TablePointed.Record recordRemoved = null;
					for (Iterator<TableEvent<DatabaseRecord>> it = l.iterator(); it.hasNext(); ) {
						TableEvent<DatabaseRecord> te2 = it.next();
						Table<DatabaseRecord> table = te2.getTable(db.getDbwrapper());
						DatabaseRecord dr1 = te.getOldDatabaseRecord() == null ? te.getNewDatabaseRecord()
								: te.getOldDatabaseRecord();
						DatabaseRecord dr2 = te2.getOldDatabaseRecord() == null ? te2.getNewDatabaseRecord()
								: te2.getOldDatabaseRecord();
						if (dr1 == null)
							throw new IllegalAccessError();
						if (dr2 == null)
							throw new IllegalAccessError();

						if (te.getTable(db.getDbwrapper()).getClass().getName().equals(table.getClass().getName())
								&& table.equals(dr1, dr2)) {

							it.remove();
							if (table.getClass().getName().equals(TablePointed.class.getName())) {
								recordRemoved = (TablePointed.Record) te2.getNewDatabaseRecord();
								tablePointed = table;
							}
							break;
						}

					}
					if (recordRemoved != null) {
						for (Iterator<TableEvent<DatabaseRecord>> it = l.iterator(); it.hasNext(); ) {
							TableEvent<DatabaseRecord> te2 = it.next();
							Table<DatabaseRecord> table = te2.getTable(db.getDbwrapper());
							if (table.getClass().getName().equals(TablePointing.class.getName())) {
								TablePointing.Record tp = te2.getOldDatabaseRecord() == null
										? (TablePointing.Record) te2.getNewDatabaseRecord()
										: (TablePointing.Record) te2.getOldDatabaseRecord();
								if (tp.table2 != null && tablePointed.equals(tp.table2, recordRemoved))
									it.remove();
							}
						}

					}
					break;
				}
			}
			l.add(te);
		}
		for (TableEvent<DatabaseRecord> te : l)
			testEventSynchronized(db, te, synchronizedOk);
	}

	protected Map<String, Object> getMapPrimaryKeys(Table<DatabaseRecord> table, DatabaseRecord record)
			throws DatabaseException {
		Map<String, Object> res = new HashMap<>();
		for (FieldAccessor fa : table.getPrimaryKeysFieldAccessors()) {
			res.put(fa.getFieldName(), fa.getValue(record));
		}
		return res;
	}

	protected void testEventSynchronized(CommonDecentralizedTests.Database db, TableEvent<DatabaseRecord> event, boolean synchronizedOk)
			throws DatabaseException {

		if (event.getType() == DatabaseEventType.ADD || event.getType() == DatabaseEventType.UPDATE) {
			Table<DatabaseRecord> table = event.getTable(db.getDbwrapper());

			DatabaseRecord dr = table.getRecord(getMapPrimaryKeys(table, event.getNewDatabaseRecord()));
			if (synchronizedOk)
				Assert.assertNotNull(dr, event.getType().name() + " ; " + event.getTable(db.getDbwrapper()));
			Assert.assertEquals(table.equalsAllFields(dr, event.getNewDatabaseRecord()), synchronizedOk,
					"Concerned event : " + event);
		} else if (event.getType() == DatabaseEventType.REMOVE
				|| event.getType() == DatabaseEventType.REMOVE_WITH_CASCADE) {
			Table<DatabaseRecord> table = event.getTable(db.getDbwrapper());
			DatabaseRecord dr = table.getRecord(getMapPrimaryKeys(table, event.getOldDatabaseRecord()));
			if (synchronizedOk)
				Assert.assertNull(dr);
		} else
			throw new IllegalAccessError();
	}

	protected void testCollision(CommonDecentralizedTests.Database db, TableEvent<DatabaseRecord> event, CommonDecentralizedTests.DetectedCollision collision)
			throws DatabaseException {
		Table<DatabaseRecord> table = event.getTable(db.getDbwrapper());
		Assert.assertNotNull(collision);
		Assert.assertEquals(collision.type, event.getType());
		Assert.assertEquals(collision.concernedTable.getSqlTableName(), table.getSqlTableName());
		Assert.assertNotEquals(collision.distantPeerID, db.getHostID());
		switch (event.getType()) {
			case ADD:
				Assert.assertNotNull(collision.actualValues);
				Assert.assertNull(event.getOldDatabaseRecord());
				Assert.assertTrue(table.equals(event.getNewDatabaseRecord(), collision.actualValues));
				Assert.assertTrue(table.equalsAllFields(event.getNewDatabaseRecord(), collision.newValues));
				break;
			case REMOVE:

				Assert.assertNotNull(event.getOldDatabaseRecord());
				Assert.assertNull(event.getNewDatabaseRecord());
				Assert.assertNull(collision.newValues);
				if (collision.actualValues != null) {
					Assert.assertTrue(table.equals(event.getOldDatabaseRecord(), collision.actualValues));
				}
				break;
			case REMOVE_WITH_CASCADE:
				Assert.assertNotNull(event.getOldDatabaseRecord());
				Assert.assertNull(event.getNewDatabaseRecord());
				Assert.assertNull(collision.newValues);
				Assert.assertNull(collision.actualValues);
				break;
			case UPDATE:
				Assert.assertNotNull(event.getOldDatabaseRecord());
				Assert.assertNotNull(event.getNewDatabaseRecord());
				Assert.assertNotNull(collision.newValues);
				// Assert.assertNull(collision.actualValues);
				break;

		}
	}

	protected DatabaseRecord clone(DatabaseRecord record) {
		if (record == null)
			return null;

		if (record instanceof TableAlone.Record) {
			TableAlone.Record r = (TableAlone.Record) record;
			return r.clone();
		} else if (record instanceof TablePointing.Record) {
			TablePointing.Record r = (TablePointing.Record) record;
			return r.clone();
		} else if (record instanceof TablePointed.Record) {
			TablePointed.Record r = (TablePointed.Record) record;
			return r.clone();
		} else if (record instanceof UndecentralizableTableA1.Record) {
			UndecentralizableTableA1.Record r = (UndecentralizableTableA1.Record) record;
			return r.clone();
		} else if (record instanceof UndecentralizableTableB1.Record) {
			UndecentralizableTableB1.Record r = (UndecentralizableTableB1.Record) record;
			return r.clone();
		} else
			throw new IllegalAccessError("Unkown type " + record.getClass());
	}

	protected TableEvent<DatabaseRecord> clone(TableEvent<DatabaseRecord> event) {
		return new TableEvent<>(event.getID(), event.getType(), clone(event.getOldDatabaseRecord()),
				clone(event.getNewDatabaseRecord()), event.getHostsDestination());
	}

	protected List<TableEvent<DatabaseRecord>> clone(List<TableEvent<DatabaseRecord>> events) {
		ArrayList<TableEvent<DatabaseRecord>> res = new ArrayList<>(events.size());
		for (TableEvent<DatabaseRecord> te : events)
			res.add(clone(te));
		return res;
	}

	protected void testSynchroBetweenPeersImpl(int peersNumber, boolean exceptionDuringTransaction,
											   boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		if (peersNumber < 2 || peersNumber > listDatabase.size())
			throw new IllegalArgumentException();
		List<TableEvent<DatabaseRecord>> levents = Collections.singletonList(event);
		ArrayList<CommonDecentralizedTests.Database> l = new ArrayList<>(peersNumber);
		for (int i = 0; i < peersNumber; i++)
			l.add(listDatabase.get(i));
		CommonDecentralizedTests.Database[] concernedDatabase = new CommonDecentralizedTests.Database[l.size()];
		for (int i = 0; i < l.size(); i++)
			concernedDatabase[i] = l.get(i);

		if (exceptionDuringTransaction) {
			if (peersInitiallyConnected) {
				connectSelectedDatabase(concernedDatabase);
				exchangeMessages();
				for (int i = 1; i < peersNumber; i++) {
					CommonDecentralizedTests.Database db = concernedDatabase[i];
					db.setNewDatabaseEventDetected(false);
				}
			}

			CommonDecentralizedTests.Database db = concernedDatabase[0];
			try {
				proceedEvent(db, true, levents);
				Assert.fail();
			} catch (Exception ignored) {

			}

			if (!peersInitiallyConnected)
				connectSelectedDatabase(concernedDatabase);

			exchangeMessages();

			for (int i = 1; i < peersNumber; i++) {
				db = concernedDatabase[i];
				if (peersInitiallyConnected)
					Assert.assertFalse(db.isNewDatabaseEventDetected(), "" + db.getLocalEvents());
				testEventSynchronized(db, event, false);
			}

			disconnectSelectedDatabase(concernedDatabase);
		} else {

			if (generateDirectConflict) {
				int i = 0;
				for (CommonDecentralizedTests.Database db : concernedDatabase) {
					if (i++ == 0) {
						db.setReplaceWhenCollisionDetected(false);
					} else {
						db.setReplaceWhenCollisionDetected(true);
					}
					proceedEvent(db, false, clone(levents), true);

				}
				connectSelectedDatabase(concernedDatabase);
				exchangeMessages();
				i = 0;
				for (CommonDecentralizedTests.Database db : concernedDatabase) {
					Assert.assertTrue(db.isNewDatabaseEventDetected());

					CommonDecentralizedTests.DetectedCollision dcollision = db.getDetectedCollision();
					Assert.assertNotNull(dcollision, "i=" + (i));
					testCollision(db, event, dcollision);
					Assert.assertTrue(db.getAnomalies().isEmpty() || !sendIndirectTransactions(), db.getAnomalies().toString());

					++i;
				}

			} else {
				if (peersInitiallyConnected) {
					connectSelectedDatabase(concernedDatabase);
					for (CommonDecentralizedTests.Database db : concernedDatabase)
						db.setNewDatabaseEventDetected(false);
				}

				CommonDecentralizedTests.Database db = concernedDatabase[0];
				proceedEvent(db, false, levents);

				if (!peersInitiallyConnected)
					connectSelectedDatabase(concernedDatabase);

				exchangeMessages();
				Assert.assertTrue(db.getAnomalies().isEmpty());

				for (int i = 1; i < concernedDatabase.length; i++) {
					db = concernedDatabase[i];
					Assert.assertNull(db.getDetectedCollision());
					Assert.assertTrue(db.getAnomalies().isEmpty());
					Assert.assertTrue(db.isNewDatabaseEventDetected());
					testEventSynchronized(db, event, true);

				}

			}
			disconnectSelectedDatabase(concernedDatabase);
			for (int i = peersNumber; i < listDatabase.size(); i++) {
				CommonDecentralizedTests.Database db = listDatabase.get(i);
				testEventSynchronized(db, event, false);
				db.clearPendingEvents();
			}

			connectAllDatabase();
			exchangeMessages();

			for (int i = peersNumber; i < listDatabase.size(); i++) {
				CommonDecentralizedTests.Database db = listDatabase.get(i);
				// DetectedCollision collision=db.getDetectedCollision();
				// Assert.assertNotNull(collision, "Database N°"+i);
				Assert.assertTrue(db.getAnomalies().isEmpty());
				Assert.assertTrue(db.isNewDatabaseEventDetected());
				testEventSynchronized(db, event, true);

			}

			disconnectAllDatabase();
		}
		testSynchronisation();
		checkAllDatabaseInternalDataUsedForSynchro();

	}

	protected void testTransactionBetweenPeers(int peersNumber, boolean peersInitiallyConnected,
											 List<TableEvent<DatabaseRecord>> levents, boolean threadTest)
			throws Exception {
		if (peersNumber < 2 || peersNumber > listDatabase.size())
			throw new IllegalArgumentException();
		ArrayList<CommonDecentralizedTests.Database> l = new ArrayList<>(listDatabase.size());
		for (int i = 0; i < peersNumber; i++)
			l.add(listDatabase.get(i));

		CommonDecentralizedTests.Database[] concernedDatabase = new CommonDecentralizedTests.Database[l.size()];
		for (int i = 0; i < l.size(); i++)
			concernedDatabase[i] = l.get(i);

		if (peersInitiallyConnected && !threadTest)
			connectSelectedDatabase(concernedDatabase);

		CommonDecentralizedTests.Database db = concernedDatabase[0];

		proceedEvent(db, false, levents);

		if (!peersInitiallyConnected && !threadTest)
			connectSelectedDatabase(concernedDatabase);

		exchangeMessages();
		Assert.assertTrue(db.getAnomalies().isEmpty());

		for (int i = 1; i < peersNumber; i++) {
			db = concernedDatabase[i];

			Assert.assertNull(db.getDetectedCollision());
			if (!threadTest)
				Assert.assertTrue(db.isNewDatabaseEventDetected());
			if (!threadTest)
				testEventSynchronized(db, levents, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

		}

		for (int i = peersNumber; i < listDatabase.size(); i++) {
			db = listDatabase.get(i);

			if (!threadTest)
				testEventSynchronized(db, levents, false);
		}
		if (!threadTest) {
			disconnectSelectedDatabase(concernedDatabase);
			connectAllDatabase();
			exchangeMessages();

			for (int i = 2; i < listDatabase.size(); i++) {
				db = listDatabase.get(i);

				Assert.assertNull(db.getDetectedCollision());
				Assert.assertTrue(db.isNewDatabaseEventDetected());
				testEventSynchronized(db, levents, true);
				Assert.assertTrue(db.getAnomalies().isEmpty());

			}

			disconnectAllDatabase();
			checkAllDatabaseInternalDataUsedForSynchro();
		}

	}

	public Object[][] provideDataForTransactionBetweenTwoPeers(int numberTransactions) throws DatabaseException {
		Object[][] res = new Object[2 * numberTransactions][];
		int index = 0;
		for (boolean peersInitiallyConnected : new boolean[] { true, false }) {
			for (int i = 0; i < numberTransactions; i++) {
				res[index++] = new Object[] {peersInitiallyConnected,
						provideTableEvents((int) (5.0 + Math.random() * 10.0)) };

			}
		}

		return res;
	}

	@DataProvider(name = "provideDataSynchroBetweenThreePeers")
	public Object[][] provideDataSynchroBetweenThreePeers() throws DatabaseException {
		return provideDataForSynchroBetweenTwoPeers();
	}

	@DataProvider(name = "provideDataForTransactionBetweenTwoPeers")
	public Object[][] provideDataForTransactionBetweenTwoPeers() throws DatabaseException {
		return provideDataForTransactionBetweenTwoPeers(40);
	}
	@DataProvider(name = "provideDataForTransactionBetweenTwoPeersForRestorationTests")
	public Object[][] provideDataForTransactionBetweenTwoPeersForRestorationTests() throws DatabaseException {
		return provideDataForTransactionBetweenTwoPeers(5);
	}

	@DataProvider(name = "provideDataForTransactionBetweenThreePeers")
	public Object[][] provideDataForTransactionBetweenThreePeers() throws DatabaseException {
		return provideDataForTransactionBetweenTwoPeers();
	}
	@DataProvider(name = "provideDataForTransactionBetweenThreePeersForRestorationTests")
	public Object[][] provideDataForTransactionBetweenThreePeersForRestorationTests() throws DatabaseException {
		return provideDataForTransactionBetweenTwoPeersForRestorationTests();
	}

	@DataProvider(name = "provideDataForTransactionSynchros")
	public Object[][] provideDataForTransactionSynchros() throws DatabaseException {
		return provideDataForTransactionBetweenTwoPeers();
	}


	@DataProvider(name = "provideDataForTransactionSynchrosWithIndirectConnection")
	public Object[][] provideDataForTransactionSynchrosWithIndirectConnection() throws DatabaseException {
		return provideDataForTransactionBetweenTwoPeers();
	}

	@DataProvider(name = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", parallel = true)
	public Object[][] provideDataForTransactionSynchrosWithIndirectConnectionThreaded() throws DatabaseException {
		int numberTransactions = 40;
		Object[][] res = new Object[numberTransactions][];
		int index = 0;
		for (int i = 0; i < numberTransactions; i++) {
			res[index++] = new Object[] { provideTableEvents((int) (5.0 + Math.random() * 10.0)) };

		}

		return res;

	}

	protected void testTransactionsSynchrosWithIndirectConnection(boolean peersInitiallyConnected,
																  List<TableEvent<DatabaseRecord>> levents, boolean multiThread)
			throws Exception {
		final CommonDecentralizedTests.Database[] segmentA = new CommonDecentralizedTests.Database[] { listDatabase.get(0), listDatabase.get(1) };
		final CommonDecentralizedTests.Database[] segmentB = new CommonDecentralizedTests.Database[] { listDatabase.get(1), listDatabase.get(2) };

		if (peersInitiallyConnected && !multiThread) {
			connectSelectedDatabase(segmentA);
			connectSelectedDatabase(segmentB);
		}

		CommonDecentralizedTests.Database db = listDatabase.get(0);
		proceedEvent(db, false, levents);

		if (!peersInitiallyConnected && !multiThread) {
			connectSelectedDatabase(segmentA);
			connectSelectedDatabase(segmentB);
		}

		exchangeMessages();

		Assert.assertTrue(db.getAnomalies().isEmpty());

		db = listDatabase.get(1);
		Assert.assertNull(db.getDetectedCollision());
		if (!multiThread)
			Assert.assertTrue(db.isNewDatabaseEventDetected());
		if (!multiThread)
			testEventSynchronized(db, levents, true);
		Assert.assertTrue(db.getAnomalies().isEmpty());

		db = listDatabase.get(2);
		Assert.assertNull(db.getDetectedCollision());
		if (!multiThread)
			Assert.assertTrue(db.isNewDatabaseEventDetected());
		Assert.assertTrue(db.getAnomalies().isEmpty());
		if (!multiThread)
			testEventSynchronized(db, levents, true);

		if (!multiThread) {
			disconnectSelectedDatabase(segmentA);
			disconnectSelectedDatabase(segmentB);

			connectAllDatabase();
			exchangeMessages();
			disconnectAllDatabase();
		}

	}



}
