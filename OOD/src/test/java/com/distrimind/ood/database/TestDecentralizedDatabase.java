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
package com.distrimind.ood.database;

import com.distrimind.ood.database.DatabaseWrapper.SynchronizationAnomalyType;
import com.distrimind.ood.database.decentralizeddatabase.*;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.DecentralizedIDGenerator;
import com.distrimind.util.crypto.ASymmetricAuthentifiedSignatureType;
import com.distrimind.util.crypto.SecureRandomType;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
@SuppressWarnings("deprecation")
public abstract class TestDecentralizedDatabase {
	public static class DistantDatabaseEvent {
		private final byte[] eventToSend;
		private final byte[] joindedData;
		private final AbstractDecentralizedID hostDest;

		DistantDatabaseEvent(DatabaseWrapper wrapper, DatabaseEventToSend eventToSend)
				throws Exception {
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
				try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
					oos.writeObject(eventToSend);
				}

				baos.flush();

				this.eventToSend = baos.toByteArray();
			}
			if (eventToSend instanceof BigDatabaseEventToSend) {
				BigDatabaseEventToSend b = (BigDatabaseEventToSend) eventToSend;
				final AtomicReference<ByteArrayOutputStream> baos=new AtomicReference<>();
				try (OutputStreamGetter osg=new OutputStreamGetter() {
					
					@Override
					public OutputStream initOrResetOutputStream() throws IOException {
						if (baos.get()!=null)
							baos.get().close();
						baos.set(new ByteArrayOutputStream());
						return baos.get();		
					}

					@Override
					public void close() throws Exception {
						if (baos.get()!=null)
							baos.get().close();
					}
				}) {
					b.exportToOutputStream(wrapper, osg);
					baos.get().flush();
					this.joindedData = baos.get().toByteArray();
				}
			} else
				this.joindedData = null;
			hostDest = eventToSend.getHostDestination();
		}

		public DatabaseEventToSend getDatabaseEventToSend() throws IOException, ClassNotFoundException {
			try (ByteArrayInputStream bais = new ByteArrayInputStream(eventToSend)) {
				try (ObjectInputStream ois = new ObjectInputStream(bais)) {
					return (DatabaseEventToSend) ois.readObject();
				}
			}
		}

		public InputStream getInputStream() {
			return new ByteArrayInputStream(joindedData);
		}

		public AbstractDecentralizedID getHostDestination() {
			return hostDest;
		}
	}

	public class Database implements AutoCloseable, DatabaseWrapper.DatabaseNotifier {
		private final DatabaseWrapper dbwrapper;
		private volatile boolean connected;
		private final AbstractDecentralizedID hostID;
		private final ArrayList<DatabaseEvent> localEvents;
		private final List<DistantDatabaseEvent> eventsReceivedStack;
		private TableAlone tableAlone;
		private TablePointed tablePointed;
		private TablePointing tablePointing;
		private UndecentralizableTableA1 undecentralizableTableA1;
		private UndecentralizableTableB1 undecentralizableTableB1;
		private volatile boolean newDatabaseEventDetected = false;
		private volatile boolean replaceWhenCollisionDetected = false;
		private volatile DetectedCollision collisionDetected = null;

		private volatile TablePointed.Record recordPointed = null;
		private TablePointing.Record recordPointingNull = null, recordPointingNotNull = null;
		private TableAlone.Record recordAlone = null;
		private final List<Anomaly> anomalies;

		Database(DatabaseWrapper dbwrapper) throws DatabaseException {
			this.dbwrapper = dbwrapper;
			connected = false;
			hostID = new DecentralizedIDGenerator();
			localEvents = new ArrayList<>();
			eventsReceivedStack = Collections.synchronizedList(new LinkedList<DistantDatabaseEvent>());
			dbwrapper.loadDatabase(new DatabaseConfiguration(TableAlone.class.getPackage()), true);
			tableAlone = (TableAlone) dbwrapper.getTableInstance(TableAlone.class);
			tablePointed = (TablePointed) dbwrapper.getTableInstance(TablePointed.class);
			tablePointing = (TablePointing) dbwrapper.getTableInstance(TablePointing.class);
			undecentralizableTableA1 = (UndecentralizableTableA1) dbwrapper
					.getTableInstance(UndecentralizableTableA1.class);
			undecentralizableTableB1 = (UndecentralizableTableB1) dbwrapper
					.getTableInstance(UndecentralizableTableB1.class);
			anomalies = Collections.synchronizedList(new ArrayList<Anomaly>());
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
			synchronized (TestDecentralizedDatabase.class) {
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

		public List<DistantDatabaseEvent> getReceivedDatabaseEvents() {
			return this.eventsReceivedStack;
		}

		@Override
		public void newDatabaseEventDetected(DatabaseWrapper _wrapper) {
			newDatabaseEventDetected = true;
		}

		@Override
		public boolean collisionDetected(AbstractDecentralizedID _distantPeerID,
				AbstractDecentralizedID _intermediatePeer, DatabaseEventType _type, Table<?> _concernedTable,
				HashMap<String, Object> _keys, DatabaseRecord _newValues, DatabaseRecord _actualValues) {
			synchronized (TestDecentralizedDatabase.class) {
				collisionDetected = new DetectedCollision(_distantPeerID, _intermediatePeer, _type, _concernedTable,
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

		public DetectedCollision getDetectedCollision() {
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

		public List<Anomaly> getAnomalies() {
			return anomalies;
		}

		@Override
		public void anomalyDetected(AbstractDecentralizedID distantPeerID, AbstractDecentralizedID intermediatePeerID,
                                    SynchronizationAnomalyType _type, Table<?> _concernedTable, Map<String, Object> _primary_keys,
                                    DatabaseRecord _record) {
			anomalies.add(new Anomaly(distantPeerID, intermediatePeerID, _type, _concernedTable, _primary_keys, _record));
		}

	}

	public static class Anomaly {

		final AbstractDecentralizedID distantPeerID;
		final AbstractDecentralizedID intermediatePeerID;
		final Table<?> table;
		final Map<String, Object> keys;
		final DatabaseRecord record;
		final SynchronizationAnomalyType type;

		public Anomaly(AbstractDecentralizedID _distantPeerID, AbstractDecentralizedID _intermediatePeerID,SynchronizationAnomalyType type,
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
		final AbstractDecentralizedID distantPeerID;
		final AbstractDecentralizedID intermediatePeer;
		final DatabaseEventType type;
		final Table<?> concernedTable;
		final HashMap<String, Object> keys;
		final DatabaseRecord newValues;
		final DatabaseRecord actualValues;

		public DetectedCollision(AbstractDecentralizedID _distantPeerID, AbstractDecentralizedID _intermediatePeer,
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

	private volatile Database db1 = null, db2 = null, db3 = null, db4 = null;
	private final ArrayList<Database> listDatabase = new ArrayList<>(3);

	public abstract DatabaseWrapper getDatabaseWrapperInstance1() throws IllegalArgumentException, DatabaseException;

	public abstract DatabaseWrapper getDatabaseWrapperInstance2() throws IllegalArgumentException, DatabaseException;

	public abstract DatabaseWrapper getDatabaseWrapperInstance3() throws IllegalArgumentException, DatabaseException;

	public abstract DatabaseWrapper getDatabaseWrapperInstance4() throws IllegalArgumentException, DatabaseException;

	public abstract void removeDatabaseFiles1();

	public abstract void removeDatabaseFiles2();

	public abstract void removeDatabaseFiles3();

	public abstract void removeDatabaseFiles4();

	public void loadDatabase(Database db) throws DatabaseException {
		db.getDbwrapper()
				.loadDatabase(new DatabaseConfiguration(TableAlone.class.getPackage(), new DatabaseCreationCallable() {

					@Override
					public void transfertDatabaseFromOldVersion(DatabaseConfiguration _newDatabaseTables) {
					}

					@Override
					public boolean hasToRemoveOldDatabase() {
						return false;
					}

					@Override
					public void afterDatabaseCreation(DatabaseConfiguration _newDatabaseTables) {

					}
				}, null), true);

	}

	@BeforeClass
	public void loadDatabase() throws DatabaseException {
		unloadDatabase();
		db1 = new Database(getDatabaseWrapperInstance1());
		db2 = new Database(getDatabaseWrapperInstance2());
		db3 = new Database(getDatabaseWrapperInstance3());
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
					}
				}
			}
		}
	}

	@Override
	public void finalize() {
		unloadDatabase();
	}

	@AfterMethod
	public void cleanPendedEvents() {
		synchronized (TestDecentralizedDatabase.class) {
			for (Database db : listDatabase) {
				db.clearPendingEvents();
			}
		}
	}

	private void sendDistantDatabaseEvent(DistantDatabaseEvent event) {
		for (Database db : listDatabase) {
			if (db.getHostID().equals(event.getHostDestination())) {
				if (db.isConnected())
					db.getReceivedDatabaseEvents().add(event);
				else
					Assert.fail();
				break;
			}
		}
	}

	private boolean checkMessages(Database db) throws Exception {
		boolean changed = false;
		while (!db.getReceivedDatabaseEvents().isEmpty()) {
			changed = true;
			final DistantDatabaseEvent dde = db.getReceivedDatabaseEvents().remove(0);

			DatabaseEventToSend event = dde.getDatabaseEventToSend();
			if (event instanceof BigDatabaseEventToSend) {
				try (InputStreamGetter is = new InputStreamGetter() {
					
					private InputStream actual=null;
					
					@Override
					public InputStream initOrResetInputStream() throws IOException {
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
		return changed;
	}

	private boolean checkMessages() throws Exception {
		synchronized (TestDecentralizedDatabase.class) {
			boolean changed = false;
			for (Database db : listDatabase) {
				changed |= checkMessages(db);
			}
			return changed;
		}
	}

	private void exchangeMessages() throws Exception {
		synchronized (TestDecentralizedDatabase.class) {
			boolean loop = true;
			while (loop) {
				loop = false;
				for (Database db : listDatabase) {

					DatabaseEvent e = db.getDbwrapper().getSynchronizer().nextEvent();
					if (e != null) {

						loop = true;
						if (e instanceof DatabaseEventToSend) {
							DatabaseEventToSend es = (DatabaseEventToSend) e;

							Assert.assertEquals(es.getHostSource(), db.getHostID());
							Assert.assertNotEquals(es.getHostDestination(), db.getHostID());
							if (db.isConnected()) {
								sendDistantDatabaseEvent(new DistantDatabaseEvent(db.getDbwrapper(), es));
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

	private void checkAllDatabaseInternalDataUsedForSynchro() throws DatabaseException {
		for (Database db : listDatabase) {
			checkDatabaseInternalDataUsedForSynchro(db);
		}
	}

	private void checkDatabaseInternalDataUsedForSynchro(Database db) throws DatabaseException {
		synchronized (TestDecentralizedDatabase.class) {
			Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionsPerHostTable().getRecords().size(), 0);

			Assert.assertEquals(db.getDbwrapper().getTransactionsTable().getRecords().size(), 0);
			Assert.assertEquals(db.getDbwrapper().getDatabaseEventsTable().getRecords().size(), 0);
			Assert.assertEquals(db.getDbwrapper().getHooksTransactionsTable().getRecords().size(), listDatabase.size());
			Assert.assertEquals(db.getDbwrapper().getDatabaseDistantTransactionEvent().getRecords().size(), 0);
			Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionEventsTable().getRecords().size(), 0);
		}

	}

	private void connectLocal(Database db) throws DatabaseException {
		synchronized (TestDecentralizedDatabase.class) {
			if (!db.isConnected()) {
				db.getDbwrapper().getSynchronizer().initLocalHostID(db.getHostID());
				db.setConnected(true);
			}
		}
	}

	private void connectDistant(Database db, Database... listDatabase) throws DatabaseException {

		synchronized (TestDecentralizedDatabase.class) {
			if (db.isConnected()) {
				for (Database otherdb : listDatabase) {
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

	private void connectSelectedDatabase(Database... listDatabase)
			throws Exception {
		for (Database db : listDatabase) {
			connectLocal(db);
		}
		for (Database db : listDatabase) {
			connectDistant(db, listDatabase);
		}
		exchangeMessages();
	}

	private void connectAllDatabase() throws Exception {
		Database dbs[] = new Database[listDatabase.size()];
		for (int i = 0; i < dbs.length; i++)
			dbs[i] = listDatabase.get(i);
		for (Database db : listDatabase) {
			connectLocal(db);
		}
		for (Database db : listDatabase) {
			connectDistant(db, dbs);
		}
		exchangeMessages();
	}

	private void disconnect(Database db, Database... listDatabase) throws DatabaseException {
		synchronized (TestDecentralizedDatabase.class) {
			if (db.isConnected()) {
				db.setConnected(false);
				db.getDbwrapper().getSynchronizer().deconnectHook(db.getHostID());
				for (Database dbother : listDatabase) {
					if (dbother != db && dbother.isConnected())
						dbother.getDbwrapper().getSynchronizer().deconnectHook(db.getHostID());
				}
				Assert.assertFalse(db.isConnected());
			}
		}
	}

	private void disconnectSelectedDatabase(Database... listDatabase) throws DatabaseException {
		for (Database db : listDatabase)
			disconnect(db, listDatabase);
	}

	private void disconnectAllDatabase() throws DatabaseException {
		Database dbs[] = new Database[listDatabase.size()];
		for (int i = 0; i < dbs.length; i++)
			dbs[i] = listDatabase.get(i);
		disconnectSelectedDatabase(dbs);
	}

	private TableAlone.Record generatesTableAloneRecord() throws DatabaseException {
		TableAlone.Record ralone = new TableAlone.Record();
		ralone.id = new DecentralizedIDGenerator();
		try {
			ralone.id2 = ASymmetricAuthentifiedSignatureType.BC_SHA512withECDSA_CURVE_25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair().getASymmetricPublicKey();
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
		ralone.value = generateString();
		return ralone;

	}

	@SuppressWarnings("SameParameterValue")
    private void addTableAloneRecord(Database db, boolean first) throws DatabaseException {
		TableAlone.Record ralone = generatesTableAloneRecord();

		db.getTableAlone().addRecord(((Table<TableAlone.Record>) db.getTableAlone()).getMap(ralone, true, true));
		if (first)
			db.setRecordAlone(ralone);
	}

	private void addUndecentralizableTableA1Record(Database db) throws DatabaseException {
		UndecentralizableTableA1.Record record = new UndecentralizableTableA1.Record();
		StringBuilder sb=new StringBuilder(db.getHostID().toString());
		for (int i = 0; i < 10; i++) {
			sb.append('a' + ((int) (Math.random() * 52)));
		}
		record.value=sb.toString();
		db.getUndecentralizableTableA1().addRecord(record);

	}

	private void addUndecentralizableTableB1Record(Database db) throws DatabaseException {
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

	private String generateString() {
		StringBuilder res = new StringBuilder();
		for (int i = 0; i < 10; i++) {
			res.append('a' + ((int) (Math.random() * 52)));
		}
		return res.toString();
	}

	private TablePointed.Record generatesTablePointedRecord() {
		TablePointed.Record rpointed = new TablePointed.Record();
		rpointed.id = new DecentralizedIDGenerator();
		rpointed.value = generateString();
		return rpointed;
	}

	private TablePointing.Record generatesTablePointingRecord(TablePointed.Record rpointed) throws DatabaseException {
		TablePointing.Record rpointing1 = new TablePointing.Record();
		try {
			rpointing1.id = ASymmetricAuthentifiedSignatureType.BC_SHA512withECDSA_CURVE_25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair().getASymmetricPublicKey();
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
		}
		rpointing1.table2 = Math.random() < 0.5 ? null : rpointed;
		return rpointing1;
	}

	@SuppressWarnings("SameParameterValue")
    private void addTablePointedAndPointingRecords(Database db, boolean first) throws DatabaseException {
		TablePointed.Record rpointed = new TablePointed.Record();
		rpointed.id = new DecentralizedIDGenerator();
		rpointed.value = generateString();

		rpointed = db.getTablePointed().addRecord(rpointed);

		TablePointing.Record rpointing1 = new TablePointing.Record();
		try {
			rpointing1.id = ASymmetricAuthentifiedSignatureType.BC_SHA512withECDSA_CURVE_25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair().getASymmetricPublicKey();
		}
		catch(Exception e)
		{
			throw DatabaseException.getDatabaseException(e);
		}

		rpointing1.table2 = null;
		rpointing1 = db.getTablePointing().addRecord(rpointing1);
		TablePointing.Record rpointing2 = new TablePointing.Record();
		try {
			rpointing2.id = ASymmetricAuthentifiedSignatureType.BC_SHA512withECDSA_CURVE_25519.getKeyPairGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKeyPair().getASymmetricPublicKey();
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

	private void addElements(Database db) throws DatabaseException {
		addTableAloneRecord(db, true);
		addTablePointedAndPointingRecords(db, true);
		addUndecentralizableTableA1Record(db);
		addUndecentralizableTableB1Record(db);
	}

	private void addElements() throws DatabaseException {
		for (Database db : listDatabase)
			addElements(db);
	}


	@Test
	public void testAddFirstElements() throws DatabaseException {
		addElements();
	}

	@Test(dependsOnMethods = { "testAddFirstElements" })
	public void testInit() throws DatabaseException {
		for (Database db : listDatabase) {
			db.getDbwrapper().getSynchronizer().setNotifier(db);
			db.getDbwrapper().setMaxTransactionsToSynchronizeAtTheSameTime(5);
			db.getDbwrapper().setMaxTransactionEventsKeepedIntoMemory(3);
			db.getDbwrapper().getSynchronizer().addHookForLocalDatabaseHost(db.getHostID(),
					TablePointed.class.getPackage());
			Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized());

		}

		for (Database db : listDatabase) {
			for (Database other : listDatabase) {
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
		for (Database db : listDatabase) {
			for (Database other : listDatabase) {
				Assert.assertTrue(db.getDbwrapper().getSynchronizer().isInitialized(other.getHostID()));
			}
		}
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

	private void testSynchronisation(Database db) throws DatabaseException {

		for (TableAlone.Record r : db.getTableAlone().getRecords()) {

			for (Database other : listDatabase) {
				if (other != db) {
					TableAlone.Record otherR = other.getTableAlone().getRecord("id", r.id, "id2", r.id2);
					Assert.assertNotNull(otherR);
					Assert.assertEquals(otherR.value, r.value);
				}
			}
		}
		for (TablePointed.Record r : db.getTablePointed().getRecords()) {
			for (Database other : listDatabase) {
				if (other != db) {
					TablePointed.Record otherR = other.getTablePointed().getRecord("id", r.id);
					Assert.assertNotNull(otherR);
					Assert.assertEquals(otherR.value, r.value);
				}
			}
		}
		for (TablePointing.Record r : db.getTablePointing().getRecords()) {
			for (Database other : listDatabase) {
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
			for (Database other : listDatabase) {
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
			for (Database other : listDatabase) {
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

	private void testSynchronisation() throws DatabaseException {
		for (Database db : listDatabase)
			testSynchronisation(db);
	}

	@Test(dependsOnMethods = { "testAllConnect" })
	public void testOldElementsAddedBeforeAddingSynchroSynchronized()
			throws Exception {
		exchangeMessages();
		testSynchronisation();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();

	}

	private ArrayList<TableEvent<DatabaseRecord>> proviveTableEvents(int number) throws DatabaseException {
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
					for (TableEvent<DatabaseRecord> te : proviveTableEvents(numberEvents)) {
						res[index++] = new Object[] {exceptionDuringTransaction,
								generateDirectConflict, peersInitiallyConnected, te };
					}
				}
			}

		}
		return res;
	}

	private void proceedEvent(final Database db, final boolean exceptionDuringTransaction,
			final List<TableEvent<DatabaseRecord>> events) throws DatabaseException {
		proceedEvent(db, exceptionDuringTransaction, events, false);
	}

	private void proceedEvent(final Database db, final boolean exceptionDuringTransaction,
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

	private void testEventSynchronized(Database db, List<TableEvent<DatabaseRecord>> levents, boolean synchronizedOk)
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

	private Map<String, Object> getMapPrimaryKeys(Table<DatabaseRecord> table, DatabaseRecord record)
			throws DatabaseException {
		Map<String, Object> res = new HashMap<>();
		for (FieldAccessor fa : table.getPrimaryKeysFieldAccessors()) {
			res.put(fa.getFieldName(), fa.getValue(record));
		}
		return res;
	}

	private void testEventSynchronized(Database db, TableEvent<DatabaseRecord> event, boolean synchronizedOk)
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

	private void testCollision(Database db, TableEvent<DatabaseRecord> event, DetectedCollision collision)
			throws DatabaseException {
		Table<DatabaseRecord> table = event.getTable(db.getDbwrapper());
		Assert.assertNotNull(collision);
		Assert.assertEquals(collision.type, event.getType());
		Assert.assertEquals(collision.concernedTable.getName(), table.getName());
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

	private DatabaseRecord clone(DatabaseRecord record) {
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

	private TableEvent<DatabaseRecord> clone(TableEvent<DatabaseRecord> event) {
		return new TableEvent<>(event.getID(), event.getType(), clone(event.getOldDatabaseRecord()),
				clone(event.getNewDatabaseRecord()), event.getHostsDestination());
	}

	private List<TableEvent<DatabaseRecord>> clone(List<TableEvent<DatabaseRecord>> events) {
		ArrayList<TableEvent<DatabaseRecord>> res = new ArrayList<>(events.size());
		for (TableEvent<DatabaseRecord> te : events)
			res.add(clone(te));
		return res;
	}

	private void testSynchroBetweenPeers(int peersNumber, boolean exceptionDuringTransaction,
			boolean generateDirectConflict, boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		if (peersNumber < 2 || peersNumber > listDatabase.size())
			throw new IllegalArgumentException();
		List<TableEvent<DatabaseRecord>> levents = Collections.singletonList(event);
		ArrayList<Database> l = new ArrayList<>(peersNumber);
		for (int i = 0; i < peersNumber; i++)
			l.add(listDatabase.get(i));
		Database[] concernedDatabase = new Database[l.size()];
		for (int i = 0; i < l.size(); i++)
			concernedDatabase[i] = l.get(i);

		if (exceptionDuringTransaction) {
			if (peersInitiallyConnected) {
				connectSelectedDatabase(concernedDatabase);
				exchangeMessages();
				for (int i = 1; i < peersNumber; i++) {
					Database db = concernedDatabase[i];
					db.setNewDatabaseEventDetected(false);
				}
			}

			Database db = concernedDatabase[0];
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
				for (Database db : concernedDatabase) {
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
				for (Database db : concernedDatabase) {
					Assert.assertTrue(db.isNewDatabaseEventDetected());

					DetectedCollision dcollision = db.getDetectedCollision();
					Assert.assertNotNull(dcollision, "i=" + (i));
					testCollision(db, event, dcollision);
					Assert.assertTrue(db.getAnomalies().isEmpty(), db.getAnomalies().toString());

					++i;
				}

			} else {
				if (peersInitiallyConnected) {
					connectSelectedDatabase(concernedDatabase);
					for (Database db : concernedDatabase)
						db.setNewDatabaseEventDetected(false);
				}

				Database db = concernedDatabase[0];
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
				Database db = listDatabase.get(i);
				testEventSynchronized(db, event, false);
				db.clearPendingEvents();
			}

			connectAllDatabase();
			exchangeMessages();

			for (int i = peersNumber; i < listDatabase.size(); i++) {
				Database db = listDatabase.get(i);
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

	private void testTransactionBetweenPeers(int peersNumber, boolean peersInitiallyConnected,
			List<TableEvent<DatabaseRecord>> levents, boolean threadTest)
			throws Exception {
		if (peersNumber < 2 || peersNumber > listDatabase.size())
			throw new IllegalArgumentException();
		ArrayList<Database> l = new ArrayList<>(listDatabase.size());
		for (int i = 0; i < peersNumber; i++)
			l.add(listDatabase.get(i));

		Database[] concernedDatabase = new Database[l.size()];
		for (int i = 0; i < l.size(); i++)
			concernedDatabase[i] = l.get(i);

		if (peersInitiallyConnected && !threadTest)
			connectSelectedDatabase(concernedDatabase);

		Database db = concernedDatabase[0];

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

	@Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods = {
			"testOldElementsAddedBeforeAddingSynchroSynchronized" })
	// @Test(dataProvider = "provideDataForSynchroBetweenTwoPeers",
	// dependsOnMethods={"testSynchroBetweenThreePeers2"})
	public void testSynchroBetweenTwoPeers(boolean exceptionDuringTransaction, boolean generateDirectConflict,
			boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenPeers(2, exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dependsOnMethods = { "testSynchroBetweenTwoPeers" })
	public void testSynchroAfterTestsBetweenTwoPeers() throws DatabaseException {
		testSynchronisation();
	}

	@DataProvider(name = "provideDataSynchroBetweenThreePeers")
	public Object[][] provideDataSynchroBetweenThreePeers() throws DatabaseException {
		return provideDataForSynchroBetweenTwoPeers();
	}

	@Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods = { "testSynchroBetweenTwoPeers" })
	public void testSynchroBetweenThreePeers(boolean exceptionDuringTransaction, boolean generateDirectConflict,
			boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenPeers(3, exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dependsOnMethods = { "testSynchroBetweenThreePeers" })
	public void testSynchroAfterTestsBetweenThreePeers() throws DatabaseException {
		testSynchronisation();
	}

	@DataProvider(name = "provideDataForIndirectSynchro")
	public Object[][] provideDataForIndirectSynchro() throws DatabaseException {
		int numberEvents = 40;
		Object[][] res = new Object[numberEvents * 2 * 2 * 2][];
		int index = 0;
		for (boolean generateDirectConflict : new boolean[] { true, false }) {
			boolean ict[] = generateDirectConflict ? new boolean[] { true } : new boolean[] { false };
			for (boolean peersInitiallyConnected : ict) {
				for (TableEvent<DatabaseRecord> te : proviveTableEvents(numberEvents)) {
					res[index++] = new Object[] {generateDirectConflict,
							peersInitiallyConnected, te };
				}
			}
		}
		return res;

	}

	@Test(dataProvider = "provideDataForIndirectSynchro", dependsOnMethods = {
			"testSynchroAfterTestsBetweenThreePeers" })
	public void testIndirectSynchro(boolean generateDirectConflict, boolean peersInitiallyConnected,
			TableEvent<DatabaseRecord> event) throws Exception {
		List<TableEvent<DatabaseRecord>> levents = Collections.singletonList(event);
		final Database[] indirectDatabase = new Database[] { listDatabase.get(0), listDatabase.get(2) };
		final Database[] segmentA = new Database[] { listDatabase.get(0), listDatabase.get(1) };
		final Database[] segmentB = new Database[] { listDatabase.get(1), listDatabase.get(2) };
		if (generateDirectConflict) {
			int i = 0;
			for (Database db : indirectDatabase)// TODO test with opposite direction
			{
				if (i++ == 0) {
					db.setReplaceWhenCollisionDetected(false);
				} else {
					db.setReplaceWhenCollisionDetected(true);
				}

				proceedEvent(db, false, levents);
			}
			listDatabase.get(1).setReplaceWhenCollisionDetected(false);
			connectSelectedDatabase(segmentA);
			exchangeMessages();

			Database db = listDatabase.get(1);

			Assert.assertNull(db.getDetectedCollision());

			Assert.assertTrue(db.isNewDatabaseEventDetected());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());
			db.setNewDatabaseEventDetected(false);

			disconnectSelectedDatabase(segmentA);
			cleanPendedEvents();

			connectSelectedDatabase(segmentB);
			exchangeMessages();

			DetectedCollision dcollision = db.getDetectedCollision();

			testCollision(db, event, dcollision);
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());
			db = listDatabase.get(2);

			DetectedCollision collision = db.getDetectedCollision();
			testCollision(db, event, collision);
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.isNewDatabaseEventDetected());
			Assert.assertTrue(db.getAnomalies().isEmpty());

			disconnectSelectedDatabase(segmentB);
			cleanPendedEvents();

			connectSelectedDatabase(segmentB);
			exchangeMessages();

			db = listDatabase.get(1);

			Assert.assertNull(db.getDetectedCollision());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());
			if (listDatabase.size() == 3) {
				Assert.assertEquals(db.getDbwrapper().getDatabaseDistantTransactionEvent().getRecords().size(), 0);
				Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionsPerHostTable().getRecords().size(), 0);
				Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionEventsTable().getRecords().size(), 0);
			}

			db = listDatabase.get(2);

			Assert.assertNull(db.getDetectedCollision());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());
			if (listDatabase.size() == 3) {
				Assert.assertEquals(db.getDbwrapper().getDatabaseDistantTransactionEvent().getRecords().size(), 0);
				Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionsPerHostTable().getRecords().size(), 0);
				Assert.assertEquals(db.getDbwrapper().getDatabaseTransactionEventsTable().getRecords().size(), 0);
			}

			disconnectSelectedDatabase(segmentB);
			cleanPendedEvents();

			connectSelectedDatabase(segmentA);
			exchangeMessages();

			db = listDatabase.get(0);

			Assert.assertNull(db.getDetectedCollision());
			// Assert.assertFalse(db.isNewDatabaseEventDetected());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

			disconnectSelectedDatabase(segmentA);
			cleanPendedEvents();
			// checkAllDatabaseInternalDataUsedForSynchro();
			connectSelectedDatabase(segmentB);
			exchangeMessages();

			db = listDatabase.get(2);

			Assert.assertNull(db.getDetectedCollision());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.isNewDatabaseEventDetected());
			Assert.assertTrue(db.getAnomalies().isEmpty());

			db = listDatabase.get(1);

			Assert.assertNull(db.getDetectedCollision());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

			disconnectSelectedDatabase(segmentB);
			cleanPendedEvents();
			// TODO make possible this test here :
			// checkAllDatabaseInternalDataUsedForSynchro();
		} else {
			if (peersInitiallyConnected)
				connectSelectedDatabase(segmentA);

			Database db = listDatabase.get(0);
			proceedEvent(db, false, levents);

			if (!peersInitiallyConnected)
				connectSelectedDatabase(segmentA);

			exchangeMessages();
			Assert.assertTrue(db.getAnomalies().isEmpty());

			db = listDatabase.get(1);
			Assert.assertNull(db.getDetectedCollision());
			Assert.assertTrue(db.isNewDatabaseEventDetected());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

			disconnectSelectedDatabase(segmentA);
			connectSelectedDatabase(segmentB);
			exchangeMessages();

			db = listDatabase.get(2);
			Assert.assertNull(db.getDetectedCollision());
			Assert.assertTrue(db.isNewDatabaseEventDetected());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());
			disconnectSelectedDatabase(segmentB);
		}

		connectAllDatabase();
		exchangeMessages();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();

	}

	@Test(dependsOnMethods = { "testIndirectSynchro" })
	public void testSynchroAfterIndirectTestsBetweenPeers() throws DatabaseException {
		testSynchronisation();
	}

	@DataProvider(name = "provideDataForIndirectSynchroWithIndirectConnection")
	public Object[][] provideDataForIndirectSynchroWithIndirectConnection() throws DatabaseException {
		return provideDataForIndirectSynchro();
	}

	@Test(dataProvider = "provideDataForIndirectSynchroWithIndirectConnection", dependsOnMethods = {
			"testIndirectSynchro" })
	// @Test(dataProvider = "provideDataForIndirectSynchroWithIndirectConnection",
	// dependsOnMethods={"testOldElementsAddedBeforeAddingSynchroSynchronized"})
	public void testIndirectSynchroWithIndirectConnection(boolean generateDirectConflict,
			boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		List<TableEvent<DatabaseRecord>> levents = Collections.singletonList(event);
		final Database[] indirectDatabase = new Database[] { listDatabase.get(0), listDatabase.get(2) };
		final Database[] segmentA = new Database[] { listDatabase.get(0), listDatabase.get(1) };
		final Database[] segmentB = new Database[] { listDatabase.get(1), listDatabase.get(2) };
		if (generateDirectConflict) {
			int i = 0;
			for (Database db : indirectDatabase) {
				if (i++ == 0)
					db.setReplaceWhenCollisionDetected(false);
				else
					db.setReplaceWhenCollisionDetected(true);
				proceedEvent(db, false, levents);
			}
			segmentA[1].setReplaceWhenCollisionDetected(false);
			connectSelectedDatabase(segmentA);
			connectSelectedDatabase(segmentB);
			exchangeMessages();

			Database db = listDatabase.get(0);
			// Assert.assertFalse(db.isNewDatabaseEventDetected());
			Assert.assertNull(db.getDetectedCollision());

			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

			db = listDatabase.get(1);
			testCollision(db, event, db.getDetectedCollision());
			// Assert.assertTrue(db.isNewDatabaseEventDetected());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

			db = listDatabase.get(2);

			// Assert.assertFalse(db.isNewDatabaseEventDetected());
			testCollision(db, event, db.getDetectedCollision());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());
			disconnectSelectedDatabase(segmentA);
			disconnectSelectedDatabase(segmentB);
			// TODO make possible this test here :
			// checkAllDatabaseInternalDataUsedForSynchro();
		} else {
			if (peersInitiallyConnected) {
				connectSelectedDatabase(segmentA);
				connectSelectedDatabase(segmentB);
			}

			Database db = listDatabase.get(0);
			proceedEvent(db, false, levents);

			if (!peersInitiallyConnected) {
				connectSelectedDatabase(segmentA);
				connectSelectedDatabase(segmentB);
			}

			exchangeMessages();
			Assert.assertTrue(db.getAnomalies().isEmpty());

			db = listDatabase.get(1);
			Assert.assertNull(db.getDetectedCollision());
			Assert.assertTrue(db.isNewDatabaseEventDetected());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

			db = listDatabase.get(2);
			Assert.assertNull(db.getDetectedCollision());
			Assert.assertTrue(db.isNewDatabaseEventDetected());
			testEventSynchronized(db, event, true);
			Assert.assertTrue(db.getAnomalies().isEmpty());

			disconnectSelectedDatabase(segmentA);
			disconnectSelectedDatabase(segmentB);
		}

		connectAllDatabase();
		exchangeMessages();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();

	}

	@Test(dependsOnMethods = { "testIndirectSynchroWithIndirectConnection" })
	public void testSynchroAfterPostIndirectTestsBetweenPeers() throws DatabaseException {
		testSynchronisation();
	}

	@DataProvider(name = "provideDataForTransactionBetweenTwoPeers")
	public Object[][] provideDataForTransactionBetweenTwoPeers() throws DatabaseException {
		int numberTransactions = 40;
		Object res[][] = new Object[2 * numberTransactions][];
		int index = 0;
		for (boolean peersInitiallyConnected : new boolean[] { true, false }) {
			for (int i = 0; i < numberTransactions; i++) {
				res[index++] = new Object[] {peersInitiallyConnected,
						proviveTableEvents((int) (5.0 + Math.random() * 10.0)) };

			}
		}

		return res;
	}

	@Test(dataProvider = "provideDataForTransactionBetweenTwoPeers", dependsOnMethods = {
			"testSynchroAfterPostIndirectTestsBetweenPeers" })
	public void testTransactionBetweenTwoPeers(boolean peersInitiallyConnected,
			List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionBetweenPeers(2, peersInitiallyConnected, levents, false);
	}

	@DataProvider(name = "provideDataForTransactionBetweenThreePeers")
	public Object[][] provideDataForTransactionBetweenThreePeers() throws DatabaseException {
		return provideDataForTransactionBetweenTwoPeers();
	}

	@Test(dataProvider = "provideDataForTransactionBetweenThreePeers", dependsOnMethods = {
			"testTransactionBetweenTwoPeers" })
	public void testTransactionBetweenThreePeers(boolean peersInitiallyConnected,
			List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionBetweenPeers(3, peersInitiallyConnected, levents, false);
	}

	@DataProvider(name = "provideDataForTransactionSynchros")
	public Object[][] provideDataForTransactionSynchros() throws DatabaseException {
		return provideDataForTransactionBetweenTwoPeers();
	}

	@Test(dependsOnMethods = { "testTransactionBetweenThreePeers" })
	public void preTestTransactionSynchros() throws Exception {
		Database[] concernedDatabase = new Database[3];
		concernedDatabase[0] = listDatabase.get(0);
		concernedDatabase[1] = listDatabase.get(1);
		concernedDatabase[2] = listDatabase.get(2);

		connectSelectedDatabase(concernedDatabase);
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods = {
			"preTestTransactionSynchros" }, invocationCount = 4)
	public void testTransactionSynchros(List<TableEvent<DatabaseRecord>> levents)
			throws Exception {
		testTransactionBetweenPeers(3, true, levents, true);
	}

	@Test(dependsOnMethods = { "testTransactionSynchros" })
	public void postTestTransactionSynchros() throws Exception {
		Database[] concernedDatabase = new Database[3];
		concernedDatabase[0] = listDatabase.get(0);
		concernedDatabase[1] = listDatabase.get(1);
		concernedDatabase[2] = listDatabase.get(2);
		disconnectSelectedDatabase(concernedDatabase);

		connectAllDatabase();
		exchangeMessages();
		disconnectAllDatabase();
		testSynchronisation();

		checkAllDatabaseInternalDataUsedForSynchro();
	}

	@DataProvider(name = "provideDataForTransactionSynchrosWithIndirectConnection")
	public Object[][] provideDataForTransactionSynchrosWithIndirectConnection() throws DatabaseException {
		return provideDataForTransactionBetweenTwoPeers();
	}

	@DataProvider(name = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", parallel = true)
	public Object[][] provideDataForTransactionSynchrosWithIndirectConnectionThreaded() throws DatabaseException {
		int numberTransactions = 40;
		Object res[][] = new Object[numberTransactions][];
		int index = 0;
		for (int i = 0; i < numberTransactions; i++) {
			res[index++] = new Object[] { proviveTableEvents((int) (5.0 + Math.random() * 10.0)) };

		}

		return res;

	}

	private void testTransactionSynchrosWithIndirectConnection(boolean peersInitiallyConnected,
			List<TableEvent<DatabaseRecord>> levents, boolean multiThread)
			throws Exception {
		final Database[] segmentA = new Database[] { listDatabase.get(0), listDatabase.get(1) };
		final Database[] segmentB = new Database[] { listDatabase.get(1), listDatabase.get(2) };

		if (peersInitiallyConnected && !multiThread) {
			connectSelectedDatabase(segmentA);
			connectSelectedDatabase(segmentB);
		}

		Database db = listDatabase.get(0);
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

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnection", dependsOnMethods = {
			"postTestTransactionSynchros" })
	public void testTransactionSynchrosWithIndirectConnection(boolean peersInitiallyConnected,
			List<TableEvent<DatabaseRecord>> levents) throws Exception {
		synchronized (TestDecentralizedDatabase.class) {
			testTransactionSynchrosWithIndirectConnection(peersInitiallyConnected, levents, false);
		}
	}

	@Test(dependsOnMethods = { "testTransactionSynchrosWithIndirectConnection" })
	public void preTestTransactionSynchrosWithIndirectConnectionThreaded()
			throws Exception {
		testSynchronisation();
		final Database[] segmentA = new Database[] { listDatabase.get(0), listDatabase.get(1) };
		final Database[] segmentB = new Database[] { listDatabase.get(1), listDatabase.get(2) };
		connectSelectedDatabase(segmentA);
		connectSelectedDatabase(segmentB);

	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods = {
			"preTestTransactionSynchrosWithIndirectConnectionThreaded" })
	public void testTransactionSynchrosWithIndirectConnectionThreaded(List<TableEvent<DatabaseRecord>> levents)
			throws Exception {
		testTransactionSynchrosWithIndirectConnection(true, levents, true);
	}

	@Test(dependsOnMethods = { "testTransactionSynchrosWithIndirectConnectionThreaded" })
	public void postTestTransactionSynchrosWithIndirectConnectionThreaded()
			throws Exception {
		final Database[] segmentA = new Database[] { listDatabase.get(0), listDatabase.get(1) };
		final Database[] segmentB = new Database[] { listDatabase.get(1), listDatabase.get(2) };
		exchangeMessages();
		disconnectSelectedDatabase(segmentA);
		disconnectSelectedDatabase(segmentB);
		disconnectAllDatabase();
		connectAllDatabase();
		exchangeMessages();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();
		testSynchronisation();

	}

	@Test(dependsOnMethods = { "postTestTransactionSynchrosWithIndirectConnectionThreaded" })
	public void testSynchroTransactionTests() throws DatabaseException {
		testSynchronisation();
	}

	@Test(dependsOnMethods = { "testSynchroTransactionTests" })
	public void addNewPeer() throws Exception {
		// TODO add new peer a second time at the end of these tests
		connectAllDatabase();
		testSynchronisation();
		disconnectAllDatabase();
		db4 = new Database(getDatabaseWrapperInstance4());
		listDatabase.add(db4);

		db4.getDbwrapper().getSynchronizer().setNotifier(db4);
		db4.getDbwrapper().setMaxTransactionsToSynchronizeAtTheSameTime(5);
		db4.getDbwrapper().setMaxTransactionEventsKeepedIntoMemory(3);
		db4.getDbwrapper().getSynchronizer().addHookForLocalDatabaseHost(db4.getHostID(),
				TablePointed.class.getPackage());
		Assert.assertTrue(db4.getDbwrapper().getSynchronizer().isInitialized());

		for (Database other : listDatabase) {
			if (other != db4) {
				HookAddRequest har = db4.getDbwrapper().getSynchronizer().askForHookAddingAndSynchronizeDatabase(
						other.getHostID(), false, TablePointed.class.getPackage());
				har = other.getDbwrapper().getSynchronizer().receivedHookAddRequest(har);
				db4.getDbwrapper().getSynchronizer().receivedHookAddRequest(har);
			}
		}

		testAllConnect();
		disconnectAllDatabase();
		checkAllDatabaseInternalDataUsedForSynchro();
		testSynchronisation();

	}

	@Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods = { "addNewPeer" })
	public void testSynchroBetweenTwoPeers2(boolean exceptionDuringTransaction, boolean generateDirectConflict,
			boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenTwoPeers(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods = { "testSynchroBetweenTwoPeers2" })
	public void testSynchroBetweenThreePeers2(boolean exceptionDuringTransaction, boolean generateDirectConflict,
			boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenThreePeers(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected,
				event);
	}

	@Test(dataProvider = "provideDataForIndirectSynchro", dependsOnMethods = { "testSynchroBetweenThreePeers2" })
	public void testIndirectSynchro2(boolean generateDirectConflict, boolean peersInitiallyConnected,
			TableEvent<DatabaseRecord> event) throws Exception {
		testIndirectSynchro(generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataForIndirectSynchroWithIndirectConnection", dependsOnMethods = {
			"testIndirectSynchro2" })
	public void testIndirectSynchroWithIndirectConnection2(boolean generateDirectConflict,
			boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testIndirectSynchroWithIndirectConnection(generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataForTransactionBetweenTwoPeers", dependsOnMethods = {
			"testIndirectSynchroWithIndirectConnection2" })
	public void testTransactionBetweenTwoPeers2(boolean peersInitiallyConnected,
			List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionBetweenTwoPeers(peersInitiallyConnected, levents);
	}

	@Test(dataProvider = "provideDataForTransactionBetweenThreePeers", dependsOnMethods = {
			"testTransactionBetweenTwoPeers2" })
	public void testTransactionBetweenThreePeers2(boolean peersInitiallyConnected,
			List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionBetweenThreePeers(peersInitiallyConnected, levents);
	}

	@Test(dependsOnMethods = { "testTransactionBetweenThreePeers2" })
	public void preTestTransactionSynchros2() throws Exception {
		preTestTransactionSynchros();
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods = {
			"preTestTransactionSynchros2" }, invocationCount = 4)
	public void testTransactionSynchros2(List<TableEvent<DatabaseRecord>> levents)
			throws Exception {
		testTransactionSynchros(levents);
	}

	@Test(dependsOnMethods = { "testTransactionSynchros2" })
	public void postTestTransactionSynchros2() throws Exception {
		postTestTransactionSynchros();
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnection", dependsOnMethods = {
			"postTestTransactionSynchros2" })
	public void testTransactionSynchrosWithIndirectConnection2(boolean peersInitiallyConnected,
			List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionSynchrosWithIndirectConnection(peersInitiallyConnected, levents);
	}

	@Test(dependsOnMethods = { "testTransactionSynchrosWithIndirectConnection2" })
	public void preTestTransactionSynchrosWithIndirectConnectionThreaded2()
			throws Exception {
		preTestTransactionSynchrosWithIndirectConnectionThreaded();
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods = {
			"preTestTransactionSynchrosWithIndirectConnectionThreaded2" })
	public void testTransactionSynchrosWithIndirectConnectionThreaded2(List<TableEvent<DatabaseRecord>> levents)
			throws Exception {
		testTransactionSynchrosWithIndirectConnectionThreaded(levents);
	}

	@Test(dependsOnMethods = { "testTransactionSynchrosWithIndirectConnectionThreaded2" })
	public void postTestTransactionSynchrosWithIndirectConnectionThreaded2()
			throws Exception {
		postTestTransactionSynchrosWithIndirectConnectionThreaded();
	}

	@Test(dependsOnMethods = { "postTestTransactionSynchrosWithIndirectConnectionThreaded2" })
	public void testSynchroTransactionTests2() throws DatabaseException {
		testSynchronisation();
	}

	@Test(dependsOnMethods = { "testSynchroTransactionTests2" })
	public void removeNewPeer() throws Exception {

		for (Database other : listDatabase) {
			if (db4 != other) {
				other.getDbwrapper().getSynchronizer().removeHook(db4.getHostID(), TableAlone.class.getPackage());
				db4.getDbwrapper().getSynchronizer().removeHook(other.getHostID(), TableAlone.class.getPackage());
			}
		}
		db4.getDbwrapper().getSynchronizer().removeHook(db4.getHostID(), TableAlone.class.getPackage());

		unloadDatabase4();

		testAllConnect();
		testSynchronisation();
		disconnectAllDatabase();

	}

	@Test(dataProvider = "provideDataForSynchroBetweenTwoPeers", dependsOnMethods = { "removeNewPeer" })
	public void testSynchroBetweenTwoPeers3(boolean exceptionDuringTransaction, boolean generateDirectConflict,
			boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenTwoPeers(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataSynchroBetweenThreePeers", dependsOnMethods = { "testSynchroBetweenTwoPeers3" })
	public void testSynchroBetweenThreePeers3(boolean exceptionDuringTransaction, boolean generateDirectConflict,
			boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testSynchroBetweenThreePeers(exceptionDuringTransaction, generateDirectConflict, peersInitiallyConnected,
				event);
	}

	@Test(dataProvider = "provideDataForIndirectSynchro", dependsOnMethods = { "testSynchroBetweenThreePeers3" })
	public void testIndirectSynchro3(boolean generateDirectConflict, boolean peersInitiallyConnected,
			TableEvent<DatabaseRecord> event) throws Exception {
		testIndirectSynchro(generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataForIndirectSynchroWithIndirectConnection", dependsOnMethods = {
			"testIndirectSynchro3" })
	public void testIndirectSynchroWithIndirectConnection3(boolean generateDirectConflict,
			boolean peersInitiallyConnected, TableEvent<DatabaseRecord> event)
			throws Exception {
		testIndirectSynchroWithIndirectConnection(generateDirectConflict, peersInitiallyConnected, event);
	}

	@Test(dataProvider = "provideDataForTransactionBetweenTwoPeers", dependsOnMethods = {
			"testIndirectSynchroWithIndirectConnection3" })
	public void testTransactionBetweenTwoPeers3(boolean peersInitiallyConnected,
			List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionBetweenTwoPeers(peersInitiallyConnected, levents);
	}

	@Test(dataProvider = "provideDataForTransactionBetweenThreePeers", dependsOnMethods = {
			"testTransactionBetweenTwoPeers3" })
	public void testTransactionBetweenThreePeers3(boolean peersInitiallyConnected,
			List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionBetweenThreePeers(peersInitiallyConnected, levents);
	}

	@Test(dependsOnMethods = { "testTransactionBetweenThreePeers3" })
	public void preTestTransactionSynchros3() throws Exception {
		preTestTransactionSynchros();
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods = {
			"preTestTransactionSynchros3" }, invocationCount = 4)
	public void testTransactionSynchros3(List<TableEvent<DatabaseRecord>> levents)
			throws Exception {
		testTransactionSynchros(levents);
	}

	@Test(dependsOnMethods = { "testTransactionSynchros3" })
	public void postTestTransactionSynchros3() throws Exception {
		postTestTransactionSynchros();
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnection", dependsOnMethods = {
			"postTestTransactionSynchros3" })
	public void testTransactionSynchrosWithIndirectConnection3(boolean peersInitiallyConnected,
			List<TableEvent<DatabaseRecord>> levents) throws Exception {
		testTransactionSynchrosWithIndirectConnection(peersInitiallyConnected, levents);
	}

	@Test(dependsOnMethods = { "testTransactionSynchrosWithIndirectConnection3" })
	public void preTestTransactionSynchrosWithIndirectConnectionThreaded3()
			throws Exception {
		preTestTransactionSynchrosWithIndirectConnectionThreaded();
	}

	@Test(dataProvider = "provideDataForTransactionSynchrosWithIndirectConnectionThreaded", dependsOnMethods = {
			"preTestTransactionSynchrosWithIndirectConnectionThreaded3" })
	public void testTransactionSynchrosWithIndirectConnectionThreaded3(List<TableEvent<DatabaseRecord>> levents)
			throws Exception {
		testTransactionSynchrosWithIndirectConnectionThreaded(levents);
	}

	@Test(dependsOnMethods = { "testTransactionSynchrosWithIndirectConnectionThreaded3" })
	public void postTestTransactionSynchrosWithIndirectConnectionThreaded3()
			throws Exception {
		postTestTransactionSynchrosWithIndirectConnectionThreaded();
	}

	@Test(dependsOnMethods = { "postTestTransactionSynchrosWithIndirectConnectionThreaded3" })
	public void testSynchroTransactionTests3() throws DatabaseException {
		testSynchronisation();
	}

}
