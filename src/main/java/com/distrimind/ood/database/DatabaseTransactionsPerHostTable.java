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
package com.distrimind.ood.database;

import com.distrimind.ood.database.DatabaseEventsTable.DatabaseEventsIterator;
import com.distrimind.ood.database.DatabaseWrapper.SynchronizationAnomalyType;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.exceptions.*;
import com.distrimind.util.DecentralizedValue;
import com.distrimind.util.Reference;
import com.distrimind.util.io.*;

import java.io.EOFException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.0
 */
final class DatabaseTransactionsPerHostTable extends Table<DatabaseTransactionsPerHostTable.Record> {

	private volatile DatabaseTransactionEventsTable databaseTransactionEventsTable = null;
	private volatile DatabaseHooksTable databaseHooksTable = null;
	private volatile DatabaseEventsTable databaseEventsTable = null;
	private volatile DatabaseDistantTransactionEvent databaseDistantTransactionEventTable = null;
	private volatile DatabaseDistantEventsTable databaseDistantEventsTable = null;
	private volatile IDTable idTable = null;

	@SuppressWarnings("ProtectedMemberInFinalClass")
	protected DatabaseTransactionsPerHostTable() throws DatabaseException {
		super();
	}

	DatabaseTransactionEventsTable getDatabaseTransactionEventsTable() throws DatabaseException {
		if (databaseTransactionEventsTable == null)
			databaseTransactionEventsTable = getDatabaseWrapper()
					.getTableInstance(DatabaseTransactionEventsTable.class);
		return databaseTransactionEventsTable;
	}

	DatabaseEventsTable getDatabaseEventsTable() throws DatabaseException {
		if (databaseEventsTable == null)
			databaseEventsTable = getDatabaseWrapper()
					.getTableInstance(DatabaseEventsTable.class);
		return databaseEventsTable;
	}

	DatabaseHooksTable getDatabaseHooksTable() throws DatabaseException {
		if (databaseHooksTable == null)
			databaseHooksTable = getDatabaseWrapper().getTableInstance(DatabaseHooksTable.class);
		return databaseHooksTable;
	}

	DatabaseDistantTransactionEvent getDatabaseDistantTransactionEvent() throws DatabaseException {
		if (databaseDistantTransactionEventTable == null)
			databaseDistantTransactionEventTable = getDatabaseWrapper()
					.getTableInstance(DatabaseDistantTransactionEvent.class);
		return databaseDistantTransactionEventTable;
	}

	DatabaseDistantEventsTable getDatabaseDistantEventsTable() throws DatabaseException {
		if (databaseDistantEventsTable == null)
			databaseDistantEventsTable = getDatabaseWrapper()
					.getTableInstance(DatabaseDistantEventsTable.class);
		return databaseDistantEventsTable;
	}

	IDTable getIDTable() throws DatabaseException {
		if (idTable == null)
			idTable = getDatabaseWrapper().getTableInstance(IDTable.class);
		return idTable;

	}

	static class Record extends DatabaseRecord {
		@PrimaryKey
		@ForeignKey
		private DatabaseTransactionEventsTable.Record transaction;
		@PrimaryKey
		@ForeignKey
		private DatabaseHooksTable.Record hook;

		void set(DatabaseTransactionEventsTable.Record _transaction, DatabaseHooksTable.Record _hook) {
			transaction = _transaction;
			hook = _hook;
		}

		DatabaseTransactionEventsTable.Record getTransaction() {
			return transaction;
		}

		DatabaseHooksTable.Record getHook() {
			return hook;
		}
	}
	void removeTransactions(final DatabaseHooksTable.Record hook)
			throws DatabaseException {
		if (hook == null)
			throw new NullPointerException("hook");
		getDatabaseWrapper().runTransaction(new Transaction() {

			@Override
			public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {

				DatabaseTransactionsPerHostTable.this
						.removeRecordsWithCascade("hook.id=:id", "id", hook.getID());
				getDatabaseTransactionEventsTable().removeUnusedTransactions();
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
		}, true);

	}
	void removeTransactions(final DatabaseHooksTable.Record hook, final Set<String> removedPackages)
			throws DatabaseException {
		if (hook == null)
			throw new NullPointerException("hook");
		if (removedPackages == null)
			return;
		if (removedPackages.size()==0)
			return;
		getDatabaseWrapper().runTransaction(new Transaction() {

			@Override
			public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {

				DatabaseTransactionsPerHostTable.this
						.removeRecordsWithCascade(new Filter<>() {

							@Override
							public boolean nextRecord(
									com.distrimind.ood.database.DatabaseTransactionsPerHostTable.Record _record) {
								return _record.getTransaction().isConcernedByOneOf(removedPackages);
							}
						}, "hook.id=:id", "id", hook.getID());
				getDatabaseTransactionEventsTable().removeUnusedTransactions();
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
		}, true);

	}

	void removeTransactions(final Set<String> removedPackages)
			throws DatabaseException {
		if (removedPackages == null)
			return;
		if (removedPackages.size()==0)
			return;
		getDatabaseWrapper().runTransaction(new Transaction() {

			@Override
			public Object run(DatabaseWrapper _sql_connection) throws DatabaseException {

				DatabaseTransactionsPerHostTable.this
						.removeRecords(new Filter<>() {

							@Override
							public boolean nextRecord(
									com.distrimind.ood.database.DatabaseTransactionsPerHostTable.Record _record) {
								return _record.getTransaction().isConcernedByOneOf(removedPackages);
							}
						});
				getDatabaseTransactionEventsTable().removeUnusedTransactions();
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
		}, true);

	}

	long validateTransactions(final DatabaseHooksTable.Record hook, final long lastID) throws DatabaseException {
		if (hook == null)
			throw new NullPointerException("hook");

		// if (hook.getLastValidatedTransaction()<lastID)
		{
			return getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<>() {

				@Override
				public Long run() throws Exception {
					removeRecords("transaction.id<=:lastID AND hook=:hook", "lastID", lastID, "hook", hook);
					final AtomicLong actualLastID = new AtomicLong(Long.MAX_VALUE);
					getRecords(new Filter<>() {

						@Override
						public boolean nextRecord(Record _record) {
							if (_record.getTransaction().getID() - 1 < actualLastID.get())
								actualLastID.set(_record.getTransaction().getID() - 1);
							if (actualLastID.get() == lastID)
								this.stopTableParsing();
							return false;
						}

					}, "hook=:hook", "hook", hook);
					if (actualLastID.get() > lastID) {
						getDatabaseDistantTransactionEvent()
								.getRecords(new Filter<>() {

												@Override
												public boolean nextRecord(
														DatabaseDistantTransactionEvent.Record _record)
														throws SerializationDatabaseException {
													if (_record.isConcernedBy(hook.getHostID())) {
														if (_record.getLocalID() - 1 < actualLastID.get())
															actualLastID.set(_record.getLocalID() - 1);
														if (actualLastID.get() == lastID)
															this.stopTableParsing();
													}
													return false;
												}
											}, "localID<:maxLocalID AND localID>=:minLocalID and peersInformedFull=:peersInformedFull",
										"maxLocalID", actualLastID.get() - 1, "minLocalID",
										lastID + 1, "peersInformedFull", Boolean.FALSE);
					}
					if (actualLastID.get() == Long.MAX_VALUE) {
						actualLastID.set(getIDTable().getLastTransactionID());
					} else if (actualLastID.get() < lastID)
						throw new IllegalAccessError();
					hook.setLastValidatedLocalTransactionID(actualLastID.get());
					getDatabaseHooksTable().updateRecord(hook);


					getDatabaseDistantTransactionEvent()
							.updateRecords(new AlterRecordFilter<>() {

								@Override
								public void nextRecord(
										DatabaseDistantTransactionEvent.Record _record)
										throws DatabaseException {
									if (_record.addNewHostIDAndTellsIfNewPeersCanBeConcerned(getDatabaseHooksTable(),
											hook.getHostID())) {
										update();
									} else {
										removeWithCascade();
									}
								}

							}, "localID<=:lastID", "lastID", actualLastID.get());
					getDatabaseTransactionEventsTable().removeTransactionsFromLastID();

					return actualLastID.get();
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
	}

	
	
	DecentralizedValue detectCollisionAndGetObsoleteDistantEventsToRemove(
			final DecentralizedValue comingFrom, final String concernedTable, final DatabaseEventType eventType, final byte[] keys,
			final boolean force,
			final Set<DatabaseDistantTransactionEvent.Record> recordsToRemove)
			throws DatabaseException {
		recordsToRemove.clear();
		if (force)
			return null;
		final Reference<DecentralizedValue> collision = new Reference<>(null);
		Filter<DatabaseDistantEventsTable.Record> filter= new Filter<>() {

			@Override
			public boolean nextRecord(com.distrimind.ood.database.DatabaseDistantEventsTable.Record _record)
					throws DatabaseException {
				if (_record.getTransaction().isConcernedBy(comingFrom)) {
					collision.set(_record.getTransaction().getHook().getHostID());
					recordsToRemove.add(_record.getTransaction());
				}
				return false;
			}
		};
		if (eventType==DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE)
		{
			getDatabaseDistantEventsTable().getRecords(filter, "concernedTable==:concernedTable",
					"concernedTable", concernedTable);
		}
		else
			getDatabaseDistantEventsTable().getRecords(filter, "concernedTable==:concernedTable AND concernedSerializedPrimaryKey==:concernedSerializedPrimaryKey",
					"concernedTable", concernedTable, "concernedSerializedPrimaryKey", keys);

		return collision.get();
	}

	boolean detectCollisionAndGetObsoleteEventsToRemove(final DecentralizedValue comingFrom,
																  final String concernedTable, DatabaseEventType eventType, final byte[] keys, final boolean force,
																  final Set<DatabaseTransactionEventsTable.Record> toRemove)
			throws DatabaseException {

		final Reference<Boolean> collisionDetected = new Reference<>(false);
		Filter<DatabaseEventsTable.Record> filter= new Filter<>() {

			@Override
			public boolean nextRecord(com.distrimind.ood.database.DatabaseEventsTable.Record _record)
					throws DatabaseException {

				for (DatabaseTransactionsPerHostTable.Record rph : DatabaseTransactionsPerHostTable.this
						.getRecordsWithAllFields("transaction", _record.getTransaction())) {
					toRemove.add(rph.getTransaction());
					if (!force) {
						if (rph.getHook().getHostID().equals(comingFrom))
							collisionDetected.set(true);
					}
				}
				return false;
			}
		};
		if (eventType==DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE)
		{
			getDatabaseEventsTable().getRecords(filter, "concernedTable==:concernedTable",
					"concernedTable", concernedTable);
		}
		else {
			getDatabaseEventsTable().getRecords(filter, "concernedTable==:concernedTable AND concernedSerializedPrimaryKey==:concernedSerializedPrimaryKey",
					"concernedTable", concernedTable, "concernedSerializedPrimaryKey", keys);
		}
		return collisionDetected.get();
	}


	void alterDatabase(final DecentralizedValue comingFrom, final RandomInputStream inputStream)
			throws DatabaseException {
		alterDatabase(comingFrom, comingFrom, inputStream);
	}

	private void checkIfAllTablesAreClearedDuringOneCycle(Set<Class<?>> clearedTablesAtOneTime, long transactionUTC, Class<?> tableClass) throws DatabaseException {
		if (clearedTablesAtOneTime.size()>0 && !clearedTablesAtOneTime.iterator().next().getPackage().equals(tableClass.getPackage()))
			clearedTablesAtOneTime.clear();
		clearedTablesAtOneTime.add(tableClass);
		DatabaseConfiguration dc=getDatabaseWrapper().getDatabaseConfiguration(tableClass.getPackage());
		if (dc!=null && dc.getDatabaseSchema().getTableClasses().equals(clearedTablesAtOneTime) )
		{
			getDatabaseWrapper().getDatabaseTable().updateLastRestorationTimeUTCInMS(dc.getDatabaseSchema().getPackage().getName(), transactionUTC);
			clearedTablesAtOneTime.clear();
		}
	}
	private void notClearTableEvent(Set<Class<?>> clearedTablesAtOneTime)
	{
		clearedTablesAtOneTime.clear();
	}
	private void alterDatabase(final DatabaseHooksTable.Record directPeer,
							   final Reference<DatabaseHooksTable.Record> _fromHook,
							   final DatabaseTransactionEventsTable.AbstractRecord transaction,
							   final DatabaseEventsTable.DatabaseEventsIterator iterator, final AtomicLong lastValidatedTransaction,
							   final HashSet<DecentralizedValue> hooksToNotify,
							   final Reference<String>	databasePackage, final boolean acceptTransactionEmpty, boolean comingFromBackup) throws DatabaseException {
		final Set<Class<?>> clearedTablesAtOneTime=new HashSet<>();
		getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

				@SuppressWarnings("unchecked")
				@Override
				public Void run() throws Exception {
					DatabaseNotifier dn=getDatabaseWrapper().getSynchronizer().getNotifier();
					if (dn!=null)
						dn.startNewSynchronizationTransaction();
					try {
						final boolean indirectTransaction = transaction instanceof DatabaseDistantTransactionEvent.Record;
						boolean validatedTransaction = true;
						final Reference<Boolean> obsoleteTransaction=new Reference<>(false);
						boolean transactionNotEmpty = false;


						// Set<AbstractDecentralizedID> hostsDestination=new HashSet<>();


						DatabaseDistantTransactionEvent.Record distantTransaction;

						if (indirectTransaction) {
							distantTransaction = (DatabaseDistantTransactionEvent.Record) transaction;
							_fromHook.set(distantTransaction.getHook());
						} else {
							distantTransaction = new DatabaseDistantTransactionEvent.Record(transaction.getID(),
									getIDTable().getLastTransactionID(), transaction.getTimeUTC(), _fromHook.get(), false, null, false);
							final Set<DecentralizedValue> concernedHosts = ((DatabaseTransactionEventsTable.Record) transaction)
									.getConcernedHosts();
							if (transaction.isForced() || concernedHosts.size() > 0) {

								final Set<DecentralizedValue> l = new HashSet<>();
								Filter<com.distrimind.ood.database.DatabaseHooksTable.Record> f= new Filter<>() {

									@Override
									public boolean nextRecord(com.distrimind.ood.database.DatabaseHooksTable.Record _record) {
										l.add(_record.getHostID());
										return false;
									}

								};
								if (concernedHosts.isEmpty())
									getDatabaseHooksTable().getRecords(f, "concernsDatabaseHost=:cdh or hostID=:dphid", "cdh", true, "dphid", directPeer.getHostID());
								else
									getDatabaseHooksTable().getRecords(f, "concernsDatabaseHost=:cdh or hostID=:dphid or hostID not in :chs", "cdh", true, "dphid", directPeer.getHostID(), "chs", concernedHosts);

								distantTransaction.setPeersInformed(l);
							}
						}
						final DatabaseHooksTable.Record fromHook=_fromHook.get();

						try {
							transactionNotEmpty = getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<>() {

								@Override
								public Boolean run() throws Exception {
									boolean transactionNotEmpty = false;
									boolean validatedTransaction = true;
									try (RandomCacheFileOutputStream cos = RandomCacheFileCenter.getSingleton().getNewBufferedRandomCacheFileOutputStream(true, RandomFileOutputStream.AccessMode.READ_AND_WRITE, BufferedRandomInputStream.DEFAULT_MAX_BUFFER_SIZE, 1)) {

										while (iterator.hasNext()) {
											transactionNotEmpty = true;
											DatabaseEventsTable.AbstractRecord event = iterator.next();

											Table<DatabaseRecord> t;
											Long lastRestorationTimeUTCInMS = null;
											try {
												t = (Table<DatabaseRecord>) getDatabaseWrapper().getTableInstance(event.getConcernedTable());
												if (databasePackage.get() == null) {
													databasePackage.set(t.getClass().getPackage().getName());
													lastRestorationTimeUTCInMS = getDatabaseWrapper().getDatabaseTable().getLastRestorationTimeUTCInMS(databasePackage.get());
												}
											} catch (Exception e) {
												String p = event.getConcernedPackage();
												if (getDatabaseWrapper().getDatabaseConfigurationsBuilder().getConfigurations().getDatabaseConfigurations().stream().noneMatch(c -> c.getDatabaseSchema().getPackage().getName().equals(p))) {
													continue;
												} else
													throw new SerializationDatabaseException("indirectTransaction=" + indirectTransaction, e);

											}
											if (lastRestorationTimeUTCInMS != null && lastRestorationTimeUTCInMS >= transaction.getTimeUTC()) {
												validatedTransaction = false;
												obsoleteTransaction.set(true);
											}
											final DatabaseEventType type = DatabaseEventType.getEnum(event.getType());
											if (type == null)
												throw new SerializationDatabaseException(
														"Impossible to decode database event type : " + event.getType());
											DatabaseRecord drNew = null, drOld = null;
											HashMap<String, Object> mapKeys = new HashMap<>();
											if (validatedTransaction) {
												if (type != DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE) {
													t.deserializePrimaryKeys(mapKeys, event.getConcernedSerializedPrimaryKey());
													if (type.needsNewValue()) {
														drNew = t.getDefaultRecordConstructor().newInstance();
														t.deserializePrimaryKeys(drNew, event.getConcernedSerializedPrimaryKey());
														t.deserializeFields(drNew, event.getConcernedSerializedNewForeignKey(), false, true, false);
														t.deserializeFields(drNew, event.getConcernedSerializedNewNonKey(), false, false, true);
													}

													if (type.hasOldValue() || transaction.isForced()) {
														drOld = t.getRecord(mapKeys);
													}
												}

												if (transaction.getID() <= fromHook.getLastValidatedDistantTransactionID()) {
													validatedTransaction = false;
												}
											}
											boolean eventForce = false;
											if (validatedTransaction) {

												HashSet<DatabaseTransactionEventsTable.Record> r = new HashSet<>();

												boolean collision = detectCollisionAndGetObsoleteEventsToRemove(fromHook.getHostID(),
														event.getConcernedTable(), type, event.getConcernedSerializedPrimaryKey(),
														transaction.isForced(), r);

												Set<DatabaseDistantTransactionEvent.Record> ir = new HashSet<>();
												DecentralizedValue indirectCollisionWith = null;
												if (!collision) {
													indirectCollisionWith = detectCollisionAndGetObsoleteDistantEventsToRemove(
															fromHook.getHostID(), event.getConcernedTable(), type,
															event.getConcernedSerializedPrimaryKey(), transaction.isForced(), ir);
												}


												if (collision || indirectCollisionWith != null) {

													if (!type.hasOldValue() && type != DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE)
														drOld = t.getRecord(mapKeys);
													if (!t.areDuplicatedEventsNotConsideredAsCollisions() || (drOld == drNew || (drNew != null && t.equalsAllFields(drNew, drOld))))
														validatedTransaction = (eventForce = t.collisionDetected(
																fromHook.getHostID(), indirectTransaction ? directPeer.getHostID() : null,
																type, mapKeys, drNew, drOld));

												}
												if (validatedTransaction) {
													for (DatabaseTransactionEventsTable.Record er : r) {
														cos.write(0);
														cos.writeLong(er.id);
													}
													for (DatabaseDistantTransactionEvent.Record er : ir) {
														cos.write(1);
														cos.writeLong(er.id);
													}

												}
											}
											if (validatedTransaction) {
												switch (type) {
													case REMOVE:
													case REMOVE_WITH_CASCADE: {
														if (drOld == null && !comingFromBackup) {
															if (!transaction.isForced() && !eventForce) {

																t.anomalyDetected(fromHook.getHostID(),
																		indirectTransaction ? directPeer.getHostID() : null,
																		SynchronizationAnomalyType.RECORD_TO_REMOVE_NOT_FOUND, mapKeys,
																		null);
																validatedTransaction = false;
															}
														}

													}
													break;
													case UPDATE:
														if (drOld == null && !eventForce && !transaction.isForced() && !comingFromBackup) {

															t.anomalyDetected(fromHook.getHostID(),
																	indirectTransaction ? directPeer.getHostID() : null,
																	SynchronizationAnomalyType.RECORD_TO_UPDATE_NOT_FOUND, mapKeys, drNew);
															validatedTransaction = false;
														}
														break;
													default:
														break;
												}
											}

										}
										cos.write(2);
										if (!validatedTransaction)
											throw new TransactionCanceledException();
										try (RandomInputStream cis = cos.getRandomInputStream()) {
											if (cis.currentPosition() != 0)
												cis.seek(0);
											int next = cis.read();
											while (next != 2) {
												if (next == 0) {
													long id = cis.readLong();
													getDatabaseTransactionEventsTable().removeRecordsWithAllFieldsWithCascade("id", id);
												} else if (next == 1) {
													long id = cis.readLong();
													getDatabaseDistantTransactionEvent().removeRecordsWithAllFieldsWithCascade("id", id);
												} else
													throw new IllegalAccessError();


												next = cis.read();
											}
										}
									}

									return transactionNotEmpty;
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
						} catch (TransactionCanceledException e) {
							validatedTransaction = false;
						}

						if (indirectTransaction) {
							if (distantTransaction.getLocalID() > directPeer.getLastValidatedDistantTransactionID()) {
								if (lastValidatedTransaction.get() > distantTransaction.getLocalID())
									throw new DatabaseException("Transactions must be ordered !");
								lastValidatedTransaction.set(distantTransaction.getLocalID());

								getDatabaseHooksTable().validateLastDistantTransactionIDAndLastTransactionUTC(directPeer, distantTransaction.getLocalID(), distantTransaction.getTimeUTC());


							}
						} else {
							if (transaction.getID() > directPeer.getLastValidatedDistantTransactionID()) {
								if (lastValidatedTransaction.get() > transaction.getID())
									throw new DatabaseException("Transactions must be ordered !");
								lastValidatedTransaction.set(transaction.getID());
							}
						}
						if (getDatabaseHooksTable().validateLastDistantTransactionIDAndLastTransactionUTC(fromHook, transaction.getID(), transaction.getTimeUTC())) {
							if (indirectTransaction)
							{
								hooksToNotify.add(fromHook.getHostID());
							}
						}
						if (!validatedTransaction) {
							if (!obsoleteTransaction.get()) {
								databasePackage.set(null);
							}
							return null;
						}

						boolean distantTransactionAdded = false;
						if (getDatabaseHooksTable().isConcernedByIndirectTransaction(distantTransaction)
								&& distantTransaction.addNewHostIDAndTellsIfNewPeersCanBeConcerned(
								getDatabaseHooksTable(), getDatabaseHooksTable().getLocalDatabaseHost().getHostID())) {
							distantTransaction.setLocalID(getIDTable().getAndIncrementTransactionID());
							distantTransaction = getDatabaseDistantTransactionEvent().addRecord(distantTransaction);
							distantTransactionAdded = true;
						}

						if (transactionNotEmpty || acceptTransactionEmpty) {
							DatabaseTransactionEvent localDTE = new DatabaseTransactionEvent();
							iterator.reset();
							while (iterator.hasNext()) {

								if (localDTE.getEvents().size() > 0 && localDTE.getEvents().size() >= getDatabaseWrapper().getMaxTransactionEventsKeptIntoMemory()) {
									getDatabaseWrapper().getSynchronizer().addNewDatabaseEvent(localDTE);
									localDTE = new DatabaseTransactionEvent();
								}
								DatabaseEventsTable.AbstractRecord event = iterator.next();

								Table<DatabaseRecord> t;
								try {
									t = (Table<DatabaseRecord>) getDatabaseWrapper().getTableInstance(event.getConcernedTable());
								} catch (Exception e) {
									String p=event.getConcernedPackage();
									if (getDatabaseWrapper().getDatabaseConfigurationsBuilder().getConfigurations().getDatabaseConfigurations().stream().noneMatch(c -> c.getDatabaseSchema().getPackage().getName().equals(p))) {
										continue;
									}
									else
										throw new SerializationDatabaseException("", e);
								}
								final Table<DatabaseRecord> table=t;

								DatabaseEventType type = DatabaseEventType.getEnum(event.getType());
								if (type == null)
									throw new SerializationDatabaseException(
											"Impossible to decode database event type : " + event.getType());
								DatabaseRecord drNew = null, drOld=null;
								HashMap<String, Object> mapKeys = new HashMap<>();
								if (type!=DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE) {
									table.deserializePrimaryKeys(mapKeys, event.getConcernedSerializedPrimaryKey());
									if (type.needsNewValue()) {
										drNew = table.getDefaultRecordConstructor().newInstance();
										table.deserializePrimaryKeys(drNew, event.getConcernedSerializedPrimaryKey());
										table.deserializeFields(drNew, event.getConcernedSerializedNewForeignKey(), false, true, false);
										table.deserializeFields(drNew, event.getConcernedSerializedNewNonKey(), false, false, true);
									}

									drOld = table.getRecord(mapKeys);
								}

								TableEvent<DatabaseRecord> addedEvent = null;
								switch (type) {
									case REMOVE_ALL_RECORDS_WITH_CASCADE : localDTE.addEvent(addedEvent = new TableEvent<>(-1, type, table, null, null, null));break;
									case ADD : {
										if (drOld != null)
											localDTE.addEvent(addedEvent = new TableEvent<>(-1, type, table, drOld, drNew, null, null, true));
										else
											localDTE.addEvent(addedEvent = new TableEvent<>(-1, type, table, null, drNew, null));
										break;
									}
									case REMOVE: case REMOVE_WITH_CASCADE : {
										if (drOld == null) {
											localDTE.addEvent(addedEvent =
													new TableEvent<>(-1, type, table, null, drNew, null, mapKeys, false));
										} else {
											localDTE.addEvent(addedEvent = new TableEvent<>(-1, type, table, drOld, drNew, null));
										}
										break;
									}
									case UPDATE : localDTE.addEvent(addedEvent = new TableEvent<>(-1, type, table, drOld, drNew, null));break;
								}

								final boolean transactionToResendFinal = false;

								if (distantTransactionAdded) {
									DatabaseDistantEventsTable.Record e;
									if (indirectTransaction)
										e = (DatabaseDistantEventsTable.Record) event;
									else
										e = new DatabaseDistantEventsTable.Record(event, distantTransaction);
									e.setTransaction(distantTransaction);
									getDatabaseDistantEventsTable().addRecord(e);
								}
								if (addedEvent.getType()==DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE)
									checkIfAllTablesAreClearedDuringOneCycle(clearedTablesAtOneTime, transaction.getTimeUTC(), addedEvent.getTable().getClass());
								else
									notClearTableEvent(clearedTablesAtOneTime);
								switch (addedEvent.getType()) {
									case ADD:
										if (addedEvent.isOldAlreadyPresent()) {
											try {
												addedEvent.getTable().updateUntypedRecord(addedEvent.getNewDatabaseRecord(), false, null);
											} catch (ConstraintsNotRespectedDatabaseException | FieldDatabaseException | RecordNotFoundDatabaseException e) {

												addedEvent.getTable().anomalyDetected(fromHook.getHostID(),
														indirectTransaction ? directPeer.getHostID() : null,
														SynchronizationAnomalyType.RECORD_TO_ADD_HAS_INCOMPATIBLE_PRIMARY_KEYS,
														addedEvent.getMapKeys(),
														addedEvent.getNewDatabaseRecord());
											}
										} else {
											try {
												addedEvent.getTable().addUntypedRecord(addedEvent.getNewDatabaseRecord(),
														true, transactionToResendFinal, null);

											} catch (ConstraintsNotRespectedDatabaseException e) {

												addedEvent.getTable().anomalyDetected(fromHook.getHostID(),
														indirectTransaction ? directPeer.getHostID() : null,
														SynchronizationAnomalyType.RECORD_TO_ADD_ALREADY_PRESENT,
														addedEvent.getMapKeys(),
														addedEvent.getNewDatabaseRecord());
											} catch (FieldDatabaseException | RecordNotFoundDatabaseException e) {

												addedEvent.getTable().anomalyDetected(fromHook.getHostID(),
														indirectTransaction ? directPeer.getHostID() : null,
														SynchronizationAnomalyType.RECORD_TO_ADD_HAS_INCOMPATIBLE_PRIMARY_KEYS,
														addedEvent.getMapKeys(),
														addedEvent.getNewDatabaseRecord());
											}
										}
										break;
									case REMOVE:
										if (addedEvent.getOldDatabaseRecord() != null) {
											try {
												addedEvent.getTable().removeUntypedRecord(addedEvent.getOldDatabaseRecord(),
														transactionToResendFinal, null);
											} catch (ConstraintsNotRespectedDatabaseException e) {

												addedEvent.getTable().anomalyDetected(fromHook.getHostID(),
														indirectTransaction ? directPeer.getHostID() : null,
														SynchronizationAnomalyType.RECORD_TO_REMOVE_HAS_DEPENDENCIES,
														addedEvent.getMapKeys(),
														addedEvent.getNewDatabaseRecord());
											} catch (RecordNotFoundDatabaseException e) {

												addedEvent.getTable().anomalyDetected(fromHook.getHostID(),
														indirectTransaction ? directPeer.getHostID() : null,
														SynchronizationAnomalyType.RECORD_TO_REMOVE_NOT_FOUND,
														addedEvent.getMapKeys(),
														addedEvent.getNewDatabaseRecord());
											}
										}
										break;
									case REMOVE_WITH_CASCADE:
										if (addedEvent.getOldDatabaseRecord() != null) {
											try {
												addedEvent.getTable().removeUntypedRecordWithCascade(addedEvent.getOldDatabaseRecord(), transactionToResendFinal);
											} catch (RecordNotFoundDatabaseException e) {

												addedEvent.getTable().anomalyDetected(fromHook.getHostID(),
														indirectTransaction ? directPeer.getHostID() : null,
														SynchronizationAnomalyType.RECORD_TO_REMOVE_NOT_FOUND,
														addedEvent.getMapKeys(),
														addedEvent.getNewDatabaseRecord());
											}
										}
										break;
									case UPDATE:
										if (addedEvent.getOldDatabaseRecord() == null) {
											try {
												addedEvent.getTable().addUntypedRecord(addedEvent.getNewDatabaseRecord(), true, false, null);
											} catch (ConstraintsNotRespectedDatabaseException e) {

												addedEvent.getTable().anomalyDetected(fromHook.getHostID(),
														indirectTransaction ? directPeer.getHostID() : null,
														SynchronizationAnomalyType.RECORD_TO_ADD_ALREADY_PRESENT,
														addedEvent.getMapKeys(),
														addedEvent.getNewDatabaseRecord());
											} catch (FieldDatabaseException | RecordNotFoundDatabaseException e) {

												addedEvent.getTable().anomalyDetected(fromHook.getHostID(),
														indirectTransaction ? directPeer.getHostID() : null,
														SynchronizationAnomalyType.RECORD_TO_ADD_HAS_INCOMPATIBLE_PRIMARY_KEYS,
														addedEvent.getMapKeys(),
														addedEvent.getNewDatabaseRecord());
											}

										} else {
											try {
												addedEvent.getTable().updateUntypedRecord(addedEvent.getNewDatabaseRecord(),
														transactionToResendFinal, null);
											} catch (ConstraintsNotRespectedDatabaseException | FieldDatabaseException | RecordNotFoundDatabaseException e) {

												addedEvent.getTable().anomalyDetected(fromHook.getHostID(),
														indirectTransaction ? directPeer.getHostID() : null,
														SynchronizationAnomalyType.RECORD_TO_UPDATE_HAS_INCOMPATIBLE_PRIMARY_KEYS,
														addedEvent.getMapKeys(),
														addedEvent.getNewDatabaseRecord());
											}
										}
										break;
									case REMOVE_ALL_RECORDS_WITH_CASCADE: {

										addedEvent.getTable().removeAllRecordsWithCascade(false);
									}
										break;
								}

							}


							if (localDTE.getEvents().size() > 0) {
								getDatabaseWrapper().getSynchronizer().addNewDatabaseEvent(localDTE);
							}

						} else {
							databasePackage.set(null);
							throw new SerializationDatabaseException("The transaction should not be empty");
						}

						return null;
					}
					finally
					{
						if (dn!=null)
							dn.endSynchronizationTransaction();
					}
				}

				@Override
				public TransactionIsolation getTransactionIsolation() {
					return TransactionIsolation.TRANSACTION_SERIALIZABLE;
				}

				@Override
				public boolean doesWriteData() {
					return true;
				}

				private boolean oneCycleDone = false;

				@Override
				public void initOrReset() throws IOException {
					if (oneCycleDone)
						iterator.reset();
					else
						oneCycleDone = true;
					hooksToNotify.clear();
				}

			});
	}


	private void alterDatabase(final DecentralizedValue directPeerID, final DecentralizedValue comingFrom,
							   final RandomInputStream ois) throws DatabaseException {

		if (comingFrom == null)
			throw new NullPointerException("comingFrom");
		if (ois == null)
			throw new NullPointerException("inputStream");
		if (directPeerID == null)
			throw new NullPointerException("directPeerID");
		if (comingFrom.equals(getDatabaseHooksTable().getLocalDatabaseHost().getHostID()))
			throw new IllegalArgumentException(
					"The given distant host ID cannot be equals to the local host ID : " + comingFrom);

		final DatabaseHooksTable.Record directPeer = getDatabaseHooksTable().getHook(comingFrom);
		if (directPeer==null)
			throw new SerializationDatabaseException("The give host id is not valid : " + comingFrom);

		final AtomicLong lastValidatedTransaction = new AtomicLong(-1);
		final HashSet<DecentralizedValue> hooksToNotify = new HashSet<>();
		final Set<String> lastValidatedIDPerPackages=new HashSet<>();
		try  {
			final AtomicInteger next = new AtomicInteger(ois.readByte());
			while (next.get() != EXPORT_FINISHED) {
				DatabaseEventsIterator it=null;

				try
				{
					Reference<String> packageString=new Reference<>();
					if (next.get() == EXPORT_INDIRECT_TRANSACTION) {
						DatabaseDistantTransactionEvent.Record ite = getDatabaseDistantTransactionEvent()
								.unserializeDistantTransactionEvent(ois);

						alterDatabase(directPeer, new Reference<>(),
								ite, it=getDatabaseDistantEventsTable().distantEventTableIterator(ois),
								lastValidatedTransaction, hooksToNotify, packageString, false, false);

					} else if (next.get() == EXPORT_DIRECT_TRANSACTION) {
						DatabaseTransactionEventsTable.Record dte = getDatabaseTransactionEventsTable().unserialize(ois,
								true, false);
						alterDatabase(directPeer, new Reference<>(directPeer),
								dte, it=getDatabaseEventsTable().eventsTableIterator(ois), lastValidatedTransaction,
								hooksToNotify, packageString, false, false);
						if (packageString.get()==null)
							hooksToNotify.add(comingFrom);
					}
					if (packageString.get()!=null)
						lastValidatedIDPerPackages.add(packageString.get());


				}
				finally
				{
					if (it!=null)
						it.close();
				}
				next.set(ois.readByte());
			}
		} catch (EOFException e) {
			throw new SerializationDatabaseException("Unexpected EOF", e);
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		} finally {
			hooksToNotify.add(comingFrom);
			for (String p : lastValidatedIDPerPackages) {
				getDatabaseWrapper().getSynchronizer()
						.addNewTransactionConfirmationEvents(comingFrom, p, lastValidatedTransaction.get());
			}
			for (DecentralizedValue id : hooksToNotify) {
				DatabaseHooksTable.Record h = getDatabaseHooksTable().getHook(id);
				getDatabaseWrapper().getSynchronizer().sendLastValidatedIDIfConnected(h);
			}
		}

	}

	boolean alterDatabaseFromBackup(final String databasePackage, final DecentralizedValue comingFrom,
									final RandomInputStream ois, boolean referenceFile) throws DatabaseException {

		if (comingFrom == null)
			throw new NullPointerException("comingFrom");
		if (ois == null)
			throw new NullPointerException("inputStream");
		if (comingFrom.equals(getDatabaseHooksTable().getLocalDatabaseHost().getHostID()))
			throw new IllegalArgumentException(
					"The given distant host ID cannot be equals to the local host ID : " + comingFrom);




		final AtomicLong lastValidatedTransaction = new AtomicLong(-1);
		final HashSet<DecentralizedValue> hooksToNotify = new HashSet<>();

		DecentralizedValue hostID=getDatabaseWrapper().getSynchronizer().getLocalHostID();
		if (hostID==null)
			throw new DatabaseException("No local host id defined for database synchronization");
		if (comingFrom.equals(hostID))
			throw new DatabaseException("The distant ID cannot be equal to the local host ID");
		ArrayList<DatabaseHooksTable.Record> hooks = getDatabaseHooksTable().getRecordsWithAllFields("hostID",
				comingFrom);
		if (hooks.isEmpty())
			throw new SerializationDatabaseException("The give host id is not valid : " + comingFrom);
		else if (hooks.size() > 1)
			throw new IllegalAccessError();
		final DatabaseHooksTable.Record comingFromRecord = hooks.get(0);

		long lastDistantTransactionID=getDatabaseWrapper().getSynchronizer().getLastValidatedDistantIDSynchronization(comingFrom);
		Long lastRestorationTimeUTCInMS=getDatabaseWrapper().getDatabaseTable().getLastRestorationTimeUTCInMS(databasePackage);
		final Set<Class<?>> clearedTablesAtOneTime=new HashSet<>();

		try  {
			ois.skipNBytes(8);

			if (!ois.readBoolean())
				return false;//null;
			long startTransactionID=ois.readLong();
			long endTransactionID=ois.readLong();
			if (startTransactionID>lastDistantTransactionID+1 || endTransactionID<=lastDistantTransactionID)
				return false;//new DatabaseWrapper.TransactionsInterval(startTransactionID, endTransactionID);
			List<Class<? extends Table<?>>> fileListTables=null;
			if (referenceFile)
				fileListTables=BackupRestoreManager.extractClassesList(ois);
			BackupRestoreManager.positionForDataRead(ois, referenceFile);
			while(ois.available()>0) {
				int nextTransactionPosition = ois.readInt();
				if (nextTransactionPosition==-1)
					break;
				if (nextTransactionPosition<=ois.currentPosition())
					throw new DatabaseException("Invalid data");
				long transactionID;

				if (!(ois.readBoolean()) || (transactionID=ois.readLong())<=lastDistantTransactionID)
				{
					ois.seek(nextTransactionPosition);
					continue;
				}
				long transactionUTC=ois.readLong();
				if (lastRestorationTimeUTCInMS!=null && lastRestorationTimeUTCInMS>=transactionUTC) {
					lastDistantTransactionID=transactionID;
					getDatabaseHooksTable().validateLastDistantTransactionIDAndLastTransactionUTC(comingFromRecord, transactionID, transactionUTC);
					ois.seek(nextTransactionPosition);
					continue;
				}
				if (ois.readInt()!=-1)
					throw new DatabaseException("Invalid data");
				final DatabaseTransactionEventsTable.Record dte=new DatabaseTransactionEventsTable.Record(transactionID, transactionUTC, databasePackage);
				List<Class<? extends Table<?>>> classes=getDatabaseWrapper().getDatabaseConfiguration(databasePackage).getDatabaseSchema().getSortedTableClasses();
				if (fileListTables!=null)
				{
					if (!fileListTables.equals(classes))
						return false;
				}
				final ArrayList<Table<?>> tables=new ArrayList<>(classes.size());
				for (Class<? extends Table<?>> c : classes) {
					Table<?> t = getDatabaseWrapper().getTableInstance(c);
					tables.add(t);
				}

				final long positionOffset=ois.currentPosition();
				DatabaseEventsTable.DatabaseEventsIterator it=new DatabaseEventsTable.DatabaseEventsIterator(new LimitedRandomInputStream(ois, positionOffset), false){
					Byte eventTypeByte;
					int position=0;
					//private final byte[] recordBuffer=new byte[1<<24-1];
					@Override
					public DatabaseEventsTable.AbstractRecord next() throws DatabaseException {

						try {
							long startRecord=getDataInputStream().currentPosition()+positionOffset-1;
							DatabaseEventType eventType = DatabaseEventType.getEnum(eventTypeByte);
							if (eventType == null)
								throw new IOException();

							int tableIndex = getDataInputStream().readUnsignedShort();
							if (tableIndex >= tables.size())
								throw new IOException();
							Table<?> table = tables.get(tableIndex);
							DatabaseEventsTable.Record event = new DatabaseEventsTable.Record();
							event.setConcernedTable(table.getClass().getName());
							event.setPosition(position++);

							event.setType(eventTypeByte);
							event.setTransaction(dte);
							if (eventType!=DatabaseEventType.REMOVE_ALL_RECORDS_WITH_CASCADE) {
								notClearTableEvent(clearedTablesAtOneTime);
								event.setConcernedSerializedPrimaryKey(getDataInputStream().readBytesArray(false, Table.MAX_PRIMARY_KEYS_SIZE_IN_BYTES));
								switch (eventType) {
									case ADD:
									case UPDATE: {
										if (getDataInputStream().readBoolean()) {
											event.setConcernedSerializedNewForeignKey(getDataInputStream().readBytesArray(false, Table.MAX_PRIMARY_KEYS_SIZE_IN_BYTES));
										} else
											event.setConcernedSerializedNewForeignKey(new byte[0]);

										if (getDataInputStream().readBoolean()) {
											event.setConcernedSerializedNewNonKey(getDataInputStream().readBytesArray(true, Table.MAX_NON_KEYS_SIZE_IN_BYTES));
										}

									}

									break;
									case REMOVE:
									case REMOVE_WITH_CASCADE:
										break;
									default:
										throw new IllegalAccessError();


								}
							}
							else
								checkIfAllTablesAreClearedDuringOneCycle(clearedTablesAtOneTime, transactionUTC, table.getClass());

							int previous=getDataInputStream().readInt();
							if (previous != startRecord)
								throw new IOException("previous="+previous+", startRecord="+startRecord);
							return event;
						}
						catch (IOException e)
						{
							throw DatabaseException.getDatabaseException(e);
						}
						finally {
							eventTypeByte=null;
						}
					}

					@Override
					public boolean hasNext() throws DatabaseException {
						try {
							if (eventTypeByte==null) {
								eventTypeByte = getDataInputStream().readByte();
								setNextEvent(eventTypeByte);

							}

							return eventTypeByte!=-1;
						} catch (IOException e) {
							throw DatabaseException.getDatabaseException(e);
						}
					}

					@Override
					public void close() throws IOException {
						super.close();
						eventTypeByte=null;
					}

					@Override
					public void reset() throws IOException {
						super.reset();
						eventTypeByte=null;
					}
				};
				try {
					alterDatabase(comingFromRecord, new Reference<>(comingFromRecord),
							dte, it, lastValidatedTransaction,
							hooksToNotify, new Reference<>(), true, true);
					lastDistantTransactionID=transactionID;
					ois.seek(nextTransactionPosition);
				}
				finally {
					it.close();
				}
			}
			return lastValidatedTransaction.get()>=0;

		} catch (EOFException e) {
			try {
				ois.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
			throw new SerializationDatabaseException("Unexpected EOF", e);
		} catch (Exception e) {
			try {
				ois.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
			throw DatabaseException.getDatabaseException(e);
		} finally {
			hooksToNotify.add(comingFrom);
			if (lastValidatedTransaction.get() != -1) {
				getDatabaseWrapper().getSynchronizer()
						.addNewTransactionConfirmationEvents(comingFrom, databasePackage, lastValidatedTransaction.get());
			}

			for (DecentralizedValue id : hooksToNotify) {
				DatabaseHooksTable.Record h = getDatabaseHooksTable().getHook(id);
				getDatabaseWrapper().getSynchronizer().sendLastValidatedIDIfConnected(h);
			}
		}

	}

	int exportTransactions(final RandomOutputStream oos, final int hookID, final int maxEventsRecords)
			throws DatabaseException {

		final AtomicInteger number = new AtomicInteger(0);

		final AtomicLong nearNextLocalID = new AtomicLong();
		final DatabaseHooksTable.Record hook = getDatabaseHooksTable().getRecord("id", hookID);

		if (hook == null)
			return 0;
		long currentTransactionID = hook.getLastValidatedLocalTransactionID();

		try {
			number.set(getDatabaseDistantTransactionEvent().exportTransactions(oos, hook, maxEventsRecords,
					currentTransactionID, nearNextLocalID));
			try {
				if (number.get() >= maxEventsRecords)
					return number.get();
				do {
					getOrderedRecords(new Filter<>() {

										  @Override
										  public boolean nextRecord(Record _record) throws DatabaseException {
											  try {
												  oos.writeByte(EXPORT_DIRECT_TRANSACTION);
												  getDatabaseTransactionEventsTable().serialize(_record.getTransaction(), oos, true,
														  false);
												  getDatabaseEventsTable().getOrderedRecords(new Filter<>() {

																								 @Override
																								 public boolean nextRecord(
																										 com.distrimind.ood.database.DatabaseEventsTable.Record _record)
																										 throws DatabaseException {
																									 _record.export(oos);
																									 return false;
																								 }

																							 }, "transaction=:transaction", new Object[]{"transaction", _record.getTransaction()},
														  true, "position");
												  oos.writeByte(EXPORT_DIRECT_TRANSACTION_FINISHED);
												  if (number.incrementAndGet() >= maxEventsRecords) {
													  this.stopTableParsing();
													  return false;
												  }
												  return false;
											  } catch (IOException e) {
												  throw DatabaseException.getDatabaseException(e);
											  }
										  }
									  }, "transaction.id<:nearNextLocalID AND transaction.id>:previousNearTransactionID AND hook=:hook",
							new Object[] { "nearNextLocalID", nearNextLocalID.get(),
									"previousNearTransactionID", currentTransactionID, "hook", hook },
							true, "transaction.id");
					currentTransactionID = nearNextLocalID.get();

					if (number.get() < maxEventsRecords && currentTransactionID != Long.MAX_VALUE)
						number.set(number.get() + getDatabaseDistantTransactionEvent().exportTransactions(oos, hook,
								maxEventsRecords - number.get(), currentTransactionID, nearNextLocalID));
				} while (number.get() < maxEventsRecords && nearNextLocalID.get() != currentTransactionID);
			} finally {
				oos.writeByte(EXPORT_FINISHED);
			}
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

		return number.get();

	}

	static final byte EXPORT_FINISHED = 1;
	static final byte EXPORT_DIRECT_TRANSACTION = 2;
	static final byte EXPORT_DIRECT_TRANSACTION_EVENT = 4;
	static final byte EXPORT_DIRECT_TRANSACTION_FINISHED = EXPORT_DIRECT_TRANSACTION | EXPORT_FINISHED;
	static final byte EXPORT_INDIRECT_TRANSACTION = 8;
	static final byte EXPORT_INDIRECT_TRANSACTION_EVENT = 16;
	static final byte EXPORT_INDIRECT_TRANSACTION_FINISHED = EXPORT_INDIRECT_TRANSACTION | EXPORT_FINISHED;
}