package com.distrimind.ood.database;

public enum HSQLDBConcurrencyControl {
	/**
	 * The two-phase locking model is the default mode. It is referred to by the
	 * keyword, LOCKS. In the 2PL model, each table that is read by a transaction is
	 * locked with a shared lock (read lock), and each table that is written to is
	 * locked with an exclusive lock (write lock). If two sessions read and modify
	 * different tables then both go through simultaneously. If one session tries to
	 * lock a table that has been locked by the other, if both locks are shared
	 * locks, it will go ahead. If either of the locks is an exclusive lock, the
	 * engine will put the session in wait until the other session commits or rolls
	 * back its transaction. The engine will throw an error if the action would
	 * result in deadlock.
	 * 
	 * HyperSQL also supports explicit locking of a group of tables for the duration
	 * of the current transaction. Use of this command blocks access to the locked
	 * tables by other sessions and ensures the current session can complete the
	 * intended reads and writes on the locked tables.
	 * 
	 * If a table is read-only, it will not be locked by any transaction.
	 * 
	 * The READ UNCOMMITTED isolation level can be used in 2PL modes for read-only
	 * operations. It is the same as READ COMMITTED plus read only.
	 * 
	 * The READ COMMITTED isolation level is the default. It keeps write locks on
	 * tables until commit, but releases the read locks after each operation.
	 * 
	 * The REPEATABLE READ level is upgraded to SERIALIZABLE. These levels keep both
	 * read and write locks on tables until commit.
	 * 
	 * It is possible to perform some critical operations at the SERIALIZABLE level,
	 * while the rest of the operations are performed at the READ COMMITTED level.
	 * 
	 * Note: two phase locking refers to two periods in the life of a transaction.
	 * In the first period, locks are acquired, in the second period locks are
	 * released. No new lock is acquired after releasing a lock.
	 * 
	 */
	TWO_PHASE_LOCKING_MODEL("LOCKS"),

	/**
	 * This model is referred to as MVLOCKS. It works the same way as normal 2PL as
	 * far as updates are concerned.
	 * 
	 * SNAPSHOT ISOLATION is a multiversion concurrency strategy which uses the
	 * snapshot of the whole database at the time of the start of the transaction.
	 * In this model, read only transactions use SNAPSHOT ISOLATION. While other
	 * sessions are busy changing the database, the read only session sees a
	 * consistent view of the database and can access all the tables even when they
	 * are locked by other sessions for updates.
	 * 
	 * There are many applications for this mode of operation. In heavily updated
	 * data sets, this mode allows uninterrupted read access to the data.
	 */
	MULTI_VERSION_ROWS_CONTROL("MVLOCKS"),

	/**
	 * When multiple connections are used to access the database, the transaction
	 * manager controls their activities. When each transaction performs only reads
	 * or writes on a single table, there is no contention. Each transaction waits
	 * until it can obtain a lock then performs the operation and commits.
	 * Contentions occur when transactions perform reads and writes on more than one
	 * table, or perform a read, followed by a write, on the same table.
	 * 
	 * For example, when sessions are working at the SERIALIZABLE level, when
	 * multiple sessions first read from a table in order to check if a row exists,
	 * then insert a row into the same table when it doesn't exist, there will be
	 * regular contention. Transaction A reads from the table, then does Transaction
	 * B. Now if either Transaction A or B attempts to insert a row, it will have to
	 * be terminated as the other transaction holds a shared lock on the table. If
	 * instead of two operations, a single MERGE statement is used to perform the
	 * read and write, no contention occurs because both locks are obtained at the
	 * same time.
	 * 
	 * Alternatively, there is the option of obtaining the necessary locks with an
	 * explicit LOCK TABLE statement. This statement should be executed before other
	 * statements and should include the names of all the tables and the locks
	 * needed. After this statement, all the other statements in the transaction can
	 * be executed and the transaction committed. The commit will remove all the
	 * locks.
	 * 
	 * HyperSQL detects deadlocks before attempting to execute a statement. When a
	 * lock is released after the completion of the statement, the first transaction
	 * that is waiting for the lock is allowed to continue.
	 * 
	 * HyperSQL is fully multi threaded. It therefore allows different transactions
	 * to execute concurrently so long as they are not waiting to lock the same
	 * table for write.
	 */
	MULTI_VERSION_CONCURRENCY_CONTROL("MVCC");

	private final String code;

	private HSQLDBConcurrencyControl(String code) {
		this.code = code;
	}

	public String getCode() {
		return code;
	}

	public static HSQLDBConcurrencyControl DEFAULT = MULTI_VERSION_CONCURRENCY_CONTROL;

}
