package com.distrimind.ood.database.tasks;
/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

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

import com.distrimind.ood.database.*;
import com.distrimind.ood.database.annotations.DatabasePeriodicTask;
import com.distrimind.ood.database.annotations.DatabasePeriodicTasks;
import com.distrimind.ood.database.annotations.TablePeriodicTask;
import com.distrimind.ood.database.annotations.TablePeriodicTasks;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.concurrent.ScheduledPoolExecutor;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.util.TreeSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.2.0
 */
public class DatabaseTasksManager {
	private final TreeSet<AbstractS<?>> tasks=new TreeSet<>();
	private final DatabaseWrapper databaseWrapper;
	private final ScheduledPoolExecutor threadPoolExecutor;
	private ScheduledFuture<?> scheduledFuture=null;
	private final ExecutedTasksTable executedTasksTable;
	private Package currentDatabasePackageNameLoading=null;

	DatabaseTasksManager(DatabaseWrapper databaseWrapper) throws DatabaseException {
		ScheduledPoolExecutor threadPoolExecutor=databaseWrapper.getDefaultPoolExecutor();
		if (threadPoolExecutor==null)
			threadPoolExecutor=new ScheduledPoolExecutor(1, 1, 10, TimeUnit.MINUTES);
		this.threadPoolExecutor=threadPoolExecutor;
		this.databaseWrapper = databaseWrapper;
		this.executedTasksTable=databaseWrapper.getTableInstance(ExecutedTasksTable.class);
	}

	private abstract class AbstractS<T extends ITaskStrategy> implements Comparable<AbstractS<T>>
	{
		protected final AbstractScheduledTask scheduledTask;
		protected final T strategy;
		protected final long timeUTCOfExecution;


		protected AbstractS(AbstractScheduledTask scheduledTask, long timeUTCOfExecution) throws DatabaseException {
			if (scheduledTask==null)
				throw new NullPointerException();
			this.scheduledTask = scheduledTask;
			try {
				//noinspection unchecked
				strategy=(T)scheduledTask.getStrategyClass().getConstructor().newInstance();
			} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				throw new DatabaseException("Impossible to instantiate "+scheduledTask.getStrategyClass(), e);
			}
			this.timeUTCOfExecution=timeUTCOfExecution;
		}
		protected long getNextTimeUTCOfExecution() throws DatabaseException {
			if (this.scheduledTask instanceof ScheduledPeriodicTask)
			{
				final ScheduledPeriodicTask scheduledPeriodicTask =(ScheduledPeriodicTask)scheduledTask;
				return databaseWrapper.runSynchronizedTransaction(new SynchronizedTransaction<Long>() {
					@Override
					public Long run() throws Exception {
						ExecutedTasksTable.Record r=executedTasksTable.getRecord("strategyClassName", scheduledPeriodicTask.getStrategyClass());
						if (r==null)
						{
							return Long.MIN_VALUE;
						}
						else {
							r.setLastExecutionTimeUTC(System.currentTimeMillis());
							executedTasksTable.updateRecord(r, "lastExecutionTimeUTC", r.getLastExecutionTimeUTC());
							long n= scheduledPeriodicTask.getNextOccurrenceInTimeUTCAfter(r.getLastExecutionTimeUTC());
							if (n>scheduledPeriodicTask.getEndTimeUTCInMs()) {
								executedTasksTable.removeRecord(r);
								return Long.MIN_VALUE;
							}
							else
								return n;
						}


					}

					@Override
					public TransactionIsolation getTransactionIsolation() {
						return TransactionIsolation.TRANSACTION_REPEATABLE_READ;
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
			else
				return Long.MIN_VALUE;
		}
		abstract AbstractS<T> getNextCycle() throws DatabaseException;

		protected abstract void run();

		@Override
		public int compareTo(AbstractS<T> o) {
			return Long.compare(this.timeUTCOfExecution, o.timeUTCOfExecution);
		}
	}
	private class DS extends AbstractS<IDatabaseTaskStrategy>
	{
		protected DS(AbstractScheduledTask scheduledTask, long timeUTCOfExecution) throws DatabaseException {
			super(scheduledTask, timeUTCOfExecution);
		}

		@Override
		protected void run() {
			strategy.launchTask(databaseWrapper);
		}

		@Override
		DS getNextCycle() throws DatabaseException {
			if (this.scheduledTask instanceof ScheduledPeriodicTask)
			{
				long n=getNextTimeUTCOfExecution();
				if (n!=Long.MIN_VALUE)
					return new DS(scheduledTask, n);
			}
			return null;
		}
	}
	private class TS extends AbstractS<ITableTaskStrategy<?>>
	{
		private final Table<?> table;
		protected TS(AbstractScheduledTask scheduledTask, long timeUTCOfExecution, Table<?> table) throws DatabaseException {
			super(scheduledTask, timeUTCOfExecution);
			if (table==null)
				throw new NullPointerException();
			this.table=table;
		}

		@Override
		protected void run() {
			strategy.launchTaskWithUntypedTable(table);
		}
		@Override
		TS getNextCycle() throws DatabaseException {
			if (this.scheduledTask instanceof ScheduledPeriodicTask)
			{
				long n=getNextTimeUTCOfExecution();
				if (n!=Long.MIN_VALUE)
					return new TS(scheduledTask, n, table);
			}
			return null;
		}
	}
	private ScheduledPoolExecutor getThreadPoolExecutor()
	{
		return threadPoolExecutor;
	}
	private void scheduleNextTask()
	{
		ScheduledPoolExecutor p=getThreadPoolExecutor();
		scheduledFuture=p.schedule(() -> {
			AbstractS<?> task;
			synchronized (DatabaseTasksManager.this)
			{
				scheduledFuture=null;
				task=tasks.pollFirst();
				if (task==null) {
					return;
				}
			}
			task.run();
			try {
				task=task.getNextCycle();
				if (task!=null)
				{
					synchronized (DatabaseTasksManager.this)
					{
						tasks.add(task);
						scheduleNextTask();
					}
				}
			} catch (DatabaseException e) {
				e.printStackTrace();
			}

		}, System.currentTimeMillis() - tasks.first().timeUTCOfExecution, TimeUnit.MILLISECONDS);
		if (scheduledFuture.isDone())
			scheduledFuture=null;
	}

	private void addTask(AbstractS<?> t)
	{
		synchronized (this) {
			if (scheduledFuture!=null)
			{
				scheduledFuture.cancel(false);
				scheduledFuture=null;
			}
			tasks.add(t);
			scheduleNextTask();
		}
	}
	public void addTask(ScheduledTask scheduledTask) throws DatabaseException {
		tasks.add(new DS(scheduledTask, scheduledTask.getStartTimeUTCInMs()));
	}


	private long getNextTimeOfExecution(ScheduledPeriodicTask scheduledPeriodicTask) throws DatabaseException {
		return databaseWrapper.runSynchronizedTransaction(new SynchronizedTransaction<Long>() {
			@Override
			public Long run() throws Exception {
				ExecutedTasksTable.Record r=executedTasksTable.getRecord("strategyClassName", scheduledPeriodicTask.getStrategyClass(), "databasePackageName", currentDatabasePackageNameLoading.getName());
				if (r==null)
				{
					r=new ExecutedTasksTable.Record(currentDatabasePackageNameLoading, scheduledPeriodicTask.getStrategyClass());
					long n=scheduledPeriodicTask.getNextOccurrenceInTimeUTCAfter(r.getLastExecutionTimeUTC());
					if (n>scheduledPeriodicTask.getEndTimeUTCInMs())
						return Long.MIN_VALUE;
					executedTasksTable.addRecord(r);
					return n;
				}
				else {
					long n= scheduledPeriodicTask.getNextOccurrenceInTimeUTCAfter(r.getLastExecutionTimeUTC());
					if (n>scheduledPeriodicTask.getEndTimeUTCInMs()) {
						executedTasksTable.removeRecord(r);
						return Long.MIN_VALUE;
					}
					else {
						executedTasksTable.updateRecord(r, "toRemove", false);
						return n;
					}
				}
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_REPEATABLE_READ;
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
	@SuppressWarnings("unused")
	void prePackageLoading(Package databasePackageNameLoading) throws DatabaseException {
		if (currentDatabasePackageNameLoading!=null)
			throw new IllegalAccessError();
		this.currentDatabasePackageNameLoading=databasePackageNameLoading;
		executedTasksTable.updateRecords(new AlterRecordFilter<ExecutedTasksTable.Record>() {
			@Override
			public void nextRecord(ExecutedTasksTable.Record _record) throws DatabaseException {
				update("toRemove", true);
			}
		}, "databasePackageName=%databasePackageLoading", databasePackageNameLoading.getName());
	}
	@SuppressWarnings("unused")
	void endPackageLoading() throws DatabaseException {
		if (currentDatabasePackageNameLoading==null)
			throw new IllegalAccessError();
		executedTasksTable.removeRecords(new Filter<ExecutedTasksTable.Record>() {
			@Override
			public boolean nextRecord(ExecutedTasksTable.Record _record) {
				return true;
			}
		}, "databasePackageName=%databasePackageLoading and toRemove=true", currentDatabasePackageNameLoading.getName());
		this.currentDatabasePackageNameLoading=null;
	}
	@SuppressWarnings("unused")
	void loadTable(Table<?> table) throws DatabaseException {
		if (currentDatabasePackageNameLoading==null)
			throw new IllegalAccessError();
		for (Annotation a : table.getClass().getAnnotations())
		{
			if (a instanceof TablePeriodicTasks)
			{
				for (TablePeriodicTask tt : ((TablePeriodicTasks) a).value())
				{
					if (tt.endTimeUTCInMs()>System.currentTimeMillis()) {
						ScheduledPeriodicTask s = new ScheduledPeriodicTask(tt.strategy(), tt.periodInMs(), tt.dayOfWeek(), tt.hour(), tt.minute(), tt.endTimeUTCInMs());
						long n=getNextTimeOfExecution(s);
						if (n!=Long.MIN_VALUE)
							addTask(new TS(s, n, table));
					}
				}
				for (DatabasePeriodicTask tt : ((DatabasePeriodicTasks) a).value())
				{
					if (tt.endTimeUTCInMs()>System.currentTimeMillis()) {
						ScheduledPeriodicTask s = new ScheduledPeriodicTask(tt.strategy(), tt.periodInMs(), tt.dayOfWeek(), tt.hour(), tt.minute(), tt.endTimeUTCInMs());
						long n=getNextTimeOfExecution(s);
						if (n!=Long.MIN_VALUE)
							addTask(new DS(s, n));
					}
				}
			}
		}
	}
}
