package com.distrimind.ood.database.tasks;
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

import com.distrimind.ood.database.*;
import com.distrimind.ood.database.annotations.DatabasePeriodicTask;
import com.distrimind.ood.database.annotations.DatabasePeriodicTasks;
import com.distrimind.ood.database.annotations.TablePeriodicTask;
import com.distrimind.ood.database.annotations.TablePeriodicTasks;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.concurrent.ScheduledPoolExecutor;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.TreeSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class permit to add and schedule database tasks.
 * Tasks can be periodic, or executed with only one time.
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.2.0
 * @see ScheduledPeriodicTask
 * @see DatabaseWrapper#getDatabaseTasksManager()
 *
 */
public class DatabaseTasksManager {
	private final TreeSet<AbstractS<?>> tasks=new TreeSet<>();
	private final DatabaseWrapper databaseWrapper;
	private final ScheduledPoolExecutor threadPoolExecutor;
	private ScheduledFuture<?> scheduledFuture=null;
	private final ExecutedTasksTable executedTasksTable;
	private Package currentDatabasePackageLoading =null;
	private boolean closed=false;

	DatabaseTasksManager(DatabaseWrapper databaseWrapper) throws DatabaseException {
		ScheduledPoolExecutor threadPoolExecutor=databaseWrapper.getDefaultPoolExecutor();
		if (threadPoolExecutor==null)
			threadPoolExecutor=new ScheduledPoolExecutor(1, 1, 10, TimeUnit.MINUTES);
		this.threadPoolExecutor=threadPoolExecutor;
		this.databaseWrapper = databaseWrapper;
		this.executedTasksTable=databaseWrapper.getTableInstance(ExecutedTasksTable.class);
	}

	private static <T extends ITaskStrategy> T getStrategy(AbstractScheduledTask scheduledTask) throws DatabaseException {
		try {
			//noinspection unchecked
			return (T)scheduledTask.getStrategyClass().getConstructor().newInstance();
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			throw new DatabaseException("Impossible to instantiate "+scheduledTask.getStrategyClass(), e);
		}
	}

	private abstract class AbstractS<T extends ITaskStrategy> implements Comparable<AbstractS<T>>
	{
		protected final AbstractScheduledTask scheduledTask;
		protected final T strategy;
		protected final long timeUTCOfExecution;

		protected boolean exception=false;
		protected final Table<?> declaredOnTable;
		protected final int annotationPosition;


		protected AbstractS(AbstractScheduledTask scheduledTask, long timeUTCOfExecution, Table<?> declaredOnTable, int annotationPosition) throws DatabaseException {
			this(scheduledTask, timeUTCOfExecution, declaredOnTable, annotationPosition, getStrategy(scheduledTask));
		}
		protected AbstractS(AbstractScheduledTask scheduledTask, long timeUTCOfExecution, Table<?> declaredOnTable, int annotationPosition, T strategy) {
			if (scheduledTask==null)
				throw new NullPointerException();
			if (strategy==null)
				throw new NullPointerException();
			if (declaredOnTable!=null && annotationPosition<0)
				throw new IllegalArgumentException();
			else if (declaredOnTable==null && annotationPosition!=-1)
				throw new IllegalArgumentException();
			this.scheduledTask = scheduledTask;
			this.strategy=strategy;
			this.timeUTCOfExecution=timeUTCOfExecution;
			this.declaredOnTable=declaredOnTable;
			this.annotationPosition=annotationPosition;
		}
		protected boolean isAnnotation()
		{
			return annotationPosition>=0;
		}
		protected long getNextTimeUTCOfExecution() throws DatabaseException {
			if (this.scheduledTask instanceof ScheduledPeriodicTask)
			{
				final ScheduledPeriodicTask scheduledPeriodicTask =(ScheduledPeriodicTask)scheduledTask;
				if (isAnnotation()) {
					return databaseWrapper.runSynchronizedTransaction(new SynchronizedTransaction<Long>() {
						@Override
						public Long run() throws Exception {
							ExecutedTasksTable.Record r = executedTasksTable.getRecord("tableClass", declaredOnTable.getClass(),"annotationPosition",annotationPosition, "strategyClass", scheduledPeriodicTask.getStrategyClass());
							if (r == null) {
								return Long.MIN_VALUE;
							} else {
								r.setLastExecutionTimeUTC(System.currentTimeMillis());
								executedTasksTable.updateRecord(r, "lastExecutionTimeUTC", r.getLastExecutionTimeUTC());
								long n = scheduledPeriodicTask.getNextOccurrenceInTimeUTCAfter(r.getLastExecutionTimeUTC());
								if (n > scheduledPeriodicTask.getEndTimeUTCInMs()) {
									executedTasksTable.removeRecord(r);
									return Long.MIN_VALUE;
								} else
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
				{
					long n=scheduledPeriodicTask.getNextOccurrenceInTimeUTCAfter(System.currentTimeMillis());
					if (n>scheduledPeriodicTask.getEndTimeUTCInMs()) {
						return Long.MIN_VALUE;
					}
					else
						return n;
				}

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

		protected DS(AbstractScheduledTask scheduledTask, long timeUTCOfExecution, Table<?> declaredOnTable, int annotationPosition) throws DatabaseException {
			super(scheduledTask, timeUTCOfExecution, declaredOnTable, annotationPosition);
		}

		protected DS(AbstractScheduledTask scheduledTask, long timeUTCOfExecution, Table<?> declaredOnTable, int annotationPosition, IDatabaseTaskStrategy strategy)  {
			super(scheduledTask, timeUTCOfExecution, declaredOnTable, annotationPosition, strategy);
		}

		@Override
		protected void run() {
			try {
				strategy.launchTask(databaseWrapper);
			} catch (DatabaseException e) {
				exception=true;
				e.printStackTrace();
			}
		}

		@Override
		DS getNextCycle() throws DatabaseException {
			if (!exception && this.scheduledTask instanceof ScheduledPeriodicTask)
			{
				long n=getNextTimeUTCOfExecution();
				if (n!=Long.MIN_VALUE)
					return new DS(scheduledTask, n, declaredOnTable, annotationPosition, strategy);
			}
			return null;
		}
	}
	private class TS extends AbstractS<ITableTaskStrategy<?>>
	{
		protected TS(AbstractScheduledTask scheduledTask, long timeUTCOfExecution, Table<?> declaredOnTable, int annotationPosition) throws DatabaseException {
			super(scheduledTask, timeUTCOfExecution, declaredOnTable, annotationPosition);
			if (declaredOnTable==null)
				throw new NullPointerException();
			if (annotationPosition<0)
				throw new IllegalArgumentException();
		}

		public TS(AbstractScheduledTask scheduledTask, long timeUTCOfExecution, Table<?> declaredOnTable, int annotationPosition, ITableTaskStrategy<?> strategy) {
			super(scheduledTask, timeUTCOfExecution, declaredOnTable, annotationPosition, strategy);
			if (declaredOnTable==null)
				throw new NullPointerException();
			if (annotationPosition<0)
				throw new IllegalArgumentException();
		}

		@Override
		protected void run() {
			try {
				strategy.launchTaskWithUntypedTable(declaredOnTable);
			} catch (DatabaseException e) {
				exception=true;
				e.printStackTrace();
			}

		}
		@Override
		TS getNextCycle() throws DatabaseException {
			if (!exception && this.scheduledTask instanceof ScheduledPeriodicTask)
			{
				long n=getNextTimeUTCOfExecution();
				if (n!=Long.MIN_VALUE)
					return new TS(scheduledTask, n, declaredOnTable, annotationPosition, strategy);
			}
			return null;
		}
	}
	private ScheduledPoolExecutor getThreadPoolExecutor()
	{
		return threadPoolExecutor;
	}
	@SuppressWarnings("unused")
	void close()
	{
		synchronized (this)
		{
			closed=true;
			if (scheduledFuture!=null)
				scheduledFuture.cancel(true);
			scheduledFuture=null;
			if (threadPoolExecutor!=databaseWrapper.getDefaultPoolExecutor())
				threadPoolExecutor.shutdownNow();
		}
	}
	private void scheduleNextTask()
	{
		if (tasks.isEmpty())
			return;

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
				if (closed)
					return;
			}
			task.run();
			try {
				task=task.getNextCycle();
				synchronized (DatabaseTasksManager.this)
				{
					if (task!=null)
						tasks.add(task);
					if (closed)
						return;
					scheduleNextTask();
				}
			} catch (DatabaseException e) {
				e.printStackTrace();
			}

		}, tasks.first().timeUTCOfExecution-System.currentTimeMillis(), TimeUnit.MILLISECONDS);
		if (scheduledFuture.isDone())
			scheduledFuture=null;
	}

	private void addTask(AbstractS<?> t)
	{
		synchronized (this) {
			if (closed)
				return;
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
		addTask(new DS(scheduledTask, scheduledTask.getStartTimeUTCInMs(), null, -1));
	}
	public void addTask(ScheduledPeriodicTask scheduledTask) throws DatabaseException {
		long n=scheduledTask.getNextOccurrenceInTimeUTCAfter(System.currentTimeMillis(), false);
		if (n!=Long.MIN_VALUE && n<scheduledTask.getEndTimeUTCInMs())
			addTask(new DS(scheduledTask, n, null, -1));
	}

	private long getNextTimeOfExecution(Table<?> table, int annotationPosition, ScheduledPeriodicTask scheduledPeriodicTask) throws DatabaseException {
		return databaseWrapper.runSynchronizedTransaction(new SynchronizedTransaction<Long>() {
			@Override
			public Long run() throws Exception {
				ExecutedTasksTable.Record r=executedTasksTable.getRecord("strategyClass", scheduledPeriodicTask.getStrategyClass(), "tableClass", table.getClass(), "annotationPosition", annotationPosition);
				if (r==null)
				{
					r=new ExecutedTasksTable.Record(table, annotationPosition, scheduledPeriodicTask.getStrategyClass());
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
		if (currentDatabasePackageLoading !=null)
			throw new IllegalAccessError();
		this.currentDatabasePackageLoading =databasePackageNameLoading;
		executedTasksTable.updateRecords(new AlterRecordFilter<ExecutedTasksTable.Record>() {
			@Override
			public void nextRecord(ExecutedTasksTable.Record _record) throws DatabaseException {
				update("toRemove", true);
			}
		}, "databasePackageName=%databasePackageLoading", "databasePackageLoading", databasePackageNameLoading.getName());
	}
	@SuppressWarnings("unused")
	void endPackageLoading() throws DatabaseException {
		if (currentDatabasePackageLoading ==null)
			throw new IllegalAccessError();
		executedTasksTable.removeRecords(new Filter<ExecutedTasksTable.Record>() {
			@Override
			public boolean nextRecord(ExecutedTasksTable.Record _record) {
				return true;
			}
		}, "databasePackageName=%databasePackageLoading and toRemove=%b", "databasePackageLoading", currentDatabasePackageLoading.getName(), "b", true);
		this.currentDatabasePackageLoading =null;
	}
	private ZoneOffset getZoneOffset(Table<?> table, String offsetId, String zoneId) throws DatabaseException {
		if (offsetId.equals("") && zoneId.equals(""))
			return ZoneOffset.systemDefault().getRules().getStandardOffset(Instant.now());
		else if (!offsetId.equals("") && !zoneId.equals(""))
			throw new DatabaseException("Only zoneOffset or zoneId must be defined into annotations of table "+table.getClass().getName());
		else if (offsetId.equals(""))
			return ZoneId.of(zoneId).getRules().getStandardOffset(Instant.now());
		else
			return ZoneOffset.of(offsetId);
	}
	private void loadAnnotation(Table<?> table, int annotationPosition, TablePeriodicTask tt) throws DatabaseException {
		if (tt.endTimeUTCInMs() > System.currentTimeMillis()) {
			ScheduledPeriodicTask s = new ScheduledPeriodicTask(tt.strategy(), tt.periodInMs(), tt.dayOfWeek(), tt.hour(), tt.minute(), tt.endTimeUTCInMs(), getZoneOffset(table, tt.zoneOffset(), tt.zoneId()));
			long n = getNextTimeOfExecution(table, annotationPosition, s);
			if (n != Long.MIN_VALUE)
				addTask(new TS(s, n, table, annotationPosition));
		}
	}
	private void loadAnnotation(Table<?> table, int annotationPosition, DatabasePeriodicTask tt) throws DatabaseException {
		if (tt.endTimeUTCInMs()>System.currentTimeMillis()) {
			ScheduledPeriodicTask s = new ScheduledPeriodicTask(tt.strategy(), tt.periodInMs(), tt.dayOfWeek(), tt.hour(), tt.minute(), tt.endTimeUTCInMs(), getZoneOffset(table, tt.zoneOffset(), tt.zoneId()));
			long n=getNextTimeOfExecution(table, annotationPosition, s);
			if (n!=Long.MIN_VALUE)
				addTask(new DS(s, n, table, annotationPosition));
		}
	}
	@SuppressWarnings("unused")
	void loadTable(Table<?> table) throws DatabaseException {
		if (currentDatabasePackageLoading ==null)
			throw new IllegalAccessError();
		int i=0;
		for (Annotation a : table.getClass().getDeclaredAnnotations())
		{
			if (a instanceof TablePeriodicTasks) {
				for (TablePeriodicTask tt : ((TablePeriodicTasks) a).value()) {
					loadAnnotation(table, i, tt);
				}
			}
			else if (a instanceof DatabasePeriodicTasks)
			{
				for (DatabasePeriodicTask tt : ((DatabasePeriodicTasks) a).value())
				{
					loadAnnotation(table, i, tt);
				}
			}
			else if (a instanceof TablePeriodicTask)
				loadAnnotation(table, i, (TablePeriodicTask)a);
			else if (a instanceof DatabasePeriodicTask)
				loadAnnotation(table, i, (DatabasePeriodicTask)a);
			i++;
		}

	}
}
