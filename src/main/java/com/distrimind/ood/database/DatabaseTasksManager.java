package com.distrimind.ood.database;
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

import com.distrimind.ood.database.annotations.TablePeriodicTask;
import com.distrimind.ood.database.annotations.TablePeriodicTasks;
import com.distrimind.ood.database.annotations.TableTask;
import com.distrimind.ood.database.annotations.TableTasks;
import com.distrimind.ood.database.exceptions.DatabaseException;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.util.TreeSet;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.2.0
 */
public class DatabaseTasksManager {
	private final TreeSet<AbstractS<?>> tasks=new TreeSet<>();
	private abstract static class AbstractS<T extends ITaskStrategy>
	{
		protected final AbstractScheduledTask scheduledTask;
		protected final T strategy;

		protected AbstractS(AbstractScheduledTask scheduledTask) throws DatabaseException {
			this.scheduledTask = scheduledTask;
			try {
				//noinspection unchecked
				strategy=(T)scheduledTask.getStrategyClass().getConstructor().newInstance();
			} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				throw new DatabaseException("Impossible to instantiate "+scheduledTask.getStrategyClass(), e);
			}
		}

		protected abstract void run();
	}
	private static class DS extends AbstractS<IDatabaseTaskStrategy>
	{
		private final DatabaseWrapper databaseWrapper;
		protected DS(AbstractScheduledTask scheduledTask, DatabaseWrapper databaseWrapper) throws DatabaseException {
			super(scheduledTask);
			if (databaseWrapper ==null)
				throw new NullPointerException();
			this.databaseWrapper = databaseWrapper;
		}

		@Override
		protected void run() {
			strategy.launchTask(databaseWrapper);
		}
	}
	private static class TS extends AbstractS<ITableTaskStrategy<?>>
	{
		private final Table<?> table;
		protected TS(AbstractScheduledTask scheduledTask, Table<?> table) throws DatabaseException {
			super(scheduledTask);
			if (table==null)
				throw new NullPointerException();
			this.table=table;
		}

		@Override
		protected void run() {
			strategy.launchTaskWithUntypedTable(table);
		}
	}
	void addTask(AbstractS<?> t)
	{
		tasks.add(t);
	}

	void loadTable(Table<?> table) throws DatabaseException {
		for (Annotation a : table.getClass().getAnnotations())
		{
			if (a instanceof TableTasks)
			{
				for (TableTask tt : ((TableTasks) a).value())
				{
					addTask(new TS(new ScheduledTask(tt.strategy(), tt.startTimeUTCInMs()), table));
				}
			}
			else if (a instanceof TablePeriodicTasks)
			{
				for (TablePeriodicTask tt : ((TablePeriodicTasks) a).value())
				{
					addTask(new TS(new ScheduledPeriodicTask(tt.strategy(), tt.startTimeUTCInMs(), tt.periodInMs(), tt.endTimeUTCInMs()), table));
				}
			}
		}
	}
}
