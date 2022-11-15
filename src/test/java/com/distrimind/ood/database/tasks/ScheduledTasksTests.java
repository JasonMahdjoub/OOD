
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

package com.distrimind.ood.database.tasks;


import com.distrimind.ood.database.DatabaseConfiguration;
import com.distrimind.ood.database.DatabaseSchema;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.SecureRandomType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.DayOfWeek;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.fail;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.2.0
 */

public abstract class ScheduledTasksTests {
	protected Table1 table1;
	protected Table2 table2;
	static AtomicBoolean testFailed=new AtomicBoolean(false);

	static Set<Class<?>> listClasses=new HashSet<>(Arrays.asList(Table1.class, Table2.class));
	static DatabaseConfiguration dbConfig1 = new DatabaseConfiguration(new DatabaseSchema(Table1.class.getPackage(), listClasses));
	private DatabaseWrapper sql_db;

	public static final AbstractSecureRandom secureRandom;
	static {
		AbstractSecureRandom rand = null;
		try {
			rand = SecureRandomType.DEFAULT.getSingleton(null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		secureRandom = rand;
	}

	@AfterClass
	public void unloadDatabase() throws DatabaseException {
		System.out.println("Unload database !");
		if (sql_db != null) {
			sql_db.close();
			deleteDatabaseFiles();
			sql_db = null;
		}

	}

	public abstract DatabaseWrapper getDatabaseWrapperInstance() throws IllegalArgumentException, DatabaseException;

	public abstract void deleteDatabaseFiles() throws IllegalArgumentException, DatabaseException;



	@DataProvider
	public Object[][] getDataProviderForPeriodicTasks()
	{
		ScheduledPeriodicTask.MIN_PERIOD_IN_MS=100;
		try {
			return new Object[][]{
					{new ScheduledPeriodicTask(RepetitiveDatabaseTaskStrategy.class, 750), 240L, 240L + 750L},
				{new ScheduledPeriodicTask(RepetitiveDatabaseTaskStrategy.class, 1250),4000L, 4000L+1250L},
				{new ScheduledPeriodicTask(RepetitiveDatabaseTaskStrategy.class, 750000, (byte)5, (byte)-1, null),8750000L, 20300000L},
				{new ScheduledPeriodicTask(RepetitiveDatabaseTaskStrategy.class, 750000, (byte)5, (byte)37, null),8750000L, 20240000L},
				{new ScheduledPeriodicTask(RepetitiveDatabaseTaskStrategy.class, 750000, (byte)5, (byte)37, DayOfWeek.FRIDAY),8750000L, 106640000L},
				{new ScheduledPeriodicTask(RepetitiveDatabaseTaskStrategy.class, 750000, (byte)-1, (byte)40, DayOfWeek.FRIDAY),8750000L, 96020000L},
				{new ScheduledPeriodicTask(RepetitiveDatabaseTaskStrategy.class, 750000, (byte)-1, (byte)-1, DayOfWeek.FRIDAY),8750000L, 95900000L},
				{new ScheduledPeriodicTask(RepetitiveDatabaseTaskStrategy.class, 750000, (byte)-1, (byte)-1, DayOfWeek.MONDAY),8750000L, 355100000L},
			};
		}
		catch (Throwable e)
		{
			e.printStackTrace();
			throw e;
		}
	}

	@Test(dataProvider = "getDataProviderForPeriodicTasks")
	public void testScheduledPeriodicTask(ScheduledPeriodicTask task, long timeUTC, long expectedNextTimeUTC)
	{
		Assert.assertEquals(task.getNextOccurrenceInTimeUTCAfter(timeUTC, false), expectedNextTimeUTC,"task="+task+", task.getNextOccurrenceInTimeUTCAfter(timeUTC)="+task.getNextOccurrenceInTimeUTCAfter(timeUTC, false)+", expectedNextTimeUTC="+expectedNextTimeUTC);
		Assert.assertTrue(task.getNextOccurrenceInTimeUTCAfter(timeUTC)>System.currentTimeMillis());
	}

	@Test(dependsOnMethods = { "testScheduledPeriodicTask" })
	public void checkUnloadedDatabase() throws IllegalArgumentException, DatabaseException {
		deleteDatabaseFiles();
		sql_db = getDatabaseWrapperInstance();
		try {
			sql_db.getDatabaseConfigurationsBuilder()
					.addConfiguration(dbConfig1, false, false)
					.commit();
			//sql_db.loadDatabase(dbConfig1, false);
			fail();
		} catch (DatabaseException ignored) {
		}
	}

	@Test(dependsOnMethods = { "checkUnloadedDatabase" })
	public void testTasksScheduling() throws IllegalArgumentException, DatabaseException {

		sql_db.getDatabaseConfigurationsBuilder()
				.addConfiguration(dbConfig1, false, true)
				.commit();
		table2 = sql_db.getTableInstance(Table2.class);
		table1 = sql_db.getTableInstance(Table1.class);

		table1.addRecord(new Table1.Record(RepetitiveDatabaseTaskStrategy.class.getSimpleName()+";1"));
		table1.addRecord(new Table1.Record(RepetitiveDatabaseTaskStrategy.class.getSimpleName()+";2"));
		table1.addRecord(new Table1.Record(RepetitiveDatabaseTaskStrategy.class.getSimpleName()+";3"));

		table2.addRecord(new Table1.Record(RepetitiveDatabaseTaskStrategy.class.getSimpleName()+";1"));
		table2.addRecord(new Table1.Record(RepetitiveDatabaseTaskStrategy.class.getSimpleName()+";2"));
		table2.addRecord(new Table1.Record(RepetitiveDatabaseTaskStrategy.class.getSimpleName()+";3"));

		table1.addRecord(new Table1.Record(RepetitiveTableTaskStrategyOnTable1.class.getSimpleName()+";1"));
		table1.addRecord(new Table1.Record(RepetitiveTableTaskStrategyOnTable1.class.getSimpleName()+";2"));
		table1.addRecord(new Table1.Record(RepetitiveTableTaskStrategyOnTable1.class.getSimpleName()+";3"));

		table2.addRecord(new Table1.Record(RepetitiveTableTaskStrategyOnTable2.class.getSimpleName()+";1"));
		table2.addRecord(new Table1.Record(RepetitiveTableTaskStrategyOnTable2.class.getSimpleName()+";2"));
		table2.addRecord(new Table1.Record(RepetitiveTableTaskStrategyOnTable2.class.getSimpleName()+";3"));

		table1.addRecord(new Table1.Record(NonAnnotationDatabaseTaskStrategy.class.getSimpleName()+";1"));
		table2.addRecord(new Table1.Record(NonAnnotationDatabaseTaskStrategy.class.getSimpleName()+";1"));

		table1.addRecord(new Table1.Record(NonAnnotationPeriodicDatabaseTaskStrategy.class.getSimpleName()+";1"));
		table1.addRecord(new Table1.Record(NonAnnotationPeriodicDatabaseTaskStrategy.class.getSimpleName()+";2"));
		table1.addRecord(new Table1.Record(NonAnnotationPeriodicDatabaseTaskStrategy.class.getSimpleName()+";3"));

		table2.addRecord(new Table1.Record(NonAnnotationPeriodicDatabaseTaskStrategy.class.getSimpleName()+";1"));
		table2.addRecord(new Table1.Record(NonAnnotationPeriodicDatabaseTaskStrategy.class.getSimpleName()+";2"));
		table2.addRecord(new Table1.Record(NonAnnotationPeriodicDatabaseTaskStrategy.class.getSimpleName()+";3"));

		table1.addRecord(new Table1.Record(NonAnnotationPeriodicDatabaseTaskStrategyWithEndTimeLimit.class.getSimpleName()+";1"));
		table1.addRecord(new Table1.Record(NonAnnotationPeriodicDatabaseTaskStrategyWithEndTimeLimit.class.getSimpleName()+";2"));

		table2.addRecord(new Table1.Record(NonAnnotationPeriodicDatabaseTaskStrategyWithEndTimeLimit.class.getSimpleName()+";1"));
		table2.addRecord(new Table1.Record(NonAnnotationPeriodicDatabaseTaskStrategyWithEndTimeLimit.class.getSimpleName()+";2"));

		sql_db.getDatabaseTasksManager().addTask(new ScheduledTask(NonAnnotationDatabaseTaskStrategy.class, System.currentTimeMillis()-NonAnnotationDatabaseTaskStrategy.delay));
		sql_db.getDatabaseTasksManager().addTask(new ScheduledPeriodicTask(NonAnnotationPeriodicDatabaseTaskStrategy.class, NonAnnotationPeriodicDatabaseTaskStrategy.periodInMs));
		sql_db.getDatabaseTasksManager().addTask(new ScheduledPeriodicTask(NonAnnotationPeriodicDatabaseTaskStrategyWithEndTimeLimit.class, NonAnnotationPeriodicDatabaseTaskStrategy.periodInMs,
				(byte)-1, (byte)-1,null,
				System.currentTimeMillis()+NonAnnotationPeriodicDatabaseTaskStrategyWithEndTimeLimit.maxNumberOfCalls*NonAnnotationPeriodicDatabaseTaskStrategyWithEndTimeLimit.periodInMs+NonAnnotationPeriodicDatabaseTaskStrategyWithEndTimeLimit.periodInMs/2));
		sql_db.getDatabaseTasksManager().addTask(new ScheduledPeriodicTask(NotLaunchedTaskStrategy.class, 1000,
				(byte)-1, (byte)-1,null,
				System.currentTimeMillis()-1
				));
		Assert.assertEquals(table1.getRecordsNumber(), 12);
		Assert.assertEquals(table2.getRecordsNumber(), 12);
		Assert.assertFalse(testFailed.get());
		try {
			Thread.sleep(1500*5);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		Assert.assertFalse(testFailed.get());
		Assert.assertEquals(table1.getRecordsNumber(), 0);
		Assert.assertEquals(table2.getRecordsNumber(), 0);
	}



}
