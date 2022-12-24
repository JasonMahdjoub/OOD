package com.distrimind.ood.database.tasks.database;
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

import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.tasks.IDatabaseTaskStrategy;
import com.distrimind.ood.database.tasks.ScheduledTasksTests;
import org.testng.Assert;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.2.0
 */
public class NonAnnotationPeriodicDatabaseTaskStrategy implements IDatabaseTaskStrategy {
	public static void reset()
	{
		oneInstance=false;
	}
	private static boolean oneInstance=false;
	public final AtomicInteger numberOfTaskCall=new AtomicInteger(0);
	public static final long periodInMs=750;
	private final long startUTC=System.currentTimeMillis();

	public NonAnnotationPeriodicDatabaseTaskStrategy() {
		assert !oneInstance;
		oneInstance=true;
	}

	static void checkTime(AtomicInteger numberOfTaskCall, long startUTC, long periodInMs)
	{
		long d=System.currentTimeMillis()-startUTC;
		long p=(numberOfTaskCall.get()+1)*periodInMs;
		Assert.assertTrue(p-25<=d, "p-20="+(p-25)+", d="+d+", periodInMs="+periodInMs+", cycle="+numberOfTaskCall.get());
		Assert.assertTrue(p+periodInMs/2>d, "p+periodInMs/2="+(p+periodInMs/2)+", d="+d+", cycle="+numberOfTaskCall.get());
	}
	@Override
	public void launchTask(DatabaseWrapper wrapper) throws DatabaseException {
		try {
			checkTime(numberOfTaskCall, startUTC, periodInMs);

			Table1 t1 = wrapper.getTableInstance(Table1.class);
			Assert.assertNotNull(t1);
			Table2 t2 = wrapper.getTableInstance(Table2.class);
			Assert.assertNotNull(t2);
			t1.removeRecordsWithAllFields("stringField", NonAnnotationPeriodicDatabaseTaskStrategy.class.getSimpleName() + ";" + numberOfTaskCall.incrementAndGet());
			t2.removeRecordsWithAllFields("stringField", NonAnnotationPeriodicDatabaseTaskStrategy.class.getSimpleName() + ";" + numberOfTaskCall.get());
		}
		catch (AssertionError e)
		{
			ScheduledTasksTests.testFailed.set(true);
			throw e;
		}

	}
}
