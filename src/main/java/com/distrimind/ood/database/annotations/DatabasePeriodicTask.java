package com.distrimind.ood.database.annotations;
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

import com.distrimind.ood.database.tasks.IDatabaseTaskStrategy;
import com.distrimind.util.concurrent.ScheduledPoolExecutor;

import java.lang.annotation.*;
import java.time.ZoneOffset;

/**
 * This annotation permit to define a periodic task to execute with the database.
 * It is possible to specify a thread pool executor with the function {@link com.distrimind.ood.database.DatabaseFactory#setDefaultPoolExecutor(ScheduledPoolExecutor)}.
 * If this last function is not used, a thread pool executor will be created to manage database tasks.
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.2.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(DatabasePeriodicTasks.class)
public @interface DatabasePeriodicTask {

	/**
	 * Define the time UTC after what the periodic task is stopped
	 * @return the time UTC after what the periodic task is stopped
	 */
	long endTimeUTCInMs() default Long.MAX_VALUE;

	/**
	 * Define the task period. -1 means that this parameter will not be used.
	 * @return the task period
	 */
	long periodInMs() default -1;

	/**
	 * Define the minute when the periodic task can begin. -1 means that this parameter will not be used.
	 * @return the minute when the periodic task can begin
	 */
	byte minute() default -1;
	/**
	 * Define the hour when the periodic task can begin. -1 means that this parameter will not be used.
	 * @return the hour when the periodic task can begin
	 */
	byte hour() default -1;

	/**
	 * Define the day week when the periodic task can begin.
	 * Day begin with day 1 that correspond to monday.
	 * -1 means that this parameter will not be used.
	 * @return Day week that start at 1 with monday and ends at 7 with sunday
	 */
	byte dayOfWeek() default -1;

	/**
	 * Define the class to instantiate in order to execute the task that implements the interface {@link IDatabaseTaskStrategy}
	 * @return the class to instantiate in order to execute the task that implements the interface {@link IDatabaseTaskStrategy}
	 */
	Class<? extends IDatabaseTaskStrategy> strategy();

	/**
	 * Define zone offset.
	 * It possible to use zoneId instead of this parameter, but not both parameters.
	 * @return the zone offset.
	 * @see ZoneOffset#of(String)
	 */
	String zoneOffset() default "";

	/**
	 * Define zone identifier.
	 * It possible to use zoneOffset instead of this parameter, but not both parameters.
	 * @return the zone offset.
	 * @see ZoneOffset#of(String)
	 */
	String zoneId() default "";
}
