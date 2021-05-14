
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
package com.distrimind.ood;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;

import com.distrimind.util.version.Description;
import com.distrimind.util.version.Person;
import com.distrimind.util.version.PersonDeveloper;
import com.distrimind.util.version.Version;

/**
 * 
 * @author Jason Mahdjoub
 * @version 2.0.0
 * @since OOD 1.0
 */
public class OOD {
	public static final Version VERSION;

	static {
		VERSION = new Version("Object Oriented Database", "OOD", "2013-04-01");
		try {
			InputStream is = OOD.class.getResourceAsStream("build.txt");
			if (is != null)
				VERSION.loadBuildNumber(is);
			VERSION.addCreator(new Person("mahdjoub", "jason"))
					.addDeveloper(new PersonDeveloper("mahdjoub", "jason", "2013-04-01"))
					.addDescription(
							new Description((short)3, (short)0, (short)0, Version.Type.STABLE, (short)0, "2020-10-28")
									.addItem("Update Utils to 4.15.2")
									.addItem("Update HSQLDB to 2.5.1")
									.addItem("Implementation of data synchronization with central database backup")
									.addItem("Make OOD compatible with Android")
									.addItem("Implementation of driver for MySQL")
									.addItem("Implementation of driver for Android H2 Database")
									.addItem("Implementation of driver for PostgreSQL")
									.addItem("Compatible with Java 8 and newer")
									.addItem("Calendar is better serialized into database (better performances, and lower space)")
									.addItem("Calendar is now comparable")
									.addItem("Implementation of DatabaseConfigurationBuilder that centralize database loading, and database synchronization with decentralized peers and with central backup database")
									.addItem("Add function Table.removeAllRecordsWithCascade()")
									.addItem("Fix issue when changing database's version and refresh tables state when BackupRestoreManager was loaded")
									.addItem("Add function Table.hasRecords(String, Map)")
									.addItem("Add function Table.hasRecords(Filter, String, Map)")
									.addItem("Add function DatabaseLifeCycles.peersAdded(Set<DecentralizedValue>) that is triggered when a set of peers was added")
									.addItem("Fix join issue when table A has two foreign keys toward the same table B")
					)
					.addDescription(
							new Description((short)2, (short)4, (short)0, Version.Type.STABLE, (short)1, "2020-02-11")
									.addItem("Update Utils to 4.9.0")
									.addItem("Update database factories")
									.addItem("Database wrappers are now instantiable only through factories")
					);

			Calendar c = Calendar.getInstance();
			c.set(2020, Calendar.JANUARY, 24);
			Description d = new Description((short)2, (short)3, (short)21, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem("Update Utils to 4.8.6");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2020, Calendar.JANUARY, 7);
			d = new Description((short)2, (short)3, (short)14, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem("Rename RandomPrivateKey.byteNumber but do not change its behavior");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.DECEMBER, 16);
			d = new Description((short)2, (short)3, (short)13, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem("Update Utils to 4.7.1 STABLE");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.NOVEMBER, 22);
			d = new Description((short)2, (short)3, (short)12, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem("Update Utils to 4.7.0 STABLE");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.NOVEMBER, 15);
			d = new Description((short)2, (short)3, (short)10, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem("Update Utils to 4.6.5 STABLE");
			d.addItem("Compile with openjdk 13 (compatibility set to Java 7");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.NOVEMBER, 13);
			d = new Description((short)2, (short)3, (short)8, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem("Update Utils to 4.6.3 STABLE");
			d.addItem("Fix backup cache upgrade issue when canceling transaction");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.OCTOBER, 31);
			d = new Description((short)2, (short)3, (short)7, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem("Update Utils to 4.6.2 STABLE");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.OCTOBER, 20);
			d = new Description((short)2, (short)3, (short)6, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem("Update dependencies");
			VERSION.addDescription(d);



			c = Calendar.getInstance();
			c.set(2019, Calendar.OCTOBER, 17);
			d = new Description((short)2, (short)3, (short)4, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem("Update Utils to 4.6.0 STABLE");
			d.addItem("Fix NullPointerException into DatabaseWrapper$DatabaseSynchronizer.getLocalHostID()");
			d.addItem("Fix HookAddRequest externalisation");
			d.addItem("Fix issue when loading the same database two times");
			d.addItem("Better reset synchronization");
			d.addItem("Better disconnect all hosts");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.SEPTEMBER, 2);
			d = new Description((short)2, (short)2, (short)1, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem("Add function DatabaseWrapper.Synchronizer.getDistantHostsIds");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.JULY, 16);
			d = new Description((short)2, (short)2, (short)0, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem("Update Utils to 4.4.3 STABLE");
			d.addItem("Add backup/restore manager, with historical management");
			d.addItem("Better manage database versions");
			d.addItem("Optimizations of several queries");
			d.addItem("Add function Table.removeAllRecordsWithCascade()");
			d.addItem("Use long values for queries limited by a number of rows");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.MAY, 6);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)107, c.getTime());
			d.addItem("Update Utils to 3.26.0 STABLE");
			d.addItem("Add Field.includeKeyExpiration()");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.APRIL, 23);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)106, c.getTime());
			d.addItem("Update Utils to 3.25.6 STABLE");
			d.addItem("Fix problem with getRecordsNumber function using where condition");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.MARCH, 21);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)104, c.getTime());
			d.addItem("Update Utils to 3.25.5 STABLE");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.MARCH, 13);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)103, c.getTime());
			d.addItem("Update Utils to 3.25.4 STABLE");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.MARCH, 1);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)100, c.getTime());
			d.addItem("Add function DatabaseCollisionsNotifier.areDuplicatedEventsNotConsideredAsCollisions()");
			d.addItem("Add function DatabaseCollisionsNotifier.startNewSynchronizationTransaction()");
			d.addItem("Add function DatabaseCollisionsNotifier.endSynchronizationTransaction()");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.FEBRUARY, 8);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)99, c.getTime());
			d.addItem("Better support of sub fields into queries");
			VERSION.addDescription(d);


			c = Calendar.getInstance();
			c.set(2019, Calendar.FEBRUARY, 6);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)98, c.getTime());
			d.addItem("Update Utils to 3.25.1");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.FEBRUARY, 5);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)97, c.getTime());
			d.addItem("Security fix : disable cache for tables that use secret ou private keys");
			d.addItem("Security improvement : add Field.disableCache property");
			d.addItem("Add function Table.isCached()");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.JANUARY, 25);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)96, c.getTime());
			d.addItem("Add function Table.removeRecord(Map keys)");
			d.addItem("Add function Table.removeRecord(Object...keys)");
			d.addItem("Add function Table.removeRecordWithCascade(Map keys)");
			d.addItem("Add function Table.removeRecordWithCascade(Object...keys)");
			d.addItem("Do not generate conflicts if events are the same");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.JANUARY, 18);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)95, c.getTime());
			d.addItem("Add H2 database driver");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.JANUARY, 15);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)94, c.getTime());
			d.addItem("Use shortest table's name");
			d.addItem("Add possibility to personalize SQL table name (see annotation TableName)");
			d.addItem("Add possibility to personalize SQL field name (see annotation Field)");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2018, Calendar.DECEMBER, 17);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)93, c.getTime());
			d.addItem("Updating utils to 3.24.0");
			VERSION.addDescription(d);


			c = Calendar.getInstance();
			c.set(2018, Calendar.NOVEMBER, 13);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)90, c.getTime());
			d.addItem("Updating utils to 3.22.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2018, Calendar.NOVEMBER, 7);
			 d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)89, c.getTime());
			d.addItem("Updating utils to 3.21.1");
			d.addItem("Manage Keys used for encryption as decentralized keys");
			d.addItem("Add DatabaseAnomaliesNotifier interface");
			d.addItem("Add DatabaseCollisionsNotifier interface");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2018, Calendar.JULY, 27);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)85, c.getTime());
			d.addItem("Updating utils to 3.18.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2018, Calendar.JULY, 17);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)84, c.getTime());
			d.addItem("Updating utils to 3.17.0");
            d.addItem("Do not loop infinitely if disconnection exception is generated infinitely");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2018, Calendar.JULY, 12);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)83, c.getTime());
			d.addItem("Updating utils to 3.16.1");
            d.addItem("Clean code");
            d.addItem("Add auto disconnection option with database wrappers");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2018, Calendar.MAY, 16);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)82, c.getTime());
			d.addItem("Updating utils to 3.15.0");
			VERSION.addDescription(d);
			
			c = Calendar.getInstance();
			c.set(2018, Calendar.MARCH, 30);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)71, c.getTime());
			d.addItem("Optimization of CachedInputStream");
			d.addItem("Optimization of CachedOutputStream");
			d.addItem("Updating utils to 3.13.1");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2018, Calendar.FEBRUARY, 10);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)66, c.getTime());
			d.addItem("Updating utils to 3.10.5");
			d.addItem("Correcting a bug with the nativeBackup of enum variables");
			VERSION.addDescription(d);
			
			c = Calendar.getInstance();
			c.set(2018, Calendar.JANUARY, 31);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)59, c.getTime());
			d.addItem("Updating utils to 3.9.0");
			VERSION.addDescription(d);
			
			c = Calendar.getInstance();
			c.set(2017, Calendar.DECEMBER, 13);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)57, c.getTime());
			d.addItem("Updating utils to 3.7.1");
			d.addItem("Managing disconnection database exception/reconnection");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.OCTOBER, 16);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)50, c.getTime());
			d.addItem("Adding secure random functions into database wrapper");
			d.addItem("public/private/secret keys can now be primary keys");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.OCTOBER, 16);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)50, c.getTime());
			d.addItem("Manage File fields");
			d.addItem("Add ExcludeFromDecentralization annotation !");
			d.addItem("Updating Utils to 3.3.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.OCTOBER, 13);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)49, c.getTime());
			d.addItem("Update Utils to 3.2.4");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.OCTOBER, 6);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)44, c.getTime());
			d.addItem("Update Utils to 3.1.1");
			d.addItem("Correcting a bug with key pair size limit");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.OCTOBER, 4);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)35, c.getTime());
			d.addItem("Update Utils to 3.0.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.SEPTEMBER, 8);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)34, c.getTime());
			d.addItem("Adding lock HSQLDB file possibility");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.SEPTEMBER, 8);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)33, c.getTime());
			d.addItem("Correcting a problem with internal tables creation");
			VERSION.addDescription(d);
			
			c = Calendar.getInstance();
			c.set(2017, Calendar.SEPTEMBER, 7);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)32, c.getTime());
			d.addItem("Correcting a problem of loop into Symbol.getFieldAccessor(Table<T>, Object)");
			VERSION.addDescription(d);

			
			c = Calendar.getInstance();
			c.set(2017, Calendar.SEPTEMBER, 7);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)30, c.getTime());
			d.addItem("Changing table lock policy");
			VERSION.addDescription(d);

			
			c = Calendar.getInstance();
			c.set(2017, Calendar.SEPTEMBER, 5);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)28, c.getTime());
			d.addItem("Changing transaction policy");
			d.addItem("Changing Calendar serialization method");
			d.addItem("Changing table locking method");
			d.addItem("Updating Utils to 2.16.0");
			d.addItem("Optimizing memory use during big data synchronisations between peers");
			d.addItem("Adding UUID field management");
			d.addItem("Possibility of use of NULL parameters into SQL Where commands");
			d.addItem("Now, JDBC drivers must be explicitly specified into dependencies of projects using OOD");
			d.addItem("Correcting rule instance locking problem");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 29);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)22, c.getTime());
			d.addItem("Possibility to insert null values for parameters");
			d.addItem("Changing locker");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 29);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)21, c.getTime());
			d.addItem("Correcting DateFieldAccessor when using null values");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 21);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)20, c.getTime());
			d.addItem("Updating Utils to 2.15.1");
			d.addItem("Minimal corrections");
			d.addItem("Corrections into the documentation");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 15);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)19, c.getTime());
			d.addItem("Optimizing database loading");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 15);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)18, c.getTime());
			d.addItem("Updating OOD to 2.15.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 13);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)17, c.getTime());
			d.addItem("Updating OOD to 2.14.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 10);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)16, c.getTime());
			d.addItem("Updating Utils to 2.12.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 9);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)15, c.getTime());
			d.addItem("Making HSQLDB not supporting 'LongVarBinary' type.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 5);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)14, c.getTime());
			d.addItem("Little byte tab primary keys are know possible for DerbyDB.");
			d.addItem("All decentralized database synchronization tests are OK with DerbyDB.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 5);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)11, c.getTime());
			d.addItem("Convert project to gradle project.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.JULY, 27);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)8, c.getTime());
			d.addItem("All decentralized database synchronization tests are OK with HSQLDB (but not with Derby DB).");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.JULY, 5);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)7, c.getTime());
			d.addItem("Tests synchro between two direct peers OK.");
			d.addItem("Correcting a problem of transaction lock.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.JULY, 2);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)6, c.getTime());
			d.addItem("Adding not null possibility for each sub field.");
			d.addItem("Begin debug decentralized database.");
			d.addItem("Optimizing tables junction.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.JUNE, 23);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)5, c.getTime());
			d.addItem("Adding not null possibility for each sub field.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.JUNE, 14);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)4, c.getTime());
			d.addItem("Debugging SQL interpreter.");
			d.addItem("Adding not null possibility for each sub field.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.JUNE, 1);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)3, c.getTime());
			d.addItem("Adding database factory.");
			d.addItem("Correcting a bug in database connections/disconnections.");
			d.addItem("Updating Utils to 2.8.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.MAY, 26);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)2, c.getTime());
			d.addItem("Debugging transaction's use.");
			d.addItem("Adding paginated queries.");
			d.addItem("Adding ordered queries.");
			d.addItem("Adding records numbers queries.");
			d.addItem("Correcting a bug with multi fields match queries.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.MAY, 24);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)1, c.getTime());
			d.addItem("Adding database synchronisation possibility between different peers (unstable).");
			d.addItem("Cleaning DatabaseWrapper functions.");
			d.addItem(
					"Correcting a lock problem into DatabaseWrapper.runSynchronizedTransaction(SynchronizedTransaction) function.");
			d.addItem("Optimizing Decentralized IDs.");
			d.addItem("Adding SQL where treatments (unstable).");
			d.addItem("Add indexation field possibility (see annotation Field).");
			d.addItem("Updating to Utils 2.7.1.");
			d.addItem("Downgrading to Derby 10.11.1.1 (JDK 7 Compatible).");
			d.addItem("Adding DatabaseConfiguration class.");
			d.addItem("Adding DatabaseLifeCycles class.");
			d.addItem(
					"Changing the database loading policy : enabling transfer data from old database to new database.");
			d.addItem("Minimizing code duplication of tests.");
			d.addItem("Adding remove database possibility.");
			d.addItem("Use of savepoint.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.MARCH, 7);
			d = new Description((short)1, (short)9, (short)7, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating Utils to 2.5.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.MARCH, 4);
			d = new Description((short)1, (short)9, (short)6, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating Utils to 2.4.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.FEBRUARY, 7);
			d = new Description((short)1, (short)9, (short)5, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating Utils to 2.3.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.JANUARY, 5);
			d = new Description((short)1, (short)9, (short)4, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating Utils to 2.2.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.DECEMBER, 31);
			d = new Description((short)1, (short)9, (short)3, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating Utils to 2.1.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.DECEMBER, 26);
			d = new Description((short)1, (short)9, (short)2, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Correcting a problem of data integrity check.");
			d.addItem("Updating Derby DB to 10.13.1.1.");
			d.addItem("Solving a problem of multithreading execution into windows (SQLNonTransientConnectionException).");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.DECEMBER, 24);
			d = new Description((short)1, (short)9, (short)1, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating Utils to version 2.0.1.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.DECEMBER, 19);
			d = new Description((short)1, (short)9, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating Utils to version 2.0.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.DECEMBER, 6);
			d = new Description((short)1, (short)8, (short)1, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating Utils to version 1.9.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.NOVEMBER, 30);
			d = new Description((short)1, (short)8, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Using ACID transactions instead of semaphores.");
			d.addItem("Adding HSQLDBConcurrencyControl class.");
			d.addItem("Adding memory refresh interval (see LoadToMemory annotation).");
			d.addItem("Adding the possibility to set composed fields by adding @Field to a class.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.OCTOBER, 13);
			d = new Description((short)1, (short)7, (short)1, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating Utils to 1.8.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.SEPTEMBER, 19);
			d = new Description((short)1, (short)7, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Correcting bug into FieldAccessor (null pointer exception).");
			d.addItem("Adding enum support.");
			d.addItem("Adding Decentralized ID support.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.AUGUST, 29);
			d = new Description((short)1, (short)6, (short)4, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating Utils to 1.7.2.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.AUGUST, 23);
			d = new Description((short)1, (short)6, (short)3, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating Utils to 1.7.1.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.JULY, 4);
			d = new Description((short)1, (short)6, (short)2, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating Utils to 1.7.");
			d.addItem("Updating to Common-Net 3.5.");
			d.addItem("Updating to HSQLDB 3.3.4.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.JUNE, 10);
			d = new Description((short)1, (short)6, (short)1, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Correction a bug into the constructor of ByteTabConvertibleFieldAccessor.");
			d.addItem("Adding version tests.");
			d.addItem("Changing license to CECILL-C.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.MARCH, 11);
			d = new Description((short)1, (short)6, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating with Utils 1.6.");
			d.addItem("Adding database nativeBackup tools.");
			d.addItem("renaming alterRecord functions to updateRecord.");
			d.addItem("Adding functions Table.addRecord(record), Table.updateRecord(record).");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.MARCH, 4);
			d = new Description((short)1, (short)5, (short)2, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating with Utils 1.5");
			d.addItem("Adding encryption keys encoding/decoding.");
			d.addItem("Correcting bugs with ByteTabConvertibleFieldAccessor class.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.MARCH, 1);
			d = new Description((short)1, (short)5, (short)1, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Updating with Utils 1.4.");
			d.addItem("Adding AllTestsNG.xml file.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.FEBRUARY, 15);
			d = new Description((short)1, (short)5, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Adding iterator functionality in class Table.");
			d.addItem("Adding ByteTabObjectConverter class.");
			d.addItem("Adding DefaultByteTabObjectConverter class.");
			d.addItem("Adding ByteTabConvertibleFieldAccessor class.");
			d.addItem("Adding function addByteTabObjectConverter in DatabaseWrapper class.");
			d.addItem(
					"Adding possibility to use Object tabs as an alternative of use of maps when referring to fields.");
			d.addItem("Optimizing use of SQL database.");
			d.addItem("Linking with Utils 1.3.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.FEBRUARY, 14);
			d = new Description((short)1, (short)4, (short)1, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Adding some close statements corrections.");
			d.addItem("Adding some multi-thread optimisations.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.FEBRUARY, 8);
			d = new Description((short)1, (short)4, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(
					"One database is associated to one package. Now, its is possible to load several database/packages into the same file.");
			d.addItem("OOD works now with HSQLDB or Apache Derby.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.FEBRUARY, 5);
			d = new Description((short)1, (short)3, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Adding dependency with Utils and updating OOD consequently.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.FEBRUARY, 1);
			d = new Description((short)1, (short)2, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Correcting some bugs into the documentation.");
			d.addItem("Upgrading to HSQLDB 2.3.3 and Commons-Net 3.4.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2013, Calendar.NOVEMBER, 18);
			d = new Description((short)1, (short)1, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Improving function Table.alterRecords(...) and class AlterRecordFilter (see documentation).");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2013, Calendar.APRIL, 24);
			d = new Description((short)1, (short)0, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem("Releasing Oriented Object Database as a stable version.");
			VERSION.addDescription(d);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("ResultOfMethodCallIgnored")
	public static void main(String[] args) throws IOException
	{
		String markdown=VERSION.getMarkdownCode();
		try(FileWriter fw=new FileWriter(new File("../versions.md")))
		{
			fw.write(markdown);
			fw.flush();
		}
		String lastVersion=VERSION.getFileHeadVersion();
		File f=new File("../lastVersion.md");
		if (f.exists())
			f.delete();
		try(FileWriter fr=new FileWriter(f))
		{
			fr.write(lastVersion);
			fr.flush();
		}
	}

}
