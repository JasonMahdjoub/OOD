
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
import static com.distrimind.util.version.DescriptionType.*;
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
							new Description(3, 2, 2, Version.Type.STABLE, 0, "2023-01-03")
									.addItem(INTERNAL_CHANGE, "Update Java language level to Java 11")
					)
					.addDescription(
							new Description(3, 2, 1, Version.Type.STABLE, 0, "2022-12-23")
									.addItem(INTERNAL_CHANGE, "Fix spelling errors")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.24.1")
					)
					.addDescription(
							new Description(3, 2, 0, Version.Type.STABLE, 0, "2022-12-08")
									.addItem(INTERNAL_CHANGE, "Target java compatibility is set to Java 11 but source code still use Java 8")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.24.0")
									.addItem(INTERNAL_CHANGE, "Update dependencies")
									.addItem(NEW_FEATURE, "Add database tasks manager, that permit to schedule periodic tasks executed into the database. See package com.distrimind.ood.database.tasks")
									.addItem(NEW_FEATURE, "Add possibility to specify a context with DatabaseWrapper. See DatabaseFactory.setContext(Object)")
									.addItem(NEW_FEATURE, "Add operator IN/NOT IN into pseudo SQL interpreter, that permit to test an identifier with a collection")
									.addItem(NEW_FEATURE, "Implements mathematical operator (+, -, *, /, %) with pseudo SQL queries. Theses operators work with Numbers, and Date values.")
									.addItem(INTERNAL_CHANGE, "Significant optimizations of pseudo SQL interpreter.")
					)
					.addDescription(
							new Description(3, 1, 26, Version.Type.STABLE, 0, "2022-04-07")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.23.5 STABLE")
					)
					.addDescription(
							new Description(3, 1, 22, Version.Type.STABLE, 0, "2022-04-05")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.23.2 STABLE")
									.addItem(INTERNAL_CHANGE, "Add lacking files into generated libraries.")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)21, Version.Type.STABLE, (short)0, "2022-04-05")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.23.1 STABLE")
									.addItem(INTERNAL_CHANGE, "Remove finalize methods into all classes, and these classes using Cleanable API")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)20, Version.Type.STABLE, (short)0, "2022-03-29")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.22.4 STABLE")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)19, Version.Type.STABLE, (short)0, "2022-03-25")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.22.3 STABLE")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)17, Version.Type.STABLE, (short)0, "2022-02-08")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.22.2 STABLE")
									.addItem(INTERNAL_CHANGE, "Update HSQLDB to 2.6.1")
									.addItem(INTERNAL_CHANGE, "Update H2 to 2.1.210")
									.addItem(INTERNAL_CHANGE, "Update mysql-connector-java 8.0.28")
									.addItem(INTERNAL_CHANGE, "Update postgresql 42.3.2")
									.addItem(INTERNAL_CHANGE, "Minimum Android version to use H2 is Android 8.0 or API26")
									.addItem(BUG_FIX, "Check if certificate is given when initialising database configurations that are synchronized with central database backup")
									.addItem(INTERNAL_CHANGE, "Update URLs")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)16, Version.Type.STABLE, (short)0, "2022-01-24")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.22.1 STABLE")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)15, Version.Type.STABLE, (short)0, "2022-01-24")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.22.0 STABLE")
									.addItem(NEW_FEATURE, "Manage WrappedString and WrappedData classes into field accessors and into queries")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)14, Version.Type.STABLE, (short)0, "2021-12-22")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.21.7 STABLE")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)13, Version.Type.STABLE, (short)0, "2021-12-22")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.21.6 STABLE")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)12, Version.Type.STABLE, (short)0, "2021-12-16")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.21.5 STABLE")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)11, Version.Type.STABLE, (short)0, "2021-12-15")
									.addItem(INTERNAL_CHANGE, "Update Gradle to 7.3.1")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)10, Version.Type.STABLE, (short)0, "2021-12-09")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.21.3")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)9, Version.Type.STABLE, (short)0, "2021-12-03")
									.addItem(INTERNAL_CHANGE, "Little corrections")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)8, Version.Type.STABLE, (short)0, "2021-12-03")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.21.2 STABLE")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)7, Version.Type.STABLE, (short)0, "2021-11-02")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.21.0 STABLE")
									.addItem(INTERNAL_CHANGE, "Update MySQL JDBC driver to 8.0.27")
									.addItem(INTERNAL_CHANGE, "Update PostgreSQL JDBC driver to 42.3.1")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)6, Version.Type.STABLE, (short)0, "2021-10-18")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.20.6 STABLE")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)3, Version.Type.STABLE, (short)0, "2021-10-13")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.20.3 STABLE")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)2, Version.Type.STABLE, (short)0, "2021-10-13")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.20.2 STABLE")
									.addItem(BUG_FIX,"Fix issue when sending messages from central server to disconnected peers")
									.addItem(BUG_FIX, "Fix issue with some messages that where not sent from one server, when several servers where used")
									.addItem(INTERNAL_CHANGE, "Better log formatting")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)1, Version.Type.STABLE, (short)0, "2021-10-01")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.19.7 STABLE")
									.addItem(BUG_FIX, "Fix issue and better update last restoration timestamp of a database")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)0, Version.Type.STABLE, (short)0, "2021-09-30")
									.addItem(NEW_FEATURE, "Add file manager")
									.addItem(INTERNAL_CHANGE, "Remove concatenated sql queries")
									.addItem(INTERNAL_CHANGE, "Clean code")
									.addItem(NEW_FEATURE, "Permit indirect initial synchronization between peers through central database backup")
									.addItem(NEW_FEATURE, "Automatically restore database from server during first connection")
									.addItem(BUG_FIX, "Fix issue and avoid creating backup reference when the database is empty. Create first backup reference at the start of the transaction, before the first queries")
									.addItem(BUG_FIX, "Fix issue when loading all fields instead of loading only primary keys from backup")
									.addItem(BUG_FIX, "Fix issue when database is restored to an old version, and when old synchronization message from other peers are received just after the restoration. Now these messages are ignored when timestamp are lower than the last restoration time.")
									.addItem(BUG_FIX, "Fix synchronization lock when two peers must apply synchronization through central database backup and whereas initial database is empty into each peer.")
					)
					.addDescription(
							new Description((short)3, (short)1, (short)0, Version.Type.BETA, (short)1, "2021-07-07")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.18.5")
									.addItem(BUG_FIX, "Make OOD compatible with MadKitLanEdition")
									.addItem(BUG_FIX, "Fix NullPointerException into DatabaseConfiguration class")
									.addItem(NEW_FEATURE, "Add function void DatabaseFactory.setEncryptionProfileProviders(EncryptionProfileProviderFactory, SecureRandomType randomType, byte[], byte[])")
									.addItem(NEW_FEATURE, "Add function void DatabaseFactory.setEncryptionProfileProviders(EncryptionProfileProviderFactory, SecureRandomType randomType)")
									.addItem(BUG_FIX, "Fix bad using of central database backup when it is not used")
									.addItem(BUG_FIX, "Fix issue with notification of local host initialization")
									.addItem(BUG_FIX, "Fix issue : update internal table state when database becomes decentralized")
									.addItem(BUG_FIX, "Fix issue with DatabaseConfigurationsBuilder")
									.addItem(BUG_FIX, "Fix issue when removing peer whereas OOD does not use CentralDatabaseBackup")
									.addItem(BUG_FIX, "Fix issue with function DatabaseConfigurationBuilder.resetSynchronizerAndRemoveAllHosts() : distant peers and local host id were not removed")
									.addItem(NEW_FEATURE, "Add class CentralDatabaseBackupReceiverFactory")
									.addItem(BUG_FIX, "Fix null pointer exceptions")
									.addItem(BUG_FIX, "Fix class cast exceptions")
									.addItem(NEW_FEATURE, "Add function CentralDatabaseBackupReceiver.sendMessageFromThisCentralDatabaseBackup(MessageComingFromCentralDatabaseBackup)")
									.addItem(NEW_FEATURE, "Add function BackupRestoreManager.hasNonFinalFiles()")
									.addItem(NEW_FEATURE, "Add function DatabaseWrapper.getNextPossibleEventTimeUTC()")
					)
					.addDescription(
							new Description((short)3, (short)0, (short)0, Version.Type.STABLE, (short)0, "2021-05-24")
									.addItem(INTERNAL_CHANGE, "Update Utils to 5.17.6")
									.addItem(INTERNAL_CHANGE, "Update HSQLDB to 2.5.1")
									.addItem(NEW_FEATURE, "Implementation of data synchronization with central database backup")
									.addItem(NEW_FEATURE, "Each pear can transfer its encrypted backup into the central database that do not permit backup reversion in a short time.")
									.addItem(NEW_FEATURE, "Removed data into central database backup are removed only after a delay has occurred. Same thing with removed accounts.")
									.addItem(NEW_FEATURE, "When a peer is added into the network, it is not necessary to add it into each peach. The adding is broadcast into the network.")
									.addItem(NEW_FEATURE, "Primary keys can be now decentralized")
									.addItem(NEW_FEATURE, "Do not synchronize database that do not share the same version")
									.addItem(BUG_FIX, "Make OOD compatible with Android")
									.addItem(NEW_FEATURE, "Implementation of driver for MySQL")
									.addItem(NEW_FEATURE, "Implementation of driver for Android H2 Database")
									.addItem(NEW_FEATURE, "Implementation of driver for PostgreSQL")
									.addItem(INTERNAL_CHANGE, "Compatible with Java 8 and newer")
									.addItem(INTERNAL_CHANGE, "Calendar is better serialized into database (better performances, and lower space)")
									.addItem(NEW_FEATURE,  "Calendar is now comparable")
									.addItem(NEW_FEATURE, "Implementation of DatabaseConfigurationBuilder that centralize database loading, database restoration, and database synchronization with decentralized peers and with central backup database. The profile can be saved into the disk.")
									.addItem(NEW_FEATURE, "Add function Table.removeAllRecordsWithCascade()")
									.addItem(BUG_FIX,"Fix issue when changing database's version and refresh tables state when BackupRestoreManager was loaded")
									.addItem(NEW_FEATURE, "Add function Table.hasRecords(String, Map)")
									.addItem(NEW_FEATURE, "Add function Table.hasRecords(Filter, String, Map)")
									.addItem(NEW_FEATURE, "Add function DatabaseLifeCycles.peersAdded(Set<DecentralizedValue>) that is triggered when a set of peers was added")
									.addItem(BUG_FIX, "Fix join issue when table A has two foreign keys that reference the same table B")
									.addItem(INTERNAL_CHANGE, "Optimize queries by using junctions")
									.addItem(NEW_FEATURE, "Add possibility to personalize the moment when OOD will create a new database backup")
									.addItem(NEW_FEATURE, "Add possibility to notify user that new database backup files were created")
									.addItem(NEW_FEATURE, "Extends authenticated messages for critical queries, by using protected keys")
									.addItem(NEW_FEATURE, "When local host identifier is changed or removed, distant hosts are notified")
					)
					.addDescription(
							new Description((short)2, (short)4, (short)0, Version.Type.STABLE, (short)1, "2020-02-11")
									.addItem(INTERNAL_CHANGE, "Update Utils to 4.9.0")
									.addItem(INTERNAL_CHANGE, "Update database factories")
									.addItem(NEW_FEATURE, "Database wrappers are now instantiable only through factories")
					);

			Calendar c = Calendar.getInstance();
			c.set(2020, Calendar.JANUARY, 24);
			Description d = new Description((short)2, (short)3, (short)21, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 4.8.6");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2020, Calendar.JANUARY, 7);
			d = new Description((short)2, (short)3, (short)14, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Rename RandomPrivateKey.byteNumber but do not change its behavior");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.DECEMBER, 16);
			d = new Description((short)2, (short)3, (short)13, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 4.7.1 STABLE");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.NOVEMBER, 22);
			d = new Description((short)2, (short)3, (short)12, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 4.7.0 STABLE");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.NOVEMBER, 15);
			d = new Description((short)2, (short)3, (short)10, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 4.6.5 STABLE");
			d.addItem(INTERNAL_CHANGE, "Compile with openjdk 13 (compatibility set to Java 7");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.NOVEMBER, 13);
			d = new Description((short)2, (short)3, (short)8, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 4.6.3 STABLE");
			d.addItem(BUG_FIX, "Fix backup cache upgrade issue when canceling transaction");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.OCTOBER, 31);
			d = new Description((short)2, (short)3, (short)7, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 4.6.2 STABLE");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.OCTOBER, 20);
			d = new Description((short)2, (short)3, (short)6, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update dependencies");
			VERSION.addDescription(d);



			c = Calendar.getInstance();
			c.set(2019, Calendar.OCTOBER, 17);
			d = new Description((short)2, (short)3, (short)4, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 4.6.0 STABLE");
			d.addItem(BUG_FIX, "Fix NullPointerException into DatabaseWrapper$DatabaseSynchronizer.getLocalHostID()");
			d.addItem(BUG_FIX, "Fix HookAddRequest externalisation");
			d.addItem(BUG_FIX, "Fix issue when loading the same database two times");
			d.addItem(INTERNAL_CHANGE, "Better reset synchronization");
			d.addItem(INTERNAL_CHANGE, "Better disconnect all hosts");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.SEPTEMBER, 2);
			d = new Description((short)2, (short)2, (short)1, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem(NEW_FEATURE, "Add function DatabaseWrapper.Synchronizer.getDistantHostsIds()");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.JULY, 16);
			d = new Description((short)2, (short)2, (short)0, Version.Type.STABLE, (short)1, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 4.4.3 STABLE");
			d.addItem(NEW_FEATURE, "Add backup/restore manager, with historical management");
			d.addItem(INTERNAL_CHANGE, "Better manage database versions");
			d.addItem(INTERNAL_CHANGE, "Optimizations of several queries");
			d.addItem(NEW_FEATURE, "Add function Table.removeAllRecordsWithCascade()");
			d.addItem(INTERNAL_CHANGE, "Use long values for queries limited by a number of rows");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.MAY, 6);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)107, c.getTime());
			d.addItem(NEW_FEATURE, "Update Utils to 3.26.0 STABLE");
			d.addItem(NEW_FEATURE, "Add Field.includeKeyExpiration()");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.APRIL, 23);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)106, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 3.25.6 STABLE");
			d.addItem(BUG_FIX, "Fix problem with getRecordsNumber function using where condition");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.MARCH, 21);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)104, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 3.25.5 STABLE");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.MARCH, 13);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)103, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 3.25.4 STABLE");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.MARCH, 1);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)100, c.getTime());
			d.addItem(NEW_FEATURE, "Add function DatabaseCollisionsNotifier.areDuplicatedEventsNotConsideredAsCollisions()");
			d.addItem(NEW_FEATURE, "Add function DatabaseCollisionsNotifier.startNewSynchronizationTransaction()");
			d.addItem(NEW_FEATURE, "Add function DatabaseCollisionsNotifier.endSynchronizationTransaction()");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.FEBRUARY, 8);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)99, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Better support of sub fields into queries");
			VERSION.addDescription(d);


			c = Calendar.getInstance();
			c.set(2019, Calendar.FEBRUARY, 6);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)98, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 3.25.1");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.FEBRUARY, 5);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)97, c.getTime());
			d.addItem(SECURITY_FIX, "disable cache for tables that use secret ou private keys");
			d.addItem(SECURITY_FIX, "Security improvement : add Field.disableCache property");
			d.addItem(NEW_FEATURE, "Add function Table.isCached()");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.JANUARY, 25);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)96, c.getTime());
			d.addItem(NEW_FEATURE, "Add function Table.removeRecord(Map keys)");
			d.addItem(NEW_FEATURE, "Add function Table.removeRecord(Object...keys)");
			d.addItem(NEW_FEATURE, "Add function Table.removeRecordWithCascade(Map keys)");
			d.addItem(NEW_FEATURE, "Add function Table.removeRecordWithCascade(Object...keys)");
			d.addItem(BUG_FIX, "Do not generate conflicts if events are the same");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.JANUARY, 18);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)95, c.getTime());
			d.addItem(NEW_FEATURE, "Add H2 database driver");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2019, Calendar.JANUARY, 15);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)94, c.getTime());
			d.addItem(NEW_FEATURE, "Use shortest table's name");
			d.addItem(NEW_FEATURE, "Add possibility to personalize SQL table name (see annotation TableName)");
			d.addItem(NEW_FEATURE, "Add possibility to personalize SQL field name (see annotation Field)");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2018, Calendar.DECEMBER, 17);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)93, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating utils to 3.24.0");
			VERSION.addDescription(d);


			c = Calendar.getInstance();
			c.set(2018, Calendar.NOVEMBER, 13);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)90, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating utils to 3.22.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2018, Calendar.NOVEMBER, 7);
			 d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)89, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating utils to 3.21.1");
			d.addItem(INTERNAL_CHANGE, "Manage Keys used for encryption as decentralized keys");
			d.addItem(NEW_FEATURE, "Add DatabaseAnomaliesNotifier interface");
			d.addItem(NEW_FEATURE, "Add DatabaseCollisionsNotifier interface");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2018, Calendar.JULY, 27);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)85, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating utils to 3.18.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2018, Calendar.JULY, 17);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)84, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating utils to 3.17.0");
            d.addItem(BUG_FIX, "Do not loop infinitely if disconnection exception is generated infinitely");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2018, Calendar.JULY, 12);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)83, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating utils to 3.16.1");
            d.addItem(INTERNAL_CHANGE, "Clean code");
            d.addItem(NEW_FEATURE, "Add auto disconnection option with database wrappers");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2018, Calendar.MAY, 16);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)82, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating utils to 3.15.0");
			VERSION.addDescription(d);
			
			c = Calendar.getInstance();
			c.set(2018, Calendar.MARCH, 30);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)71, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Optimization of CachedInputStream");
			d.addItem(INTERNAL_CHANGE, "Optimization of CachedOutputStream");
			d.addItem(INTERNAL_CHANGE, "Updating utils to 3.13.1");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2018, Calendar.FEBRUARY, 10);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)66, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating utils to 3.10.5");
			d.addItem(BUG_FIX, "Correcting a bug with the nativeBackup of enum variables");
			VERSION.addDescription(d);
			
			c = Calendar.getInstance();
			c.set(2018, Calendar.JANUARY, 31);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)59, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating utils to 3.9.0");
			VERSION.addDescription(d);
			
			c = Calendar.getInstance();
			c.set(2017, Calendar.DECEMBER, 13);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)57, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating utils to 3.7.1");
			d.addItem(NEW_FEATURE, "Managing disconnection database exception/reconnection");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.OCTOBER, 16);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)50, c.getTime());
			d.addItem(NEW_FEATURE, "Adding secure random functions into database wrapper");
			d.addItem(NEW_FEATURE, "public/private/secret keys can now be primary keys");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.OCTOBER, 16);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)50, c.getTime());
			d.addItem(NEW_FEATURE, "Manage File fields");
			d.addItem(NEW_FEATURE, "Add ExcludeFromDecentralization annotation !");
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 3.3.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.OCTOBER, 13);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)49, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 3.2.4");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.OCTOBER, 6);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)44, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 3.1.1");
			d.addItem(BUG_FIX, "Correcting a bug with key pair size limit");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.OCTOBER, 4);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)35, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Update Utils to 3.0.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.SEPTEMBER, 8);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)34, c.getTime());
			d.addItem(NEW_FEATURE, "Adding lock HSQLDB file possibility");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.SEPTEMBER, 8);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)33, c.getTime());
			d.addItem(BUG_FIX, "Correcting a problem with internal tables creation");
			VERSION.addDescription(d);
			
			c = Calendar.getInstance();
			c.set(2017, Calendar.SEPTEMBER, 7);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)32, c.getTime());
			d.addItem(BUG_FIX, "Correcting a problem of loop into Symbol.getFieldAccessor(Table<T>, Object)");
			VERSION.addDescription(d);

			
			c = Calendar.getInstance();
			c.set(2017, Calendar.SEPTEMBER, 7);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)30, c.getTime());
			d.addItem(BUG_FIX, "Changing table lock policy");
			VERSION.addDescription(d);

			
			c = Calendar.getInstance();
			c.set(2017, Calendar.SEPTEMBER, 5);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)28, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Changing transaction policy");
			d.addItem(INTERNAL_CHANGE, "Changing Calendar serialization method");
			d.addItem(INTERNAL_CHANGE, "Changing table locking method");
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 2.16.0");
			d.addItem(INTERNAL_CHANGE, "Optimizing memory use during big data synchronisations between peers");
			d.addItem(NEW_FEATURE, "Adding UUID field management");
			d.addItem(NEW_FEATURE, "Possibility of use of NULL parameters into SQL Where commands");
			d.addItem(INTERNAL_CHANGE, "Now, JDBC drivers must be explicitly specified into dependencies of projects using OOD");
			d.addItem(BUG_FIX, "Correcting rule instance locking problem");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 29);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)22, c.getTime());
			d.addItem(NEW_FEATURE, "Possibility to insert null values for parameters");
			d.addItem(BUG_FIX, "Changing locker");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 29);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)21, c.getTime());
			d.addItem(BUG_FIX, "Correcting DateFieldAccessor when using null values");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 21);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)20, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 2.15.1");
			d.addItem(INTERNAL_CHANGE, "Minimal corrections");
			d.addItem(INTERNAL_CHANGE, "Corrections into the documentation");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 15);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)19, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Optimizing database loading");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 15);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)18, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 2.15.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 13);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)17, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 2.14.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 10);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)16, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 2.12.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 9);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)15, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Making HSQLDB not supporting 'LongVarBinary' type.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 5);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)14, c.getTime());
			d.addItem(NEW_FEATURE, "Little byte tab primary keys are know possible for DerbyDB.");
			d.addItem(BUG_FIX, "All decentralized database synchronization tests are OK with DerbyDB.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.AUGUST, 5);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)11, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Convert project to gradle project.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.JULY, 27);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)8, c.getTime());
			d.addItem(BUG_FIX, "All decentralized database synchronization tests are OK with HSQLDB (but not with Derby DB).");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.JULY, 5);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)7, c.getTime());
			d.addItem(BUG_FIX, "Tests synchro between two direct peers OK.");
			d.addItem(BUG_FIX, "Correcting a problem of transaction lock.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.JULY, 2);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)6, c.getTime());
			d.addItem(NEW_FEATURE, "Adding not null possibility for each sub field.");
			d.addItem(BUG_FIX, "Begin debug decentralized database.");
			d.addItem(INTERNAL_CHANGE, "Optimizing tables junction.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.JUNE, 23);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)5, c.getTime());
			d.addItem(NEW_FEATURE, "Adding not null possibility for each sub field.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.JUNE, 14);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)4, c.getTime());
			d.addItem(BUG_FIX, "Debugging SQL interpreter.");
			d.addItem(NEW_FEATURE, "Adding not null possibility for each sub field.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.JUNE, 1);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)3, c.getTime());
			d.addItem(NEW_FEATURE, "Adding database factory.");
			d.addItem(BUG_FIX, "Correcting a bug in database connections/disconnections.");
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 2.8.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.MAY, 26);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)2, c.getTime());
			d.addItem(BUG_FIX, "Debugging transaction's use.");
			d.addItem(NEW_FEATURE, "Adding paginated queries.");
			d.addItem(NEW_FEATURE, "Adding ordered queries.");
			d.addItem(NEW_FEATURE, "Adding records numbers queries.");
			d.addItem(BUG_FIX, "Correcting a bug with multi fields match queries.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.MAY, 24);
			d = new Description((short)2, (short)0, (short)0, Version.Type.BETA, (short)1, c.getTime());
			d.addItem(NEW_FEATURE, "Adding database synchronisation possibility between different peers (unstable).");
			d.addItem(INTERNAL_CHANGE, "Cleaning DatabaseWrapper functions.");
			d.addItem(BUG_FIX, "Correcting a lock problem into DatabaseWrapper.runSynchronizedTransaction(SynchronizedTransaction) function.");
			d.addItem(INTERNAL_CHANGE,"Optimizing Decentralized IDs.");
			d.addItem(NEW_FEATURE, "Adding SQL where treatments (unstable).");
			d.addItem(NEW_FEATURE, "Add indexation field possibility (see annotation Field).");
			d.addItem(INTERNAL_CHANGE, "Updating to Utils 2.7.1.");
			d.addItem(INTERNAL_CHANGE, "Downgrading to Derby 10.11.1.1 (JDK 7 Compatible).");
			d.addItem(NEW_FEATURE, "Adding DatabaseConfiguration class.");
			d.addItem(NEW_FEATURE, "Adding DatabaseLifeCycles class.");
			d.addItem(INTERNAL_CHANGE, "Changing the database loading policy : enabling transfer data from old database to new database.");
			d.addItem(INTERNAL_CHANGE, "Minimizing code duplication of tests.");
			d.addItem(NEW_FEATURE, "Adding remove database possibility.");
			d.addItem(NEW_FEATURE, "Use of savepoint.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.MARCH, 7);
			d = new Description((short)1, (short)9, (short)7, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 2.5.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.MARCH, 4);
			d = new Description((short)1, (short)9, (short)6, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 2.4.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.FEBRUARY, 7);
			d = new Description((short)1, (short)9, (short)5, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 2.3.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, Calendar.JANUARY, 5);
			d = new Description((short)1, (short)9, (short)4, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 2.2.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.DECEMBER, 31);
			d = new Description((short)1, (short)9, (short)3, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 2.1.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.DECEMBER, 26);
			d = new Description((short)1, (short)9, (short)2, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(BUG_FIX, "Correcting a problem of data integrity check.");
			d.addItem(INTERNAL_CHANGE, "Updating Derby DB to 10.13.1.1.");
			d.addItem(BUG_FIX, "Solving a problem of multithreading execution into windows (SQLNonTransientConnectionException).");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.DECEMBER, 24);
			d = new Description((short)1, (short)9, (short)1, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to version 2.0.1.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.DECEMBER, 19);
			d = new Description((short)1, (short)9, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to version 2.0.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.DECEMBER, 6);
			d = new Description((short)1, (short)8, (short)1, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to version 1.9.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.NOVEMBER, 30);
			d = new Description((short)1, (short)8, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(NEW_FEATURE, "Using ACID transactions instead of semaphores.");
			d.addItem(NEW_FEATURE, "Adding HSQLDBConcurrencyControl class.");
			d.addItem(NEW_FEATURE, "Adding memory refresh interval (see LoadToMemory annotation).");
			d.addItem(NEW_FEATURE, "Adding the possibility to set composed fields by adding @Field to a class.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.OCTOBER, 13);
			d = new Description((short)1, (short)7, (short)1, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 1.8.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.SEPTEMBER, 19);
			d = new Description((short)1, (short)7, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(BUG_FIX, "Correcting bug into FieldAccessor (null pointer exception).");
			d.addItem(NEW_FEATURE, "Adding enum support.");
			d.addItem(NEW_FEATURE, "Adding Decentralized ID support.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.AUGUST, 29);
			d = new Description((short)1, (short)6, (short)4, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 1.7.2.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.AUGUST, 23);
			d = new Description((short)1, (short)6, (short)3, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 1.7.1.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.JULY, 4);
			d = new Description((short)1, (short)6, (short)2, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating Utils to 1.7.");
			d.addItem(INTERNAL_CHANGE, "Updating to Common-Net 3.5.");
			d.addItem(INTERNAL_CHANGE, "Updating to HSQLDB 3.3.4.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.JUNE, 10);
			d = new Description((short)1, (short)6, (short)1, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(BUG_FIX, "Correction a bug into the constructor of ByteTabConvertibleFieldAccessor.");
			d.addItem(BUG_FIX, "Adding version tests.");
			d.addItem(INTERNAL_CHANGE, "Changing license to CECILL-C.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.MARCH, 11);
			d = new Description((short)1, (short)6, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating with Utils 1.6.");
			d.addItem(NEW_FEATURE, "Adding database nativeBackup tools.");
			d.addItem(INTERNAL_CHANGE, "renaming alterRecord functions to updateRecord.");
			d.addItem(INTERNAL_CHANGE, "Adding functions Table.addRecord(record), Table.updateRecord(record).");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.MARCH, 4);
			d = new Description((short)1, (short)5, (short)2, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating with Utils 1.5");
			d.addItem(NEW_FEATURE, "Adding encryption keys encoding/decoding.");
			d.addItem(BUG_FIX, "Correcting bugs with ByteTabConvertibleFieldAccessor class.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.MARCH, 1);
			d = new Description((short)1, (short)5, (short)1, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Updating with Utils 1.4.");
			d.addItem(INTERNAL_CHANGE, "Adding AllTestsNG.xml file.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.FEBRUARY, 15);
			d = new Description((short)1, (short)5, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(NEW_FEATURE, "Adding iterator functionality in class Table.");
			d.addItem(NEW_FEATURE, "Adding ByteTabObjectConverter class.");
			d.addItem(NEW_FEATURE, "Adding DefaultByteTabObjectConverter class.");
			d.addItem(NEW_FEATURE, "Adding ByteTabConvertibleFieldAccessor class.");
			d.addItem(NEW_FEATURE, "Adding function addByteTabObjectConverter in DatabaseWrapper class.");
			d.addItem(NEW_FEATURE, "Adding possibility to use Object tabs as an alternative of use of maps when referring to fields.");
			d.addItem(INTERNAL_CHANGE, "Optimizing use of SQL database.");
			d.addItem(INTERNAL_CHANGE, "Linking with Utils 1.3.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.FEBRUARY, 14);
			d = new Description((short)1, (short)4, (short)1, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(BUG_FIX, "Adding some close statements corrections.");
			d.addItem(INTERNAL_CHANGE, "Adding some multi-thread optimisations.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.FEBRUARY, 8);
			d = new Description((short)1, (short)4, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(NEW_FEATURE, "One database is associated to one package. Now, its is possible to load several database/packages into the same file.");
			d.addItem(NEW_FEATURE, "OOD works now with HSQLDB or Apache Derby.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.FEBRUARY, 5);
			d = new Description((short)1, (short)3, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Adding dependency with Utils and updating OOD consequently.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, Calendar.FEBRUARY, 1);
			d = new Description((short)1, (short)2, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(BUG_FIX, "Correcting some bugs into the documentation.");
			d.addItem(INTERNAL_CHANGE, "Upgrading to HSQLDB 2.3.3 and Commons-Net 3.4.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2013, Calendar.NOVEMBER, 18);
			d = new Description((short)1, (short)1, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(INTERNAL_CHANGE, "Improving function Table.alterRecords(...) and class AlterRecordFilter (see documentation).");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2013, Calendar.APRIL, 24);
			d = new Description((short)1, (short)0, (short)0, Version.Type.STABLE, (short)0, c.getTime());
			d.addItem(NEW_FEATURE, "Releasing Oriented Object Database as a stable version.");
			VERSION.addDescription(d);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("ResultOfMethodCallIgnored")
	public static void main(String[] args) throws IOException
	{
		String markdown=VERSION.getMarkdownCode();
		try(FileWriter fw=new FileWriter("changelog.md"))
		{
			fw.write(markdown);
			fw.flush();
		}
		String lastVersion=VERSION.getFileHeadVersion();
		File f=new File("lastVersion.md");
		if (f.exists())
			f.delete();
		try(FileWriter fr=new FileWriter(f))
		{
			fr.write(lastVersion);
			fr.flush();
		}
	}

}
