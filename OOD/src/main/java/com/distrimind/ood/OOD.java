
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
package com.distrimind.ood;

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
		Calendar c1 = Calendar.getInstance();
		c1.set(2013, 3, 1);
		Calendar c2 = Calendar.getInstance();
		c2.set(2018, 0, 31);
		VERSION = new Version("Object Oriented Database", "OOD", 2, 0, 0, Version.Type.Beta, 58, c1.getTime(),
				c2.getTime());
		try {
			InputStream is = OOD.class.getResourceAsStream("build.txt");
			if (is != null)
				VERSION.loadBuildNumber(is);
			VERSION.addCreator(new Person("mahdjoub", "jason"));
			Calendar c = Calendar.getInstance();
			c.set(2013, 3, 1);
			VERSION.addDeveloper(new PersonDeveloper("mahdjoub", "jason", c.getTime()));

			c = Calendar.getInstance();
			c.set(2018, 0, 31);
			Description d = new Description(2, 0, 0, Version.Type.Beta, 58, c.getTime());
			d.addItem("Updating utils to 3.8.0");
			VERSION.addDescription(d);
			
			c = Calendar.getInstance();
			c.set(2017, 11, 13);
			d = new Description(2, 0, 0, Version.Type.Beta, 57, c.getTime());
			d.addItem("Updating utils to 3.7.1");
			d.addItem("Managing deconnection database exception/reconnection");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 9, 16);
			d = new Description(2, 0, 0, Version.Type.Beta, 50, c.getTime());
			d.addItem("Adding secure random functions into database wrapper");
			d.addItem("public/private/secret keys can now be primary keys");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 9, 16);
			d = new Description(2, 0, 0, Version.Type.Beta, 50, c.getTime());
			d.addItem("Manage File fields");
			d.addItem("Add ExcludeFromDecentralization annotation !");
			d.addItem("Updating Utils to 3.3.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 9, 13);
			d = new Description(2, 0, 0, Version.Type.Beta, 49, c.getTime());
			d.addItem("Update Utils to 3.2.4");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 9, 6);
			d = new Description(2, 0, 0, Version.Type.Beta, 44, c.getTime());
			d.addItem("Update Utils to 3.1.1");
			d.addItem("Correcting a bug with key pair size limit");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 9, 4);
			d = new Description(2, 0, 0, Version.Type.Beta, 35, c.getTime());
			d.addItem("Update Utils to 3.0.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 8, 8);
			d = new Description(2, 0, 0, Version.Type.Beta, 34, c.getTime());
			d.addItem("Adding lock HSQLDB file possibility");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 8, 8);
			d = new Description(2, 0, 0, Version.Type.Beta, 33, c.getTime());
			d.addItem("Correcting a problem with internal tables creation");
			VERSION.addDescription(d);
			
			c = Calendar.getInstance();
			c.set(2017, 8, 7);
			d = new Description(2, 0, 0, Version.Type.Beta, 32, c.getTime());
			d.addItem("Correcting a problem of loop into Symbol.getFieldAccessor(Table<T>, Object)");
			VERSION.addDescription(d);

			
			c = Calendar.getInstance();
			c.set(2017, 8, 7);
			d = new Description(2, 0, 0, Version.Type.Beta, 30, c.getTime());
			d.addItem("Changing table lock policy");
			VERSION.addDescription(d);

			
			c = Calendar.getInstance();
			c.set(2017, 8, 5);
			d = new Description(2, 0, 0, Version.Type.Beta, 28, c.getTime());
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
			c.set(2017, 7, 29);
			d = new Description(2, 0, 0, Version.Type.Beta, 22, c.getTime());
			d.addItem("Possibility to insert null values for parameters");
			d.addItem("Changing locker");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 7, 29);
			d = new Description(2, 0, 0, Version.Type.Beta, 21, c.getTime());
			d.addItem("Correcting DateFieldAccessor when using null values");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 7, 21);
			d = new Description(2, 0, 0, Version.Type.Beta, 20, c.getTime());
			d.addItem("Updating Utils to 2.15.1");
			d.addItem("Minimal corrections");
			d.addItem("Corrections into the documentation");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 7, 15);
			d = new Description(2, 0, 0, Version.Type.Beta, 19, c.getTime());
			d.addItem("Optimizing database loading");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 7, 15);
			d = new Description(2, 0, 0, Version.Type.Beta, 18, c.getTime());
			d.addItem("Updating OOD to 2.15.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 7, 13);
			d = new Description(2, 0, 0, Version.Type.Beta, 17, c.getTime());
			d.addItem("Updating OOD to 2.14.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 7, 10);
			d = new Description(2, 0, 0, Version.Type.Beta, 16, c.getTime());
			d.addItem("Updating Utils to 2.12.0");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 7, 9);
			d = new Description(2, 0, 0, Version.Type.Beta, 15, c.getTime());
			d.addItem("Making HSQLDB not supporting 'LongVarBinary' type.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 7, 5);
			d = new Description(2, 0, 0, Version.Type.Beta, 14, c.getTime());
			d.addItem("Little byte tab primary keys are know possible for DerbyDB.");
			d.addItem("All decentralized database synchronization tests are OK with DerbyDB.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 7, 5);
			d = new Description(2, 0, 0, Version.Type.Beta, 11, c.getTime());
			d.addItem("Convert project to gradle project.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 6, 27);
			d = new Description(2, 0, 0, Version.Type.Beta, 8, c.getTime());
			d.addItem("All decentralized database synchronization tests are OK with HSQLDB (but not with Derby DB).");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 6, 5);
			d = new Description(2, 0, 0, Version.Type.Beta, 7, c.getTime());
			d.addItem("Tests synchro between two direct peers OK.");
			d.addItem("Correcting a problem of transaction lock.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 6, 2);
			d = new Description(2, 0, 0, Version.Type.Beta, 6, c.getTime());
			d.addItem("Adding not null possibility for each sub field.");
			d.addItem("Begin debug decentralized database.");
			d.addItem("Optimizing tables junction.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 5, 23);
			d = new Description(2, 0, 0, Version.Type.Beta, 5, c.getTime());
			d.addItem("Adding not null possibility for each sub field.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 5, 14);
			d = new Description(2, 0, 0, Version.Type.Beta, 4, c.getTime());
			d.addItem("Debugging SQL interpreter.");
			d.addItem("Adding not null possibility for each sub field.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 5, 1);
			d = new Description(2, 0, 0, Version.Type.Beta, 3, c.getTime());
			d.addItem("Adding database factory.");
			d.addItem("Correcting a bug in database connections/deconnections.");
			d.addItem("Updating Utils to 2.8.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 4, 26);
			d = new Description(2, 0, 0, Version.Type.Beta, 2, c.getTime());
			d.addItem("Debuging transaction's use.");
			d.addItem("Adding pagined queries.");
			d.addItem("Adding ordered queries.");
			d.addItem("Adding records numbers queries.");
			d.addItem("Correcting a bug with multi fields match querries.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 4, 24);
			d = new Description(2, 0, 0, Version.Type.Beta, 1, c.getTime());
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
			d.addItem("Adding DatabaseCreationCallable class.");
			d.addItem(
					"Changing the database loading policy : enabling transfer data from old database to new database.");
			d.addItem("Minimizing code duplication of tests.");
			d.addItem("Adding remove database possibility.");
			d.addItem("Use of savepoints.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 2, 7);
			d = new Description(1, 9, 7, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating Utils to 2.5.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 2, 4);
			d = new Description(1, 9, 6, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating Utils to 2.4.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 1, 7);
			d = new Description(1, 9, 5, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating Utils to 2.3.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2017, 0, 5);
			d = new Description(1, 9, 4, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating Utils to 2.2.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 11, 31);
			d = new Description(1, 9, 3, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating Utils to 2.1.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 11, 26);
			d = new Description(1, 9, 2, Version.Type.Stable, 0, c.getTime());
			d.addItem("Correcting a problem of data integrity check.");
			d.addItem("Updating Derby DB to 10.13.1.1.");
			d.addItem("Solving a problem of multithread execution into windows (SQLNonTransientConnectionException).");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 11, 24);
			d = new Description(1, 9, 1, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating Utils to version 2.0.1.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 11, 19);
			d = new Description(1, 9, 0, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating Utils to version 2.0.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 11, 6);
			d = new Description(1, 8, 1, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating Utils to version 1.9.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 10, 30);
			d = new Description(1, 8, 0, Version.Type.Stable, 0, c.getTime());
			d.addItem("Using ACID transactions instead of semaphores.");
			d.addItem("Adding HSQLDBConcurrencyControl class.");
			d.addItem("Adding memory refresh interval (see LoadToMemory annotation).");
			d.addItem("Adding the possibility to set composed fields by adding @Field to a class.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 9, 13);
			d = new Description(1, 7, 1, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating Utils to 1.8.0.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 8, 19);
			d = new Description(1, 7, 0, Version.Type.Stable, 0, c.getTime());
			d.addItem("Correcting bug into FieldAccessor (null pointer exception).");
			d.addItem("Adding enum support.");
			d.addItem("Adding Decentralized ID support.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 7, 29);
			d = new Description(1, 6, 4, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating Utils to 1.7.2.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 7, 23);
			d = new Description(1, 6, 3, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating Utils to 1.7.1.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 6, 4);
			d = new Description(1, 6, 2, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating Utils to 1.7.");
			d.addItem("Updating to Common-Net 3.5.");
			d.addItem("Updating to HSDQLDB 3.3.4.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 5, 10);
			d = new Description(1, 6, 1, Version.Type.Stable, 0, c.getTime());
			d.addItem("Correction a bug into the constructor of ByteTabConvertibleFieldAccessor.");
			d.addItem("Adding version tests.");
			d.addItem("Changing license to CECILL-C.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 2, 11);
			d = new Description(1, 6, 0, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating with Utils 1.6.");
			d.addItem("Adding database backup tools.");
			d.addItem("renaming alterRecord functions to updateRecord.");
			d.addItem("Adding functions Table.addRecord(record), Table.updateRecord(record).");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 2, 4);
			d = new Description(1, 5, 2, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating with Utils 1.5");
			d.addItem("Adding encryption keys encoding/decoding.");
			d.addItem("Correcting bugs with ByteTabConvertibleFieldAccessor class.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 2, 1);
			d = new Description(1, 5, 1, Version.Type.Stable, 0, c.getTime());
			d.addItem("Updating with Utils 1.4.");
			d.addItem("Adding AllTestsNG.xml file.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 1, 15);
			d = new Description(1, 5, 0, Version.Type.Stable, 0, c.getTime());
			d.addItem("Adding iterator functionality in class Table.");
			d.addItem("Adding ByteTabObjectConverter class.");
			d.addItem("Adding DefaultByteTabObjectConverter class.");
			d.addItem("Adding ByteTabConvertibleFieldAccessor class.");
			d.addItem("Adding function addByteTabObjectConverter in DatabaseWrapper class.");
			d.addItem(
					"Adding possibility to use Object tabs as an alternative of use of maps when reffering to fields.");
			d.addItem("Optimizing use of SQL database.");
			d.addItem("Linking with Utils 1.3.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 1, 14);
			d = new Description(1, 4, 1, Version.Type.Stable, 0, c.getTime());
			d.addItem("Adding some close statements corrections.");
			d.addItem("Adding some multi-thread optimisations.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 1, 8);
			d = new Description(1, 4, 0, Version.Type.Stable, 0, c.getTime());
			d.addItem(
					"One databse is associated to one package. Now, its is possible to load several database/packages into the same file.");
			d.addItem("OOD works now with HSQLDB or Apache Derby.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 1, 5);
			d = new Description(1, 3, 0, Version.Type.Stable, 0, c.getTime());
			d.addItem("Adding dependency with Utils and updating OOD consequently.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2016, 1, 1);
			d = new Description(1, 2, 0, Version.Type.Stable, 0, c.getTime());
			d.addItem("Correcting some bugs into the documentation.");
			d.addItem("Upgrading to HSQLDB 2.3.3 and Commons-Net 3.4.");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2013, 10, 18);
			d = new Description(1, 1, 0, Version.Type.Stable, 0, c.getTime());
			d.addItem("Improving function Table.alterRecords(...) and class AlterRecordFilter (see documentation).");
			VERSION.addDescription(d);

			c = Calendar.getInstance();
			c.set(2013, 3, 24);
			d = new Description(1, 0, 0, Version.Type.Stable, 0, c.getTime());
			d.addItem("Releasing Oriented Object Database as a stable version.");
			VERSION.addDescription(d);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
