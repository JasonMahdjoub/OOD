
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
package com.distrimind.ood.database.tests;

import com.distrimind.ood.database.*;
import com.distrimind.ood.database.database.Table1.Record;
import com.distrimind.ood.database.database.Table3;
import com.distrimind.ood.database.exceptions.ConstraintsNotRespectedDatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.FileTools;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.HashMap;

/**
 * 
 * @author Jason Mahdjoub
 * @version 2.0
 * @since OOD 1.0
 */

public class DerbyTestDatabase extends TestDatabase {
	public DerbyTestDatabase() throws DatabaseException, NoSuchAlgorithmException, NoSuchProviderException {
		super();
	}

	private static final File data_directory = new File("databasetestDerby");
	private static final File data_directoryb = new File("databasetestDerbyb");
	private static final File database_backup_directory = new File("databasebackupDerby");

	@Override
	public DatabaseWrapper getDatabaseWrapperInstanceA() throws IllegalArgumentException, DatabaseException {
		return new InFileEmbeddedH2DatabaseFactory(data_directory).getDatabaseWrapperSingleton();
	}

	@Override
	public DatabaseWrapper getDatabaseWrapperInstanceB() throws IllegalArgumentException, DatabaseException {
		return new InFileEmbeddedH2DatabaseFactory(data_directoryb).getDatabaseWrapperSingleton();
	}

	@Override
	public void deleteDatabaseFilesA() throws IllegalArgumentException {
		EmbeddedDerbyWrapper.deleteDatabaseFiles(data_directory);

	}

	@Override
	public void deleteDatabaseFilesB() throws IllegalArgumentException {
		EmbeddedDerbyWrapper.deleteDatabaseFiles(data_directoryb);
	}

	@AfterClass
	public void unloadDatabase()  {
		super.unloadDatabase();
		EmbeddedDerbyWrapper.deleteDatabaseFiles(data_directory);
		EmbeddedDerbyWrapper.deleteDatabaseFiles(data_directoryb);
		FileTools.deleteDirectory(database_backup_directory);
	}

	@Override
	public File getDatabaseBackupFileName() {
		return database_backup_directory;
	}


	@Override
	public boolean isTestEnabled(int _testNumber) {
		return _testNumber != 12 && _testNumber != 13;
	}

	@Override
	public int getMultiTestsNumber() {
		return 200;
	}

	@Override
	public int getThreadTestsNumber() {
		return 200;
	}

	@Override
	public boolean isMultiConcurrentDatabase() {
		return true;
	}

}
