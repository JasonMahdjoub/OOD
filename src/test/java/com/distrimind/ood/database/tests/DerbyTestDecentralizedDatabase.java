/*
Copyright or © or Corp. Jason Mahdjoub (01/04/2013)

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

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

import com.distrimind.ood.database.*;
import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * 
 * @author Jason Mahdjoub
 * @version 2.0
 * @since OOD 1.0
 */
public class DerbyTestDecentralizedDatabase extends TestDecentralizedDatabase {
	final File centralDatabaseFileName = new File("centralDatabase");
	private final File data_directory1 = new File("databasetestDerby1");
	private final File data_directory2 = new File("databasetestDerby2");
	private final File data_directory3 = new File("databasetestDerby3");
	private final File data_directory4 = new File("databasetestDerby4");

	public DerbyTestDecentralizedDatabase() throws NoSuchProviderException, NoSuchAlgorithmException, IOException, DatabaseException {
		super();
	}

	@Override
	public DatabaseFactory<?> getDatabaseFactoryInstanceForCentralDatabaseBackupReceiver() throws IllegalArgumentException, DatabaseException {
		return null;
		//return new InFileEmbeddedDerbyDatabaseFactory(centralDatabaseFileName);
	}

	@Override
	public DatabaseFactory<?> getDatabaseFactoryInstance1() throws IllegalArgumentException, DatabaseException {
		return null;
		//return new InFileEmbeddedDerbyDatabaseFactory(data_directory1);
	}

	@Override
	public DatabaseFactory<?> getDatabaseFactoryInstance2() throws IllegalArgumentException, DatabaseException {
		return null;
		//return new InFileEmbeddedDerbyDatabaseFactory(data_directory2);
	}

	@Override
	public DatabaseFactory<?> getDatabaseFactoryInstance3() throws IllegalArgumentException, DatabaseException {
		return null;
		//return new InFileEmbeddedDerbyDatabaseFactory(data_directory3);
	}

	@Override
	public DatabaseFactory<?> getDatabaseFactoryInstance4() throws IllegalArgumentException, DatabaseException {
		return null;
		//return new InFileEmbeddedDerbyDatabaseFactory(data_directory4);
	}

	@Override
	public void removeDatabaseFiles1() {

		//EmbeddedDerbyWrapper.deleteDatabaseFiles(data_directory1);

	}

	@Override
	public void removeDatabaseFiles2() {
		//EmbeddedDerbyWrapper.deleteDatabaseFiles(data_directory2);

	}

	@Override
	public void removeDatabaseFiles3() {
		//EmbeddedDerbyWrapper.deleteDatabaseFiles(data_directory3);
	}

	@Override
	public void removeDatabaseFiles4() {
		//EmbeddedDerbyWrapper.deleteDatabaseFiles(data_directory4);
	}

	@Override
	public void removeCentralDatabaseFiles() {
		//EmbeddedDerbyWrapper.deleteDatabaseFiles(centralDatabaseFileName);

	}

}
