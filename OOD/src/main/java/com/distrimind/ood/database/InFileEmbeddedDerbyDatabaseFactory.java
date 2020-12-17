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
package com.distrimind.ood.database;

import java.io.File;

import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * 
 * 
 * @author Jason Mahdjoub
 * @version 1.2
 * @since OOD 2.0.0
 */
public class InFileEmbeddedDerbyDatabaseFactory extends DatabaseFactory<EmbeddedDerbyWrapper> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5754965997489003893L;
	private File directory;
	private boolean alwaysDisconnectAfterOnTransaction=false;
	protected InFileEmbeddedDerbyDatabaseFactory() throws DatabaseException {
		super();
	}
	/**
	 * Constructor
	 * @param directory
	 *            the database directory
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given _directory is not a directory.
	 * @throws IllegalArgumentException if arguments are incorrect
	 */
	public InFileEmbeddedDerbyDatabaseFactory(File directory) throws DatabaseException {
		this(null, directory);
	}

	/**
	 * Constructor
	 * @param databaseConfigurations the database configurations
	 * @param directory
	 *            the database directory
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given _directory is not a directory.
	 * @throws IllegalArgumentException if arguments are incorrect
	 */
	public InFileEmbeddedDerbyDatabaseFactory(DatabaseConfigurations databaseConfigurations, File directory) throws DatabaseException {
		this(databaseConfigurations, directory, false);
	}
	/**
	 * Constructor
	 * @param directory
	 *            the database directory
	 * @param alwaysDisconnectAfterOnTransaction true if the database must always be connected and detected during one transaction
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given _directory is not a directory.
	 * @throws IllegalArgumentException if arguments are incorrect
	 */
	public InFileEmbeddedDerbyDatabaseFactory(File directory, boolean alwaysDisconnectAfterOnTransaction) throws DatabaseException {
		this(null, directory, alwaysDisconnectAfterOnTransaction);
	}
	/**
	 * Constructor
	 * @param databaseConfigurations the database configurations
	 * @param directory
	 *            the database directory
	 * @param alwaysDisconnectAfterOnTransaction true if the database must always be connected and detected during one transaction
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given _directory is not a directory.
	 * @throws IllegalArgumentException if arguments are incorrect
	 */
	public InFileEmbeddedDerbyDatabaseFactory(DatabaseConfigurations databaseConfigurations, File directory, boolean alwaysDisconnectAfterOnTransaction) throws DatabaseException {
		super(databaseConfigurations);
		setDirectory(directory);
		this.alwaysDisconnectAfterOnTransaction = alwaysDisconnectAfterOnTransaction;
	}

	@Override
	protected EmbeddedDerbyWrapper newWrapperInstance(DatabaseLifeCycles databaseLifeCycles, boolean createDatabasesIfNecessaryAndCheckIt) throws DatabaseException {
		return new EmbeddedDerbyWrapper(directory, databaseConfigurations, databaseLifeCycles,
				encryptionProfileProviderFactoryForCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
				protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages.getEncryptionProfileProviderSingleton(),
				getSecureRandom(), createDatabasesIfNecessaryAndCheckIt, alwaysDisconnectAfterOnTransaction);
	}

	public File getDirectory() {
		return directory;
	}

	public void setDirectory(File directory) {
		if (directory == null)
			throw new NullPointerException("The parameter _file_name is a null pointer !");
		if (directory.exists() && !directory.isDirectory())
			throw new IllegalArgumentException("The given file name must be directory !");
		this.directory = directory;
	}

	public boolean isAlwaysDisconnectAfterOnTransaction() {
		return alwaysDisconnectAfterOnTransaction;
	}

	public void setAlwaysDisconnectAfterOnTransaction(boolean alwaysDisconnectAfterOnTransaction) {
		this.alwaysDisconnectAfterOnTransaction = alwaysDisconnectAfterOnTransaction;
	}

	@Override
	public void deleteDatabase()  {
		EmbeddedDerbyWrapper.deleteDatabaseFiles(directory);
	}

}
