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

import com.distrimind.ood.database.exceptions.DatabaseException;

import java.io.File;

/**
 * 
 * 
 * @author Jason Mahdjoub
 * @version 1.3
 * @since OOD 2.0.0
 */
public class InFileEmbeddedH2DatabaseFactory extends DatabaseFactory<EmbeddedH2DatabaseWrapper> {
	/**
	 *
	 */
	private static final long serialVersionUID = -5549181783426731120L;

	private File directory;
	protected boolean alwaysDisconnectAfterOneTransaction=false;
	private boolean fileLock=true;
	private int pageSizeBytes=2048;
	private int cacheSizeBytes=10000*1024;


	protected InFileEmbeddedH2DatabaseFactory() throws DatabaseException {
		super();
	}
	/**
	 * Constructor
	 *
	 * @param databaseConfigurations the database configurations
	 * @param _directory_name
	 *            The directory which contains the database. If this directory does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 */
	public InFileEmbeddedH2DatabaseFactory(DatabaseConfigurations databaseConfigurations, File _directory_name) throws DatabaseException {
		this(databaseConfigurations, _directory_name, false);
	}
	/**
	 * Constructor
	 *
	 *
	 * @param _directory_name
	 *            The directory which contains the database. If this directory does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 */
	public InFileEmbeddedH2DatabaseFactory(File _directory_name) throws DatabaseException {
		this(null, _directory_name);
	}

	/**
	 * Constructor
	 *
	 * @param databaseConfigurations the database configurations
	 * @param _directory_name
	 *            The directory which contains the database. If this directory does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 * @param alwaysDisconnectAfterOneTransaction true if the database must always be connected and detected during one transaction
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 */
	public InFileEmbeddedH2DatabaseFactory(DatabaseConfigurations databaseConfigurations, File _directory_name, boolean alwaysDisconnectAfterOneTransaction) throws DatabaseException {
		super(databaseConfigurations);
		setDirectory(_directory_name);
		this.alwaysDisconnectAfterOneTransaction = alwaysDisconnectAfterOneTransaction;
	}
	/**
	 * Constructor
	 *
	 * @param _directory_name
	 *            The directory which contains the database. If this directory does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 * @param alwaysDisconnectAfterOneTransaction true if the database must always be connected and detected during one transaction
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 */
	public InFileEmbeddedH2DatabaseFactory(File _directory_name, boolean alwaysDisconnectAfterOneTransaction) throws DatabaseException {
		this(null, _directory_name, alwaysDisconnectAfterOneTransaction);
	}

	/**
	 * Constructor
	 *
	 * @param databaseConfigurations the database configurations
	 * @param _directory_name
	 *            The directory which contains the database. If this directory does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 * @param alwaysDisconnectAfterOneTransaction true if the database must always be connected and detected during one transaction
	 * @param fileLock true if the database file must be locked when opened
	 * @param pageSizeBytes the page size of the database in bytes
	 * @param cacheSizeBytes the cache size in bytes
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 */
	public InFileEmbeddedH2DatabaseFactory(DatabaseConfigurations databaseConfigurations, File _directory_name, boolean alwaysDisconnectAfterOneTransaction, boolean fileLock, int pageSizeBytes
			, int cacheSizeBytes) throws DatabaseException {
		super(databaseConfigurations);
		setDirectory(_directory_name);
		this.alwaysDisconnectAfterOneTransaction = alwaysDisconnectAfterOneTransaction;
		this.fileLock=fileLock;
		this.pageSizeBytes=pageSizeBytes;
		this.cacheSizeBytes=cacheSizeBytes;
	}
	/**
	 * Constructor
	 *
	 * @param _directory_name
	 *            The directory which contains the database. If this directory does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 * @param alwaysDisconnectAfterOneTransaction true if the database must always be connected and detected during one transaction
	 * @param fileLock true if the database file must be locked when opened
	 * @param pageSizeBytes the page size of the database in bytes
	 * @param cacheSizeBytes the cache size in bytes
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 */
	public InFileEmbeddedH2DatabaseFactory(File _directory_name, boolean alwaysDisconnectAfterOneTransaction, boolean fileLock, int pageSizeBytes
			, int cacheSizeBytes) throws DatabaseException {
		this(null, _directory_name, alwaysDisconnectAfterOneTransaction, fileLock, pageSizeBytes, cacheSizeBytes);
	}

	@Override
	protected EmbeddedH2DatabaseWrapper newWrapperInstance(DatabaseLifeCycles databaseLifeCycles, boolean createDatabasesIfNecessaryAndCheckIt) throws DatabaseException {
		return new EmbeddedH2DatabaseWrapper(directory, databaseConfigurations, databaseLifeCycles,
				encryptionProfileProviderFactoryForCentralDatabaseBackup==null?null:encryptionProfileProviderFactoryForCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
				protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages==null?null:protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages.getEncryptionProfileProviderSingleton(),
				getSecureRandom(), createDatabasesIfNecessaryAndCheckIt, alwaysDisconnectAfterOneTransaction, fileLock, pageSizeBytes, cacheSizeBytes);
	}

	public boolean isAlwaysDisconnectAfterOneTransaction() {
		return alwaysDisconnectAfterOneTransaction;
	}

	public void setAlwaysDisconnectAfterOneTransaction(boolean alwaysDisconnectAfterOneTransaction) {
		this.alwaysDisconnectAfterOneTransaction = alwaysDisconnectAfterOneTransaction;
	}

	public File getDirectory() {
		return directory;
	}

	public void setDirectory(File _file_name) {
		if (_file_name == null)
			throw new NullPointerException("The parameter _file_name is a null pointer !");
		if (_file_name.exists() && !_file_name.isDirectory())
			throw new IllegalArgumentException("The given file name is not a directory !");
		directory = _file_name;
	}

	public static long getSerialVersionUID() {
		return serialVersionUID;
	}

	public boolean isFileLock() {
		return fileLock;
	}

	public void setFileLock(boolean fileLock) {
		this.fileLock = fileLock;
	}

	public int getPageSizeBytes() {
		return pageSizeBytes;
	}

	public void setPageSizeBytes(int pageSizeBytes) {
		this.pageSizeBytes = pageSizeBytes;
	}

	public int getCacheSizeBytes() {
		return cacheSizeBytes;
	}

	public void setCacheSizeBytes(int cacheSizeBytes) {
		this.cacheSizeBytes = cacheSizeBytes;
	}

	@Override
	public void deleteDatabase()  {
		EmbeddedH2DatabaseWrapper.deleteDatabaseFiles(directory);
	}

}
