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
public class InFileEmbeddedHSQLDatabaseFactory extends DatabaseFactory<EmbeddedHSQLDBWrapper> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5549181783426731120L;

	private File directory;
	private HSQLDBConcurrencyControl concurrencyControl=HSQLDBConcurrencyControl.DEFAULT;
	private int cacheRows=50000;
	private int cacheSizeBytes =10000*1024;
	private int resultMaxMemoryRows=0;
	private int cacheFreeCount=512;
	private boolean lockFile=true;
	private boolean alwaysDisconnectAfterOneTransaction=false;

	protected InFileEmbeddedHSQLDatabaseFactory() throws DatabaseException {
		super();
	}
	/**
	 * Constructor
	 *
	 * @param databaseDirectory
	 *            The directory which contains the database. If this directory does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 *
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 *
	 */
	public InFileEmbeddedHSQLDatabaseFactory(File databaseDirectory) throws DatabaseException {
		this(null, databaseDirectory);
	}
	/**
	 * Constructor
	 *
	 * @param databaseConfigurations the database configurations
	 * @param databaseDirectory
	 *            The directory which contains the database. If this directory does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 *
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 *
	 */
	public InFileEmbeddedHSQLDatabaseFactory(DatabaseConfigurations databaseConfigurations, File databaseDirectory) throws DatabaseException {
		this(databaseConfigurations, databaseDirectory, false);
	}
	/**
	 * Constructor
	 *
	 * @param databaseDirectory
	 *            The directory which contains the database. If this directory does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 * @param alwaysDisconnectAfterOneTransaction true if the database must always be connected and detected during one transaction
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 *
	 */
	public InFileEmbeddedHSQLDatabaseFactory(File databaseDirectory, boolean alwaysDisconnectAfterOneTransaction) throws DatabaseException {
		this(null, databaseDirectory, alwaysDisconnectAfterOneTransaction);
	}
	/**
	 * Constructor
	 *
	 * @param databaseConfigurations the database configurations
	 * @param databaseDirectory
	 *            The directory which contains the database. If this directory does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 * @param alwaysDisconnectAfterOneTransaction true if the database must always be connected and detected during one transaction
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 *
	 */
	public InFileEmbeddedHSQLDatabaseFactory(DatabaseConfigurations databaseConfigurations, File databaseDirectory, boolean alwaysDisconnectAfterOneTransaction) throws DatabaseException {
		super(databaseConfigurations);
		setDirectory(databaseDirectory);
		this.alwaysDisconnectAfterOneTransaction = alwaysDisconnectAfterOneTransaction;
	}
	/**
	 * Constructor
	 *
	 * @param databaseConfigurations the database configurations
	 * @param databaseDirectory
	 *            The directory which contains the database. If this directory does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 * @param alwaysDisconnectAfterOneTransaction true if the database must always be connected and detected during one transaction
	 * @param concurrencyControl the concurrency mode
	 * @param _cache_rows
	 *            indicates the maximum number of rows of cached tables that are
	 *            held in memory. The value can range between 100- 4 billion.
	 *            Default value is 100. Table loaded into memory are not concerned.
	 * @param _cache_size
	 *            Indicates the total size (in kilobytes) of rows in the memory
	 *            cache used with cached tables. The value can range between 100 KB
	 *            - 4 GB. The default is 10,000, representing 10,000 kilobytes.
	 * @param _result_max_memory_rows
	 *            This property can be set to specify how many rows of each results
	 *            or temporary table are stored in memory before the table is
	 *            written to disk. The default is zero and means data is always
	 *            stored in memory. If this setting is used, it should be set above
	 *            1000.
	 * @param _cache_free_count
	 *            The default indicates 512 unused spaces are kept for later use.
	 *            The value can range between 0 - 8096. When rows are deleted, the
	 *            space is recovered and kept for reuse for new rows. If too many
	 *            rows are deleted, the smaller recovered spaces are lost and the
	 *            largest ones are retained for later use. Normally there is no need
	 *            to set this property.
	 * @param lockFile true if the database's file must be locked to avoid concurrent access
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 *
	 */
	public InFileEmbeddedHSQLDatabaseFactory(DatabaseConfigurations databaseConfigurations, File databaseDirectory, boolean alwaysDisconnectAfterOneTransaction, HSQLDBConcurrencyControl concurrencyControl, int _cache_rows,
											 int _cache_size, int _result_max_memory_rows, int _cache_free_count, boolean lockFile) throws DatabaseException {
		super(databaseConfigurations);
		setDirectory(databaseDirectory);
		setConcurrencyControl(concurrencyControl);
		cacheRows = _cache_rows;
		cacheSizeBytes = _cache_size;
		resultMaxMemoryRows = _result_max_memory_rows;
		cacheFreeCount = _cache_free_count;
		this.alwaysDisconnectAfterOneTransaction = alwaysDisconnectAfterOneTransaction;
		this.lockFile=lockFile;
	}
	/**
	 * Constructor
	 *
	 * @param databaseDirectory
	 *            The directory which contains the database. If this directory does not
	 *            exists, it will be automatically created with the correspondent
	 *            database.
	 * @param alwaysDisconnectAfterOneTransaction true if the database must always be connected and detected during one transaction
	 * @param concurrencyControl the concurrency mode
	 * @param _cache_rows
	 *            indicates the maximum number of rows of cached tables that are
	 *            held in memory. The value can range between 100- 4 billion.
	 *            Default value is 100. Table loaded into memory are not concerned.
	 * @param _cache_size
	 *            Indicates the total size (in kilobytes) of rows in the memory
	 *            cache used with cached tables. The value can range between 100 KB
	 *            - 4 GB. The default is 10,000, representing 10,000 kilobytes.
	 * @param _result_max_memory_rows
	 *            This property can be set to specify how many rows of each results
	 *            or temporary table are stored in memory before the table is
	 *            written to disk. The default is zero and means data is always
	 *            stored in memory. If this setting is used, it should be set above
	 *            1000.
	 * @param _cache_free_count
	 *            The default indicates 512 unused spaces are kept for later use.
	 *            The value can range between 0 - 8096. When rows are deleted, the
	 *            space is recovered and kept for reuse for new rows. If too many
	 *            rows are deleted, the smaller recovered spaces are lost and the
	 *            largest ones are retained for later use. Normally there is no need
	 *            to set this property.
	 * @param lockFile true if the database's file must be locked to avoid concurrent access
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 *
	 */
	public InFileEmbeddedHSQLDatabaseFactory(File databaseDirectory, boolean alwaysDisconnectAfterOneTransaction, HSQLDBConcurrencyControl concurrencyControl, int _cache_rows,
											 int _cache_size, int _result_max_memory_rows, int _cache_free_count, boolean lockFile) throws DatabaseException {
		this(null, databaseDirectory, alwaysDisconnectAfterOneTransaction, concurrencyControl, _cache_rows, _cache_size, _result_max_memory_rows, _cache_free_count, lockFile);
	}

	@Override
	protected EmbeddedHSQLDBWrapper newWrapperInstance() throws DatabaseException {
		return new EmbeddedHSQLDBWrapper(directory, alwaysDisconnectAfterOneTransaction, concurrencyControl, cacheRows, cacheSizeBytes,
					resultMaxMemoryRows, cacheFreeCount, lockFile);
	}

	public File getDirectory() {
		return directory;
	}

	public void setDirectory(File _file_name) {
		if (_file_name == null)
			throw new NullPointerException("The parameter _file_name is a null pointer !");
		if (_file_name.exists() && !_file_name.isDirectory())
			throw new IllegalArgumentException("The given file name is not a directory !");
		this.directory = _file_name;
	}

	public HSQLDBConcurrencyControl getConcurrencyControl() {
		return concurrencyControl;
	}

	public void setConcurrencyControl(HSQLDBConcurrencyControl concurrencyControl) {
		if (concurrencyControl==null)
			throw new NullPointerException();

		this.concurrencyControl = concurrencyControl;
	}

	public int getCacheRows() {
		return cacheRows;
	}

	public void setCacheRows(int cacheRows) {
		this.cacheRows = cacheRows;
	}

	public int getCacheSizeBytes() {
		return cacheSizeBytes;
	}

	public void setCacheSizeBytes(int cacheSizeBytes) {
		this.cacheSizeBytes = cacheSizeBytes;
	}

	public int getResultMaxMemoryRows() {
		return resultMaxMemoryRows;
	}

	public void setResultMaxMemoryRows(int resultMaxMemoryRows) {
		this.resultMaxMemoryRows = resultMaxMemoryRows;
	}

	public int getCacheFreeCount() {
		return cacheFreeCount;
	}

	public void setCacheFreeCount(int cacheFreeCount) {
		this.cacheFreeCount = cacheFreeCount;
	}

	public boolean isLockFile() {
		return lockFile;
	}

	public void setLockFile(boolean lockFile) {
		this.lockFile = lockFile;
	}

	public boolean isAlwaysDisconnectAfterOneTransaction() {
		return alwaysDisconnectAfterOneTransaction;
	}

	public void setAlwaysDisconnectAfterOneTransaction(boolean alwaysDisconnectAfterOneTransaction) {
		this.alwaysDisconnectAfterOneTransaction = alwaysDisconnectAfterOneTransaction;
	}

	@Override
	public void deleteDatabase()  {
		EmbeddedH2DatabaseWrapper.deleteDatabaseFiles(directory);
	}
}
