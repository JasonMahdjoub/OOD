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
 * @version 1.0
 * @since OOD 2.5.0
 */
public class InFileEmbeddedAndroidH2DatabaseFactory extends DatabaseFactory<EmbeddedH2DatabaseWrapper> {
	/**
	 *
	 */
	private static final long serialVersionUID = -5549181783426731120L;

	private String databaseName;
	private String packageName;
	protected boolean alwaysDisconnectAfterOneTransaction=false;
	private int pageSizeBytes=1024;
	private int cacheSizeBytes=8192;
	private boolean useExternalCard=false;


	protected InFileEmbeddedAndroidH2DatabaseFactory() throws DatabaseException {
		super();
	}
	/**
	 * Constructor
	 *
	 * @param packageName the application package name
	 * @param databaseName
	 *            The database name
	 * @param useExternalCard if set true, use external storage card
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 */
	public InFileEmbeddedAndroidH2DatabaseFactory(String packageName, String databaseName, boolean useExternalCard) throws DatabaseException {
		this(null, packageName, databaseName, useExternalCard);
	}
	/**
	 * Constructor
	 *
	 * @param databaseConfigurations the database configurations
	 * @param packageName the application package name
	 * @param databaseName
	 *            The database name
	 * @param useExternalCard if set true, use external storage card
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 */
	public InFileEmbeddedAndroidH2DatabaseFactory(DatabaseConfigurations databaseConfigurations, String packageName, String databaseName, boolean useExternalCard) throws DatabaseException {
		this(databaseConfigurations, packageName, databaseName, useExternalCard, false);
	}

	/**
	 * Constructor
	 *
	 * @param packageName the application package name
	 * @param databaseName
	 *            The database name
	 * @param useExternalCard if set true, use external storage card
	 * @param alwaysDisconnectAfterOneTransaction true if the database must always be connected and detected during one transaction
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 */
	public InFileEmbeddedAndroidH2DatabaseFactory(String packageName, String databaseName, boolean useExternalCard, boolean alwaysDisconnectAfterOneTransaction) throws DatabaseException {
		this(null, packageName, databaseName, useExternalCard, alwaysDisconnectAfterOneTransaction);
	}

	/**
	 * Constructor
	 *
	 * @param databaseConfigurations the database configurations
	 * @param packageName the application package name
	 * @param databaseName
	 *            The database name
	 * @param useExternalCard if set true, use external storage card
	 * @param alwaysDisconnectAfterOneTransaction true if the database must always be connected and detected during one transaction
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 */
	public InFileEmbeddedAndroidH2DatabaseFactory(DatabaseConfigurations databaseConfigurations, String packageName, String databaseName, boolean useExternalCard, boolean alwaysDisconnectAfterOneTransaction) throws DatabaseException {
		super(databaseConfigurations);
		setDatabasePath(packageName, databaseName, useExternalCard);
		this.alwaysDisconnectAfterOneTransaction = alwaysDisconnectAfterOneTransaction;
	}
	/**
	 * Constructor
	 *
	 * @param packageName the application package name
	 * @param databaseName
	 *            The database name
	 * @param useExternalCard if set true, use external storage card
	 * @param alwaysDisconnectAfterOneTransaction true if the database must always be connected and detected during one transaction
	 * @param fileLock true if the database file must be locked when opened
	 * @param pageSizeBytes the page size of the database in bytes
	 * @param cacheSizeBytes the cache size in bytes
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 */
	public InFileEmbeddedAndroidH2DatabaseFactory(String packageName, String databaseName, boolean useExternalCard, boolean alwaysDisconnectAfterOneTransaction, boolean fileLock, int pageSizeBytes, int cacheSizeBytes) throws DatabaseException {
		this(null, packageName, databaseName, useExternalCard, alwaysDisconnectAfterOneTransaction, fileLock, pageSizeBytes, cacheSizeBytes);
	}
	/**
	 * Constructor
	 *
	 * @param databaseConfigurations the database configurations
	 * @param packageName the application package name
	 * @param databaseName
	 *            The database name
	 * @param useExternalCard if set true, use external storage card
	 * @param alwaysDisconnectAfterOneTransaction true if the database must always be connected and detected during one transaction
	 * @param fileLock true if the database file must be locked when opened
	 * @param pageSizeBytes the page size of the database in bytes
	 * @param cacheSizeBytes the cache size in bytes
	 * @throws NullPointerException
	 *             if parameters are null pointers.
	 * @throws IllegalArgumentException
	 *             If the given file is a directory.
	 */
	public InFileEmbeddedAndroidH2DatabaseFactory(DatabaseConfigurations databaseConfigurations, String packageName, String databaseName, boolean useExternalCard, boolean alwaysDisconnectAfterOneTransaction, boolean fileLock, int pageSizeBytes
			, int cacheSizeBytes) throws DatabaseException {
		super(databaseConfigurations);
		setDatabasePath(packageName, databaseName, useExternalCard);
		this.alwaysDisconnectAfterOneTransaction = alwaysDisconnectAfterOneTransaction;
		this.pageSizeBytes=pageSizeBytes;
		this.cacheSizeBytes=cacheSizeBytes;
	}

	private File getDirectory()
	{
		return new File((useExternalCard?"/sdcard/":"/data/data/")+packageName+"/"+databaseName);
	}

	@Override
	protected EmbeddedH2DatabaseWrapper newWrapperInstance(DatabaseLifeCycles databaseLifeCycles, boolean createDatabasesIfNecessaryAndCheckIt) throws DatabaseException {
		return new EmbeddedH2DatabaseWrapper(getDirectory(), databaseConfigurations, databaseLifeCycles,
				signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup==null?null:signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
				encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup==null?null:encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
				protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages==null?null:protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages.getEncryptionProfileProviderSingleton(),
				getSecureRandom(), createDatabasesIfNecessaryAndCheckIt, alwaysDisconnectAfterOneTransaction, pageSizeBytes, cacheSizeBytes);
	}

	public boolean isAlwaysDisconnectAfterOneTransaction() {
		return alwaysDisconnectAfterOneTransaction;
	}

	public void setAlwaysDisconnectAfterOneTransaction(boolean alwaysDisconnectAfterOneTransaction) {
		this.alwaysDisconnectAfterOneTransaction = alwaysDisconnectAfterOneTransaction;
	}

	public void setDatabasePath(String packageName, String databaseName, boolean useExternalCard)
	{
		if (databaseName==null)
			throw new NullPointerException();
		if (packageName==null)
			throw new NullPointerException();
		this.packageName=packageName;
		this.databaseName=databaseName;
		this.useExternalCard=useExternalCard;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getPackageName() {
		return packageName;
	}

	public boolean isUseExternalCard() {
		return useExternalCard;
	}

	public static long getSerialVersionUID() {
		return serialVersionUID;
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
		EmbeddedH2DatabaseWrapper.deleteDatabasesFiles(getDirectory());
	}

}
