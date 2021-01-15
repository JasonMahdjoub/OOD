package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.4.0
 */
class InMemoryEmbeddedDerbyDatabaseFactory extends DatabaseFactory<EmbeddedDerbyWrapper> {
	/**
	 *
	 */
	private static final long serialVersionUID = -5549181783426731120L;

	private String databaseName=null;

	public InMemoryEmbeddedDerbyDatabaseFactory() throws DatabaseException {
		super();
	}
	public InMemoryEmbeddedDerbyDatabaseFactory(String databaseName) throws DatabaseException {
		this(null, databaseName);
	}
	public InMemoryEmbeddedDerbyDatabaseFactory(DatabaseConfigurations databaseConfigurations, String databaseName) throws DatabaseException {
		super(databaseConfigurations);
		this.databaseName = databaseName;
	}
	public InMemoryEmbeddedDerbyDatabaseFactory(String databaseName, HSQLDBConcurrencyControl concurrencyControl) throws DatabaseException {
		this(null, databaseName, concurrencyControl);
	}
	public InMemoryEmbeddedDerbyDatabaseFactory(DatabaseConfigurations databaseConfigurations, String databaseName, HSQLDBConcurrencyControl concurrencyControl) throws DatabaseException {
		super(databaseConfigurations);
		this.databaseName = databaseName;
		if (concurrencyControl==null)
			throw new NullPointerException();
	}

	@Override
	protected EmbeddedDerbyWrapper newWrapperInstance(DatabaseLifeCycles databaseLifeCycles, boolean createDatabasesIfNecessaryAndCheckIt) throws DatabaseException {
		return new EmbeddedDerbyWrapper(databaseName, true, databaseConfigurations, databaseLifeCycles,
				encryptionProfileProviderFactoryForCentralDatabaseBackup==null?null:encryptionProfileProviderFactoryForCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
				protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages==null?null:protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages.getEncryptionProfileProviderSingleton(),
				getSecureRandom(), createDatabasesIfNecessaryAndCheckIt);
	}

	public static long getSerialVersionUID() {
		return serialVersionUID;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	@Override
	public void deleteDatabase()  {
		throw new UnsupportedOperationException();
	}


}

