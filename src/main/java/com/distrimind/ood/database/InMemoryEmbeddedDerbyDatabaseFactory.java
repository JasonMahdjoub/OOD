package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.4.0
 */
class InMemoryEmbeddedDerbyDatabaseFactory extends DatabaseFactory<EmbeddedDerbyWrapper> {

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
	protected EmbeddedDerbyWrapper newWrapperInstanceImpl() throws DatabaseException {
		return new EmbeddedDerbyWrapper(getDefaultPoolExecutor(), getContext(), databaseName, true, databaseConfigurations, getDatabaseLifeCycles(),
				signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup==null?null:signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
				encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup==null?null:encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
				protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages==null?null:protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages.getEncryptionProfileProviderSingleton(),
				getSecureRandom(), isCreateDatabasesIfNecessaryAndCheckIt());
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

