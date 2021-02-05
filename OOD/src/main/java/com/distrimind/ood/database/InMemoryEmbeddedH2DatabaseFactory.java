package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.4.0
 */
public class InMemoryEmbeddedH2DatabaseFactory extends DatabaseFactory<EmbeddedH2DatabaseWrapper> {
	/**
	 *
	 */
	private static final long serialVersionUID = -5549181783426731120L;

	private String databaseName=null;

	public InMemoryEmbeddedH2DatabaseFactory() throws DatabaseException {
		super();
	}
	public InMemoryEmbeddedH2DatabaseFactory(String databaseName) throws DatabaseException {
		this(null, databaseName);
	}
	public InMemoryEmbeddedH2DatabaseFactory(DatabaseConfigurations databaseConfigurations, String databaseName) throws DatabaseException {
		super(databaseConfigurations);
		this.databaseName = databaseName;
	}

	@Override
	protected EmbeddedH2DatabaseWrapper newWrapperInstance(DatabaseLifeCycles databaseLifeCycles, boolean createDatabasesIfNecessaryAndCheckIt) throws DatabaseException {
		return new EmbeddedH2DatabaseWrapper(databaseName, true, databaseConfigurations, databaseLifeCycles,
				signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup==null?null:signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
				encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup==null?null:encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
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

