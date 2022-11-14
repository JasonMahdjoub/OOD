package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.concurrent.ScheduledPoolExecutor;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.4.0
 */
public class InMemoryEmbeddedHSQLDatabaseFactory extends DatabaseFactory<EmbeddedHSQLDBWrapper> {
	/**
	 *
	 */
	private static final long serialVersionUID = -5549181783426731120L;

	private String databaseName=null;
	private HSQLDBConcurrencyControl concurrencyControl=HSQLDBConcurrencyControl.DEFAULT;

	public InMemoryEmbeddedHSQLDatabaseFactory() throws DatabaseException {
		super();
	}
	public InMemoryEmbeddedHSQLDatabaseFactory(String databaseName) throws DatabaseException {
		this(null, databaseName);
	}
	public InMemoryEmbeddedHSQLDatabaseFactory(DatabaseConfigurations databaseConfigurations, String databaseName) throws DatabaseException {
		super(databaseConfigurations);
		this.databaseName = databaseName;
	}
	public InMemoryEmbeddedHSQLDatabaseFactory(String databaseName, HSQLDBConcurrencyControl concurrencyControl) throws DatabaseException {
		this(null,databaseName, concurrencyControl);
	}
	public InMemoryEmbeddedHSQLDatabaseFactory(DatabaseConfigurations databaseConfigurations, String databaseName, HSQLDBConcurrencyControl concurrencyControl) throws DatabaseException {
		super(databaseConfigurations);
		this.databaseName = databaseName;
		if (concurrencyControl==null)
			throw new NullPointerException();
		this.concurrencyControl = concurrencyControl;
	}

	@Override
	protected EmbeddedHSQLDBWrapper newWrapperInstance(ScheduledPoolExecutor defaultPoolExecutor, DatabaseLifeCycles databaseLifeCycles, boolean createDatabasesIfNecessaryAndCheckIt) throws DatabaseException {
		return new EmbeddedHSQLDBWrapper(defaultPoolExecutor, databaseName, true, databaseConfigurations, databaseLifeCycles,
				signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup==null?null:signatureProfileFactoryForAuthenticatedMessagesDestinedToCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
				encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup==null?null:encryptionProfileFactoryForE2EDataDestinedCentralDatabaseBackup.getEncryptionProfileProviderSingleton(),
				protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages==null?null:protectedEncryptionProfileFactoryProviderForAuthenticatedP2PMessages.getEncryptionProfileProviderSingleton(),
				getSecureRandom(), createDatabasesIfNecessaryAndCheckIt, concurrencyControl);
	}

	@Override
	public void deleteDatabase()  {
		throw new UnsupportedOperationException();
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

	public HSQLDBConcurrencyControl getConcurrencyControl() {
		return concurrencyControl;
	}

	public void setConcurrencyControl(HSQLDBConcurrencyControl concurrencyControl) {
		if (concurrencyControl==null)
			throw new NullPointerException();

		this.concurrencyControl = concurrencyControl;
	}
}

