package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.4.0
 */
public class InMemoryEmbeddedDerbyDatabaseFactory extends DatabaseFactory<EmbeddedDerbyWrapper> {
	/**
	 *
	 */
	private static final long serialVersionUID = -5549181783426731120L;

	private String databaseName=null;

	public InMemoryEmbeddedDerbyDatabaseFactory() {

	}

	public InMemoryEmbeddedDerbyDatabaseFactory(String databaseName) {
		this.databaseName = databaseName;
	}

	public InMemoryEmbeddedDerbyDatabaseFactory(String databaseName, HSQLDBConcurrencyControl concurrencyControl) {
		this.databaseName = databaseName;
		if (concurrencyControl==null)
			throw new NullPointerException();
	}

	@Override
	protected EmbeddedDerbyWrapper newWrapperInstance() throws DatabaseException {
		return new EmbeddedDerbyWrapper(true, databaseName);
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

