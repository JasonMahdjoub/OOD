package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.4.0
 */
public class InMemoryEmbeddedH2DatabaseFactory extends DatabaseFactory {
	/**
	 *
	 */
	private static final long serialVersionUID = -5549181783426731120L;

	private String databaseName=null;

	public InMemoryEmbeddedH2DatabaseFactory() {

	}

	public InMemoryEmbeddedH2DatabaseFactory(String databaseName) {
		this.databaseName = databaseName;
	}

	@Override
	protected DatabaseWrapper newWrapperInstance() throws DatabaseException {
		return new EmbeddedH2DatabaseWrapper(true, databaseName);
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
}

