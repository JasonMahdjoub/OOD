package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 2.4.0
 */
public class InMemoryEmbeddedHSQLDatabaseFactory extends DatabaseFactory {
	/**
	 *
	 */
	private static final long serialVersionUID = -5549181783426731120L;

	private String databaseName=null;
	private HSQLDBConcurrencyControl concurrencyControl=HSQLDBConcurrencyControl.DEFAULT;

	public InMemoryEmbeddedHSQLDatabaseFactory() {

	}

	public InMemoryEmbeddedHSQLDatabaseFactory(String databaseName) {
		this.databaseName = databaseName;
	}

	public InMemoryEmbeddedHSQLDatabaseFactory(String databaseName, HSQLDBConcurrencyControl concurrencyControl) {
		this.databaseName = databaseName;
		if (concurrencyControl==null)
			throw new NullPointerException();
		this.concurrencyControl = concurrencyControl;
	}

	@Override
	protected DatabaseWrapper newWrapperInstance() throws DatabaseException {
		return new EmbeddedHSQLDBWrapper(true, databaseName, concurrencyControl);
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

