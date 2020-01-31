package com.distrimind.ood.database;

import com.distrimind.ood.database.exceptions.DatabaseException;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since Utils 2.4.0
 */
public class InMemoryEmbeddedH2DatabaseFactory extends DatabaseFactory {
	/**
	 *
	 */
	private static final long serialVersionUID = -5549181783426731120L;

	public InMemoryEmbeddedH2DatabaseFactory() {

	}


	@Override
	protected DatabaseWrapper newWrapperInstance() throws DatabaseException {
		return new EmbeddedH2DatabaseWrapper(true);
	}

}

