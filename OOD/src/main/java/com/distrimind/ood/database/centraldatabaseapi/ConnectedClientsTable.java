package com.distrimind.ood.database.centraldatabaseapi;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.DecentralizedValue;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public class ConnectedClientsTable extends Table<ConnectedClientsTable.Record> {
	protected ConnectedClientsTable() throws DatabaseException {
	}

	public static class Record extends DatabaseRecord
	{
		@PrimaryKey
		@NotNull
		private DecentralizedValue clientID;

		@Field
		@NotNull
		private DecentralizedValue centralID;

		private Record()
		{

		}

		Record(DecentralizedValue clientID, DecentralizedValue centralID) {
			if (clientID==null)
				throw new NullPointerException();
			if (centralID==null)
				throw new NullPointerException();
			this.clientID = clientID;
			this.centralID=centralID;
		}

		public DecentralizedValue getClientID() {
			return clientID;
		}

		public DecentralizedValue getCentralID() {
			return centralID;
		}
	}
}
