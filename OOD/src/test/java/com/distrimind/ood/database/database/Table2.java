package com.distrimind.ood.database.database;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.annotations.Unique;
import com.distrimind.ood.database.exceptions.DatabaseException;

public final class Table2 extends Table<Table2.Record> {
	protected Table2() throws DatabaseException {
		super();
	}

	public static class Record extends DatabaseRecord {
		public @PrimaryKey @ForeignKey Table1.Record fr1_pk1;

		public @Unique @Field(sqlFieldName = "int_value_personalized") int int_value;

		protected Record() {

		}
	}

}
