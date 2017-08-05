package com.distrimind.ood.database.schooldatabase;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.AutoPrimaryKey;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.exceptions.DatabaseException;

public final class Lecture extends Table<Lecture.Record> {

	protected Lecture() throws DatabaseException {
		super();
	}

	public static class Record extends DatabaseRecord {
		protected Record() {
			super();
		}

		@AutoPrimaryKey
		long id_lecture;
		@Field
		@NotNull
		String name;
		@Field
		@NotNull
		String description;
		@ForeignKey
		Group.Record group;
	}
}
