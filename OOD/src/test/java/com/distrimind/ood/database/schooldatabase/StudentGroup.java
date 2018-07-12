package com.distrimind.ood.database.schooldatabase;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;

public final class StudentGroup extends Table<StudentGroup.Record> {

	protected StudentGroup() throws DatabaseException {
		super();
	}
	@SuppressWarnings("unused")
	public static class Record extends DatabaseRecord {
		protected Record() {
			super();
		}

		@PrimaryKey
		@ForeignKey
		Student.Record student;
		@PrimaryKey
		@ForeignKey
		Group.Record group;
	}
}
