package com.distrimind.ood.database.schooldatabase;

import java.util.ArrayList;

import com.distrimind.ood.database.Filter;
import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.AutoPrimaryKey;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.exceptions.DatabaseException;

public final class Teacher extends Table<Teacher.Record> {
	protected Teacher() throws DatabaseException {
		super();
	}
	@SuppressWarnings("unused")
	public static class Record extends DatabaseRecord {
		protected Record() {
			super();
		}

		@AutoPrimaryKey
		long id_teacher;
		@Field
		@NotNull
		String name;
		@Field
		@NotNull
		long DateOfBirth;
	}

	public ArrayList<Lecture.Record> getLectures(DatabaseWrapper sql_connection, final Teacher.Record _teacher)
			throws DatabaseException {
		TeacherLecture tl = sql_connection.getTableInstance(TeacherLecture.class);
		ArrayList<TeacherLecture.Record> tls = tl.getRecords(new Filter<TeacherLecture.Record>() {

			@Override
			public boolean nextRecord(TeacherLecture.Record _record) throws DatabaseException {
				return Teacher.this.equals(_record.teacher, _teacher);
			}
		});
		ArrayList<Lecture.Record> res = new ArrayList<>(tls.size());
		for (TeacherLecture.Record r : tls)
			res.add(r.lecture);
		return res;
	}
}
