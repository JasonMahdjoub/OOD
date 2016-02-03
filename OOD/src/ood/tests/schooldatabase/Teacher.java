package ood.tests.schooldatabase;

import java.util.ArrayList;

import ood.database.DatabaseRecord;
import ood.database.Filter;
import ood.database.HSQLDBWrapper;
import ood.database.Table;
import ood.database.annotations.AutoPrimaryKey;
import ood.database.annotations.Field;
import ood.database.annotations.NotNull;
import ood.database.exceptions.DatabaseException;


public final class Teacher extends Table<Teacher.Record>
{
    protected Teacher() throws DatabaseException
    {
	super();
    }

    public static class Record extends DatabaseRecord
    {
	protected Record()
	{
	    super();
	}
	
	@AutoPrimaryKey long id_teacher;
	@Field @NotNull String name;
	@Field @NotNull long DateOfBirth;
    }
    
    public ArrayList<Lecture.Record> getLectures(HSQLDBWrapper sql_connection, final Teacher.Record _teacher) throws DatabaseException
    {
	TeacherLecture tl=(TeacherLecture)getTableInstance(sql_connection, TeacherLecture.class);
	ArrayList<TeacherLecture.Record> tls=tl.getRecords(new Filter<TeacherLecture.Record>() {
	    
	    @Override
	    public boolean nextRecord(TeacherLecture.Record _record) throws DatabaseException
	    {
		if (Teacher.this.equals(_record.teacher, _teacher))
		    return true;
		return false;
	    }
	});
	ArrayList<Lecture.Record> res=new ArrayList<>(tls.size());
	for (TeacherLecture.Record r : tls)
	    res.add(r.lecture);
	return res;
    }
}
