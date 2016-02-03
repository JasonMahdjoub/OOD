package ood.tests.schooldatabase;

import ood.database.DatabaseRecord;
import ood.database.Table;
import ood.database.annotations.ForeignKey;
import ood.database.annotations.PrimaryKey;
import ood.database.exceptions.DatabaseException;

public final class TeacherLecture extends Table<TeacherLecture.Record>
{

    protected TeacherLecture() throws DatabaseException
    {
	super();
    }

    public static class Record extends DatabaseRecord
    {
	protected Record()
	{
	    super();
	}
	@PrimaryKey @ForeignKey Teacher.Record teacher;
	@PrimaryKey @ForeignKey Lecture.Record lecture;
    }
    
    
}
