package ood.tests.schooldatabase;

import ood.database.DatabaseRecord;
import ood.database.Table;
import ood.database.annotations.ForeignKey;
import ood.database.annotations.PrimaryKey;
import ood.database.exceptions.DatabaseException;

public final class StudentGroup extends Table<StudentGroup.Record>
{

    protected StudentGroup() throws DatabaseException
    {
	super();
    }

    public static class Record extends DatabaseRecord
    {
	protected Record()
	{
	    super();
	}
	
	@PrimaryKey @ForeignKey Student.Record student;
	@PrimaryKey @ForeignKey Group.Record group;
    }
}
