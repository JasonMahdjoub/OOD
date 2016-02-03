package ood.tests.schooldatabase;

import ood.database.DatabaseRecord;
import ood.database.Table;
import ood.database.annotations.Field;
import ood.database.annotations.NotNull;
import ood.database.annotations.RandomPrimaryKey;
import ood.database.exceptions.DatabaseException;

public final class Student extends Table<Student.Record>
{

    
    protected Student() throws DatabaseException
    {
	super();
    }

    public static class Record extends DatabaseRecord
    {
	protected Record()
	{
	    super();
	}
	@RandomPrimaryKey long id_student;
	@Field @NotNull String name;
	@Field Long DateOfBirth;
    }
}
