package com.distrimind.ood.database.schooldatabase;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.annotations.RandomPrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;

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
