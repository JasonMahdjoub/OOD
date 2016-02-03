package ood.tests.schooldatabase;

import ood.database.DatabaseRecord;
import ood.database.Table;
import ood.database.annotations.AutoPrimaryKey;
import ood.database.annotations.Field;
import ood.database.annotations.ForeignKey;
import ood.database.annotations.NotNull;
import ood.database.exceptions.DatabaseException;

public final class Lecture extends Table<Lecture.Record>
{

    protected Lecture() throws DatabaseException
    {
	super();
    }

    public static class Record extends DatabaseRecord
    {
	protected Record()
	{
	    super();
	}
	@AutoPrimaryKey long id_lecture;
	@Field @NotNull String name;
	@Field @NotNull String description;
	@ForeignKey Group.Record group;
    }
}
