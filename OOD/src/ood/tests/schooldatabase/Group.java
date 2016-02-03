package ood.tests.schooldatabase;

import java.math.BigInteger;

import ood.database.DatabaseRecord;
import ood.database.Table;
import ood.database.annotations.Field;
import ood.database.annotations.RandomPrimaryKey;
import ood.database.exceptions.DatabaseException;

public final class Group extends Table<Group.Record>
{
    protected Group() throws DatabaseException
    {
	super();
    }

    public static class Record extends DatabaseRecord
    {
	protected Record()
	{
	    super();
	}
	
	@RandomPrimaryKey BigInteger id_group;
	@Field String name;
    }
}
