package ood.tests.database;

import ood.database.DatabaseRecord;
import ood.database.Table;
import ood.database.annotations.AutoPrimaryKey;
import ood.database.annotations.Field;
import ood.database.exceptions.DatabaseException;

public final class Table7 extends Table<Table7.Record>
{
    protected Table7() throws DatabaseException
    {
	super();
    }

    public static class Record extends DatabaseRecord
    {
	protected Record()
	{
	    
	}
	public @AutoPrimaryKey long pk1;
	public @Field int val1;
	public @Field int val2;
	public @Field int val3;
    }
}
