package com.distrimind.ood.tests.database;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.AutoPrimaryKey;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.exceptions.DatabaseException;

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
