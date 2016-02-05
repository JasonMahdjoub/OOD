package com.distrimind.ood.tests.database;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.ForeignKey;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.annotations.Unique;
import com.distrimind.ood.database.exceptions.DatabaseException;

public final class Table4 extends Table<Table4.Record>
{
    protected Table4() throws DatabaseException
    {
	super();
    }

    public static class Record extends DatabaseRecord
    {
	protected Record()
	{
	    
	}
	public @PrimaryKey @ForeignKey Table3.Record fr1_pk1;
	public @Unique @Field int int_value;
    }
    
    
}
