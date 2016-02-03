package ood.tests.database;

import ood.database.DatabaseRecord;
import ood.database.Table;
import ood.database.annotations.Field;
import ood.database.annotations.ForeignKey;
import ood.database.annotations.PrimaryKey;
import ood.database.annotations.Unique;
import ood.database.exceptions.DatabaseException;

public final class Table2 extends Table<Table2.Record>
{
    protected Table2() throws DatabaseException
    {
	super();
    }

    public static class Record extends DatabaseRecord
    {
	public @PrimaryKey @ForeignKey Table1.Record fr1_pk1;
	public @Unique @Field int int_value;
	
	protected Record()
	{
	    
	}
    }
    
    
}
