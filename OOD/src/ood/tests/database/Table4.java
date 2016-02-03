package ood.tests.database;

import ood.database.DatabaseRecord;
import ood.database.Table;
import ood.database.annotations.Field;
import ood.database.annotations.ForeignKey;
import ood.database.annotations.PrimaryKey;
import ood.database.annotations.Unique;
import ood.database.exceptions.DatabaseException;

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
