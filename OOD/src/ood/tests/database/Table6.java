package ood.tests.database;

import ood.database.DatabaseRecord;
import ood.database.Table;
import ood.database.annotations.ForeignKey;
import ood.database.annotations.NotNull;
import ood.database.annotations.PrimaryKey;
import ood.database.exceptions.DatabaseException;

public final class Table6 extends Table<Table6.Record>
{
    protected Table6() throws DatabaseException
    {
	super();
    }

    public static class Record extends DatabaseRecord
    {
	protected Record()
	{
	    
	}
	public @PrimaryKey @ForeignKey Table2.Record fk1_pk1;
	public @NotNull @ForeignKey Table5.Record fk2;
    }
}
