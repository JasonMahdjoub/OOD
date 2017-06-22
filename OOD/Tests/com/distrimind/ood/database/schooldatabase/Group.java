package com.distrimind.ood.database.schooldatabase;

import java.math.BigInteger;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.RandomPrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;

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
