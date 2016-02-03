package ood.tests.database;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;

import ood.database.DatabaseRecord;
import ood.database.Table;
import ood.database.annotations.AutoPrimaryKey;
import ood.database.annotations.Field;
import ood.database.annotations.LoadToMemory;
import ood.database.annotations.NotNull;
import ood.database.annotations.PrimaryKey;
import ood.database.annotations.RandomPrimaryKey;
import ood.database.exceptions.DatabaseException;

@LoadToMemory
public final class Table3 extends Table<Table3.Record>
{
    protected Table3() throws DatabaseException
    {
	super();
    }

    public static class Record extends DatabaseRecord
    {
	protected Record()
	{
	    
	}
	public @PrimaryKey int pk1;
	public @AutoPrimaryKey long pk2;
	public @RandomPrimaryKey BigInteger pk3;
	public @RandomPrimaryKey long pk4;
	public @Field int int_value;
	public @Field byte byte_value;
	public @Field char char_value;
	public @Field boolean boolean_value;
	public @Field short short_value;
	public @Field long long_value;
	public @Field float float_value;
	public @Field double double_value;
	public @Field String string_value;
	public @Field Integer IntegerNumber_value;
	public @Field Byte ByteNumber_value;
	public @Field Character CharacterNumber_value;
	public @Field Boolean BooleanNumber_value;
	public @Field Short ShortNumber_value;
	public @Field Long LongNumber_value;
	public @Field Float FloatNumber_value;
	public @Field Double DoubleNumber_value;
	public @Field byte [] byte_array_value;
	public @NotNull @Field BigInteger BigInteger_value;
	public @NotNull @Field BigDecimal BigDecimal_value;
	public @NotNull @Field Date DateValue;
	public @NotNull @Field Calendar CalendarValue;
	
    }
}

