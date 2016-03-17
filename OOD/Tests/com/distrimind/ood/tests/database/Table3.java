package com.distrimind.ood.tests.database;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;

import javax.crypto.SecretKey;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.AutoPrimaryKey;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.LoadToMemory;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.annotations.RandomPrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.crypto.SymmetricEncryptionType;

@LoadToMemory
public final class Table3 extends Table<Table3.Record>
{
    protected Table3() throws DatabaseException
    {
	super();
    }

    public static class Record extends DatabaseRecord
    {
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
	public @NotNull @Field SecretKey secretKey;
	public @NotNull @Field SymmetricEncryptionType typeSecretKey;
	
    }
}

