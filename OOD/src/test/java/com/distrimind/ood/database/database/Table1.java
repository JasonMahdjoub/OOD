package com.distrimind.ood.database.database;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.annotations.AutoPrimaryKey;
import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.annotations.NotNull;
import com.distrimind.ood.database.annotations.PrimaryKey;
import com.distrimind.ood.database.annotations.RandomPrimaryKey;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.DecentralizedIDGenerator;
import com.distrimind.util.RenforcedDecentralizedIDGenerator;
import com.distrimind.util.crypto.SymmetricEncryptionType;
import com.distrimind.util.crypto.SymmetricSecretKey;

public final class Table1 extends Table<Table1.Record> {
	protected Table1() throws DatabaseException {
		super();
	}

	public static class Record extends DatabaseRecord {
		public @PrimaryKey int pk1;
		public @AutoPrimaryKey long pk2;
		public @RandomPrimaryKey BigInteger pk3;
		public @RandomPrimaryKey long pk4;
		public @RandomPrimaryKey AbstractDecentralizedID pk5;
		public @RandomPrimaryKey DecentralizedIDGenerator pk6;
		public @RandomPrimaryKey RenforcedDecentralizedIDGenerator pk7;
		public @NotNull @Field int int_value;
		public @NotNull @Field byte byte_value;
		public @NotNull @Field char char_value;
		public @NotNull @Field boolean boolean_value;
		public @NotNull @Field short short_value;
		public @NotNull @Field long long_value;
		public @NotNull @Field float float_value;
		public @NotNull @Field double double_value;
		public @NotNull @Field String string_value;
		public @NotNull @Field Integer IntegerNumber_value;
		public @NotNull @Field Byte ByteNumber_value;
		public @NotNull @Field Character CharacterNumber_value;
		public @NotNull @Field Boolean BooleanNumber_value;
		public @NotNull @Field Short ShortNumber_value;
		public @NotNull @Field Long LongNumber_value;
		public @NotNull @Field Float FloatNumber_value;
		public @NotNull @Field Double DoubleNumber_value;
		public @Field byte[] byte_array_value;
		public @NotNull @Field BigInteger BigInteger_value;
		public @NotNull @Field BigDecimal BigDecimal_value;
		public @NotNull @Field Date DateValue;
		public @NotNull @Field Calendar CalendarValue;
		public @NotNull @Field SymmetricSecretKey secretKey;
		public @NotNull @Field SymmetricEncryptionType typeSecretKey;
		public @NotNull @Field File file;
		@Field
		public @NotNull SubField subField;
		@Field
		public @NotNull SubSubField subSubField;
		@Field
		public DecentralizedIDGenerator nullField=null;

		@Override
		public String toString() {
			return "Record{" +
					"pk1=" + pk1 +
					", pk2=" + pk2 +
					", pk3=" + pk3 +
					", pk4=" + pk4 +
					", pk5=" + pk5 +
					", pk6=" + pk6 +
					", pk7=" + pk7 +
					", int_value=" + int_value +
					", byte_value=" + byte_value +
					", char_value=" + char_value +
					", boolean_value=" + boolean_value +
					", short_value=" + short_value +
					", long_value=" + long_value +
					", float_value=" + float_value +
					", double_value=" + double_value +
					", string_value='" + string_value + '\'' +
					", IntegerNumber_value=" + IntegerNumber_value +
					", ByteNumber_value=" + ByteNumber_value +
					", CharacterNumber_value=" + CharacterNumber_value +
					", BooleanNumber_value=" + BooleanNumber_value +
					", ShortNumber_value=" + ShortNumber_value +
					", LongNumber_value=" + LongNumber_value +
					", FloatNumber_value=" + FloatNumber_value +
					", DoubleNumber_value=" + DoubleNumber_value +
					", byte_array_value=" + Arrays.toString(byte_array_value) +
					", BigInteger_value=" + BigInteger_value +
					", BigDecimal_value=" + BigDecimal_value +
					", DateValue=" + DateValue +
					", CalendarValue=" + CalendarValue +
					", secretKey=" + secretKey +
					", typeSecretKey=" + typeSecretKey +
					", file=" + file +
					", subField=" + subField +
					", subSubField=" + subSubField +
					", nullField=" + nullField +
					'}';
		}
	}
}
