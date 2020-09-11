
/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

jason.mahdjoub@distri-mind.fr

This software (Object Oriented Database (OOD)) is a computer program 
whose purpose is to manage a local database with the object paradigm 
and the java langage 

This software is governed by the CeCILL-C license under French law and
abiding by the rules of distribution of free software.  You can  use, 
modify and/ or redistribute the software under the terms of the CeCILL-C
license as circulated by CEA, CNRS and INRIA at the following URL
"http://www.cecill.info". 

As a counterpart to the access to the source code and  rights to copy,
modify and redistribute granted by the license, users are provided only
with a limited warranty  and the software's author,  the holder of the
economic rights,  and the successive licensors  have only  limited
liability. 

In this respect, the user's attention is drawn to the risks associated
with loading,  using,  modifying and/or developing or reproducing the
software by the user in light of its specific status of free software,
that may mean  that it is complicated to manipulate,  and  that  also
therefore means  that it is reserved for developers  and  experienced
professionals having in-depth computer knowledge. Users are therefore
encouraged to load and test the software's suitability as regards their
requirements in conditions enabling the security of their systems and/or 
data to be ensured and,  more generally, to use and operate it in the 
same conditions as regards security. 

The fact that you are presently reading this means that you have had
knowledge of the CeCILL-C license and that you accept its terms.
 */

package com.distrimind.ood.database;


import com.distrimind.ood.database.annotations.Field;
import com.distrimind.ood.database.database.*;
import com.distrimind.ood.database.database.Table1.Record;
import com.distrimind.ood.database.exceptions.*;
import com.distrimind.ood.database.fieldaccessors.BigDecimalFieldAccessor;
import com.distrimind.ood.database.fieldaccessors.ByteTabFieldAccessor;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.database.schooldatabase.Lecture;
import com.distrimind.ood.interpreter.Interpreter;
import com.distrimind.ood.interpreter.RuleInstance;
import com.distrimind.ood.interpreter.RuleInstance.TableJunction;
import com.distrimind.ood.interpreter.SymbolType;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.DecentralizedIDGenerator;
import com.distrimind.util.RenforcedDecentralizedIDGenerator;
import com.distrimind.util.SecuredDecentralizedID;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.SecureRandomType;
import com.distrimind.util.crypto.SymmetricEncryptionType;
import com.distrimind.util.crypto.SymmetricSecretKey;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.*;

/**
 * 
 * @author Jason Mahdjoub
 * @version 2.0
 * @since OOD 1.0
 */
@SuppressWarnings("deprecation")

public abstract class TestDatabase {
	public abstract int getMultiTestsNumber();

	public abstract int getThreadTestsNumber();

	public abstract boolean isMultiConcurrentDatabase();

	static Table1 table1;
	static Table2 table2;
	static Table3 table3;
	static Table4 table4;
	static Table5 table5;
	static Table6 table6;
	static Table7 table7;
	static Table1 table1b;
	static Table2 table2b;
	static Table3 table3b;
	static Table4 table4b;
	static Table5 table5b;
	static Table6 table6b;

	static List<Class<?>> listClasses=Arrays.asList(Table1.class, Table2.class, Table3.class, Table4.class, Table5.class, Table6.class, Table7.class);
	static DatabaseConfiguration dbConfig1 = new DatabaseConfiguration(new DatabaseConfigurationParameters(Table1.class.getPackage(), DatabaseConfigurationParameters.SynchronizationType.NO_SYNCHRONIZATION), listClasses);
	static DatabaseConfiguration dbConfig2 = new DatabaseConfiguration(new DatabaseConfigurationParameters(Lecture.class.getPackage(), DatabaseConfigurationParameters.SynchronizationType.NO_SYNCHRONIZATION), listClasses);
	private static DatabaseWrapper sql_db;
	private static DatabaseWrapper sql_dbb;

	public abstract File getDatabaseBackupFileName();

	public static final AbstractSecureRandom secureRandom;
	static {
		AbstractSecureRandom rand = null;
		try {
			rand = SecureRandomType.DEFAULT.getSingleton(null);
		} catch (Exception e) {
			e.printStackTrace();
		}
		secureRandom = rand;
	}

	public static SubField getSubField() throws DatabaseException {
		try {

			SubField res = new SubField();
			res.BigDecimal_value = new BigDecimal("3.0");
			res.BigInteger_value = new BigInteger("54");
			res.boolean_value = false;
			res.BooleanNumber_value = Boolean.TRUE;
			res.byte_array_value = new byte[] { 1, 2, 3, 4, 5 };
			res.byte_value = 25;
			res.ByteNumber_value = (byte) 98;
			res.CalendarValue = Calendar.getInstance();
			res.char_value = 'f';
			res.CharacterNumber_value = 'c';
			res.DateValue = new Date();
			res.double_value = 0.245;
			res.DoubleNumber_value = 0.4;
			res.float_value = 0.478f;
			res.FloatNumber_value = 0.86f;
			res.int_value = 5244;
			res.IntegerNumber_value = 2214;
			res.long_value = 254545;
			res.LongNumber_value = 1452L;
			res.secretKey = SymmetricEncryptionType.AES_CBC_PKCS5Padding.getKeyGenerator(secureRandom).generateKey();
			res.typeSecretKey = SymmetricEncryptionType.AES_CBC_PKCS5Padding;
			res.subField = getSubSubField();
			res.string_value = "not null";
			res.ShortNumber_value = (short) 12;
			return res;
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}

	}

	public static SubSubField getSubSubField() throws DatabaseException {
		try {
			SubSubField res = new SubSubField();
			res.BigDecimal_value = new BigDecimal("3.0");
			res.BigInteger_value = new BigInteger("54");
			res.boolean_value = false;
			res.BooleanNumber_value = Boolean.TRUE;
			res.byte_array_value = new byte[] { 1, 2, 3, 4, 5 };
			res.byte_value = 25;
			res.ByteNumber_value = (byte) 98;
			res.CalendarValue = Calendar.getInstance();
			res.char_value = 'f';
			res.CharacterNumber_value = 'c';
			res.DateValue = new Date();
			res.double_value = 0.245;
			res.DoubleNumber_value = 0.4;
			res.float_value = 0.478f;
			res.FloatNumber_value = 0.86f;
			res.int_value = 5244;
			res.IntegerNumber_value = 2214;
			res.long_value = 254545;
			res.LongNumber_value = 1452L;
			res.secretKey = SymmetricEncryptionType.AES_CBC_PKCS5Padding.getKeyGenerator(secureRandom).generateKey();
			res.typeSecretKey = SymmetricEncryptionType.AES_CBC_PKCS5Padding;
			res.string_value = "not null";
			res.ShortNumber_value = (short) 12;
			return res;
		} catch (Exception e) {
			throw DatabaseException.getDatabaseException(e);
		}
	}

	public static void assertEquals(SubField actual, SubField expected) {
		Assert.assertEquals(actual.boolean_value, expected.boolean_value);
		Assert.assertEquals(actual.byte_value, expected.byte_value);
		Assert.assertEquals(actual.char_value, expected.char_value);
		Assert.assertEquals(actual.double_value, expected.double_value);
		Assert.assertEquals(actual.float_value, expected.float_value);
		Assert.assertEquals(actual.int_value, expected.int_value);
		Assert.assertEquals(actual.long_value, expected.long_value);
		Assert.assertEquals(actual.short_value, expected.short_value);
		Assert.assertEquals(actual.string_value, expected.string_value);
		Assert.assertEquals(actual.BigDecimal_value, expected.BigDecimal_value);
		Assert.assertEquals(actual.BigInteger_value, expected.BigInteger_value);
		Assert.assertEquals(actual.BooleanNumber_value, expected.BooleanNumber_value);
		Assert.assertEquals(actual.ByteNumber_value, expected.ByteNumber_value);
		Assert.assertEquals(actual.CharacterNumber_value, expected.CharacterNumber_value);
		Assert.assertEquals(actual.DoubleNumber_value, expected.DoubleNumber_value);
		Assert.assertEquals(actual.FloatNumber_value, expected.FloatNumber_value);
		Assert.assertEquals(actual.IntegerNumber_value, expected.IntegerNumber_value);
		Assert.assertEquals(actual.LongNumber_value, expected.LongNumber_value);
		Assert.assertEquals(actual.ShortNumber_value, expected.ShortNumber_value);
		/*
		 * Assert.assertEquals(actual.CalendarValue, expected.CalendarValue);
		 * Assert.assertEquals(actual.DateValue, expected.DateValue);
		 */
		// Assert.assertEquals(actual.secretKey, expected.secretKey);
		Assert.assertEquals(actual.typeSecretKey, expected.typeSecretKey);
		assertEquals(actual.subField, expected.subField);
	}

	public static void assertEquals(SubSubField actual, SubSubField expected) {
		Assert.assertEquals(actual.boolean_value, expected.boolean_value);
		Assert.assertEquals(actual.byte_value, expected.byte_value);
		Assert.assertEquals(actual.char_value, expected.char_value);
		Assert.assertEquals(actual.double_value, expected.double_value);
		Assert.assertEquals(actual.float_value, expected.float_value);
		Assert.assertEquals(actual.int_value, expected.int_value);
		Assert.assertEquals(actual.long_value, expected.long_value);
		Assert.assertEquals(actual.short_value, expected.short_value);
		Assert.assertEquals(actual.string_value, expected.string_value);
		Assert.assertEquals(actual.BigDecimal_value, expected.BigDecimal_value);
		Assert.assertEquals(actual.BigInteger_value, expected.BigInteger_value);
		Assert.assertEquals(actual.BooleanNumber_value, expected.BooleanNumber_value);
		Assert.assertEquals(actual.ByteNumber_value, expected.ByteNumber_value);
		Assert.assertEquals(actual.CharacterNumber_value, expected.CharacterNumber_value);
		Assert.assertEquals(actual.DoubleNumber_value, expected.DoubleNumber_value);
		Assert.assertEquals(actual.FloatNumber_value, expected.FloatNumber_value);
		Assert.assertEquals(actual.IntegerNumber_value, expected.IntegerNumber_value);
		Assert.assertEquals(actual.LongNumber_value, expected.LongNumber_value);
		Assert.assertEquals(actual.ShortNumber_value, expected.ShortNumber_value);
		/*
		 * Assert.assertEquals(actual.CalendarValue, expected.CalendarValue);
		 * Assert.assertEquals(actual.DateValue, expected.DateValue);
		 */
		// Assert.assertEquals(actual.secretKey, expected.secretKey);
		Assert.assertEquals(actual.typeSecretKey, expected.typeSecretKey);
	}

	public TestDatabase()
			throws DatabaseException, NoSuchAlgorithmException, NoSuchProviderException {
		typeSecretKey = SymmetricEncryptionType.AES_CBC_PKCS5Padding;
		secretKey = typeSecretKey.getKeyGenerator(SecureRandomType.DEFAULT.getSingleton(null)).generateKey();
		subField = getSubField();
		subSubField = getSubSubField();
		fileTest=new File("fileTest");
	}

	@Override
	public void finalize() {

		unloadDatabase();

	}

	@AfterClass
	public static void unloadDatabase()  {
		System.out.println("Unload database !");
		if (sql_db != null) {
			sql_db.close();
			sql_db = null;
		}
		if (sql_dbb != null) {
			sql_dbb.close();
			sql_dbb = null;
		}

	}

	public abstract DatabaseWrapper getDatabaseWrapperInstanceA() throws IllegalArgumentException, DatabaseException;

	public abstract DatabaseWrapper getDatabaseWrapperInstanceB() throws IllegalArgumentException, DatabaseException;

	public abstract void deleteDatabaseFilesA() throws IllegalArgumentException, DatabaseException;

	public abstract void deleteDatabaseFilesB() throws IllegalArgumentException, DatabaseException;

	@Test
	public void checkUnloadedDatabase() throws IllegalArgumentException, DatabaseException {
		deleteDatabaseFilesA();
		deleteDatabaseFilesB();
		sql_db = getDatabaseWrapperInstanceA();
		try {
			sql_db.loadDatabase(dbConfig1, false);
			fail();
		} catch (DatabaseException ignored) {
		}
	}

	@Test(dependsOnMethods = { "checkUnloadedDatabase" })
	public void firstLoad() throws IllegalArgumentException, DatabaseException {

		sql_db.loadDatabase(dbConfig1, true);
		table2 = sql_db.getTableInstance(Table2.class);
		table1 = sql_db.getTableInstance(Table1.class);
		table3 = sql_db.getTableInstance(Table3.class);
		table4 = sql_db.getTableInstance(Table4.class);
		table5 = sql_db.getTableInstance(Table5.class);
		table6 = sql_db.getTableInstance(Table6.class);
		table7 = sql_db.getTableInstance(Table7.class);
		//Assert.assertEquals(table3.getSqlTableName(), Table3.class.getAnnotation(TableName.class).sqlTableName().toUpperCase());
		boolean found=false;
		for (FieldAccessor fa : table2.getFieldAccessors()) {

			if (fa.getField().getName().equals("int_value")) {
				Assert.assertEquals(fa.getSqlFieldName(), fa.getField().getAnnotation(Field.class).sqlFieldName().toUpperCase());
				found=true;
				break;
			}
		}
		Assert.assertTrue(found);
	}

	@Test(dependsOnMethods = { "firstLoad" })
	public void isLoadedIntoMemory() throws DatabaseException {
		assertFalse(table1.isLoadedInMemory());
		assertFalse(table2.isLoadedInMemory());
		assertTrue(table3.isLoadedInMemory());
		assertFalse(table4.isLoadedInMemory());
		assertTrue(table5.isLoadedInMemory());

		assertFalse(table1.isCached());
		assertTrue(table2.isCached() || !getDatabaseWrapperInstanceA().supportCache());
		assertFalse(table3.isCached());
		assertTrue(table4.isCached() || !getDatabaseWrapperInstanceA().supportCache());
		assertFalse(table5.isCached());

	}

	Date date = Calendar.getInstance().getTime();
	Calendar calendar = Calendar.getInstance();
	final SymmetricEncryptionType typeSecretKey;
	final SymmetricSecretKey secretKey;
	final SubField subField;
	final SubSubField subSubField;
	final File fileTest;

	@Test(dependsOnMethods = { "firstLoad" })
	public void firstAdd() throws DatabaseException {
		HashMap<String, Object> map = new HashMap<>();
		map.put("pk1", 0);
		map.put("int_value", 3);
		map.put("byte_value", (byte) 3);
		map.put("char_value", 'x');
		map.put("boolean_value", Boolean.TRUE);
		map.put("short_value", (short) 3);
		map.put("long_value", 300000004556256L);
		map.put("float_value", 3.3f);
		map.put("double_value", 3.3);
		map.put("string_value", "test string");
		map.put("IntegerNumber_value", 3);
		map.put("ByteNumber_value", (byte) 3);
		map.put("CharacterNumber_value", 'x');
		map.put("BooleanNumber_value", Boolean.TRUE);
		map.put("ShortNumber_value", (short) 3);
		map.put("LongNumber_value", 300000004556256L);
		map.put("FloatNumber_value", 3.3f);
		map.put("DoubleNumber_value", 3.3);
		map.put("BigInteger_value", new BigInteger("5"));
		map.put("BigDecimal_value", new BigDecimal("8.8"));
		map.put("DateValue", date);
		map.put("CalendarValue", calendar);
		map.put("secretKey", secretKey);
		map.put("typeSecretKey", typeSecretKey);
		map.put("subField", subField);
		map.put("subSubField", subSubField);
		map.put("file", fileTest);
		byte[] tab = new byte[3];
		tab[0] = 0;
		tab[1] = 1;
		tab[2] = 2;
		map.put("byte_array_value", tab);
		Table1.Record r1 = table1.addRecord(map);
		Table3.Record r2 = table3.addRecord(map);
        Assert.assertEquals(0, r1.pk1);
        Assert.assertEquals(0, r2.pk1);


		Assert.assertEquals(r1.pk2, 1);
		Assert.assertEquals(r2.pk2, 1);
		Assert.assertEquals(table1.getRecordsNumber(), 1);
		Assert.assertEquals(table3.getRecordsNumber(), 1);
		Assert.assertEquals(table1.getRecordsNumber("int_value=%v", "v", 3), 1);
		Assert.assertEquals(table1.getRecordsNumber("int_value=%v", "v", 4), 0);
		Assert.assertEquals(table3.getRecordsNumber("int_value=%v", "v", 3), 1);
		Assert.assertEquals(table3.getRecordsNumber("int_value=%v", "v", 4), 0);
	}

	@Test(dependsOnMethods = { "firstAdd" })
	public void firstTestSize() throws DatabaseException {
        Assert.assertEquals(1, table1.getRecordsNumber());
        Assert.assertEquals(1, table3.getRecordsNumber());
	}

	@Test(dependsOnMethods = { "firstTestSize" })
	public void firstReload() throws DatabaseException {
		sql_db.close();
		sql_db = getDatabaseWrapperInstanceA();
		sql_db.loadDatabase(dbConfig1, false);
		sql_db.loadDatabase(dbConfig2, true);

		table2 = sql_db.getTableInstance(Table2.class);
		table1 = sql_db.getTableInstance(Table1.class);
		table3 = sql_db.getTableInstance(Table3.class);
		table4 = sql_db.getTableInstance(Table4.class);
		table5 = sql_db.getTableInstance(Table5.class);
		table6 = sql_db.getTableInstance(Table6.class);
		table7 = sql_db.getTableInstance(Table7.class);
		sql_dbb = getDatabaseWrapperInstanceB();
		sql_dbb.loadDatabase(dbConfig1, true);
		table2b = sql_dbb.getTableInstance(Table2.class);
		table1b = sql_dbb.getTableInstance(Table1.class);
		table3b = sql_dbb.getTableInstance(Table3.class);
		table4b = sql_dbb.getTableInstance(Table4.class);
		table5b = sql_dbb.getTableInstance(Table5.class);
		table6b = sql_dbb.getTableInstance(Table6.class);

        Assert.assertEquals(sql_db, sql_db);
        assertNotEquals(sql_db, sql_dbb);

        assertNotEquals(table1.getDatabaseWrapper(), table1b.getDatabaseWrapper());
        assertNotEquals(table2.getDatabaseWrapper(), table2b.getDatabaseWrapper());
        assertNotEquals(table3.getDatabaseWrapper(), table3b.getDatabaseWrapper());
        assertNotEquals(table4.getDatabaseWrapper(), table4b.getDatabaseWrapper());
        assertNotEquals(table5.getDatabaseWrapper(), table5b.getDatabaseWrapper());
        assertNotEquals(table6.getDatabaseWrapper(), table6b.getDatabaseWrapper());

        assertNotEquals(table1, table1b);
        assertNotEquals(table2, table2b);
        assertNotEquals(table3, table3b);
        assertNotEquals(table4, table4b);
        assertNotEquals(table5, table5b);
        assertNotEquals(table6, table6b);

	}

	@Test(dependsOnMethods = { "firstReload" })
	public void secondTestSize() throws DatabaseException {
		Assert.assertEquals(table1.getRecordsNumber(), 1);
		Assert.assertEquals(table3.getRecordsNumber(), 1);

	}

	@Test(dependsOnMethods = { "secondTestSize" })
	public void testFirstAdd() throws DatabaseException {
		byte[] tab = new byte[3];
		tab[0] = 0;
		tab[1] = 1;
		tab[2] = 2;
		Table1.Record r = table1.getRecords().get(0);
        Assert.assertEquals(0, r.pk1);
        Assert.assertEquals(3, r.int_value);
        Assert.assertEquals(r.byte_value, (byte) 3);
        Assert.assertEquals('x', r.char_value);
		assertTrue(r.boolean_value);
        Assert.assertEquals(r.short_value, (short) 3);
        Assert.assertEquals(300000004556256L, r.long_value);
        Assert.assertEquals(3.3f, r.float_value, 0.0);
        Assert.assertEquals(3.3, r.double_value, 0.0);
        Assert.assertEquals("test string", r.string_value);
        Assert.assertEquals(3, r.IntegerNumber_value.intValue());
        Assert.assertEquals(r.ByteNumber_value.byteValue(), (byte) 3);
        Assert.assertEquals('x', r.CharacterNumber_value.charValue());
		assertTrue(r.BooleanNumber_value);
        Assert.assertEquals(r.ShortNumber_value.shortValue(), (short) 3);
        Assert.assertEquals(300000004556256L, (long) r.LongNumber_value);
        Assert.assertEquals(3.3f, r.FloatNumber_value, 0.0);
        Assert.assertEquals(3.3, r.DoubleNumber_value, 0.0);
        Assert.assertEquals(r.BigInteger_value, new BigInteger("5"));
        Assert.assertEquals(r.BigDecimal_value, new BigDecimal("8.8"));
        Assert.assertEquals(r.DateValue, date);
        Assert.assertEquals(r.CalendarValue, calendar);
        Assert.assertEquals(r.secretKey, secretKey);
        Assert.assertEquals(r.typeSecretKey, typeSecretKey);
        Assert.assertEquals(r.file, fileTest);
		assertEquals(r.subField, subField);
		assertEquals(r.subSubField, subSubField);
		for (int i = 0; i < 3; i++)
            Assert.assertEquals(r.byte_array_value[i], tab[i]);

		Map<String, Object> pks=new HashMap<>();
		pks.put("pk1", r.pk1);
		pks.put("pk2", r.pk2);
		pks.put("pk3", r.pk3);
		pks.put("pk4", r.pk4);
		pks.put("pk5", r.pk5);
		pks.put("pk6", r.pk6);
		pks.put("pk7", r.pk7);
		r=table1.getRecord(pks);
		assertNotNull(r);
        Assert.assertEquals(0, r.pk1);
        Assert.assertEquals(3, r.int_value);
        Assert.assertEquals(r.byte_value, (byte) 3);
        Assert.assertEquals('x', r.char_value);
		assertTrue(r.boolean_value);
        Assert.assertEquals(r.short_value, (short) 3);
        Assert.assertEquals(300000004556256L, r.long_value);
        Assert.assertEquals(3.3f, r.float_value, 0.0);
        Assert.assertEquals(3.3, r.double_value, 0.0);
        Assert.assertEquals("test string", r.string_value);
        Assert.assertEquals(3, r.IntegerNumber_value.intValue());
        Assert.assertEquals(r.ByteNumber_value.byteValue(), (byte) 3);
        Assert.assertEquals('x', r.CharacterNumber_value.charValue());
		assertTrue(r.BooleanNumber_value);
        Assert.assertEquals(r.ShortNumber_value.shortValue(), (short) 3);
        Assert.assertEquals(300000004556256L, r.LongNumber_value.longValue());
        Assert.assertEquals(3.3f, r.FloatNumber_value, 0.0);
        Assert.assertEquals(3.3, r.DoubleNumber_value, 0.0);
        Assert.assertEquals(r.BigInteger_value, new BigInteger("5"));
        Assert.assertEquals(r.BigDecimal_value, new BigDecimal("8.8"));
        Assert.assertEquals(r.DateValue, date);
        Assert.assertEquals(r.CalendarValue, calendar);
        Assert.assertEquals(r.secretKey, secretKey);
        Assert.assertEquals(r.typeSecretKey, typeSecretKey);
        Assert.assertEquals(r.file, fileTest);
		assertEquals(r.subField, subField);
		assertEquals(r.subSubField, subSubField);
		
		for (int i = 0; i < 3; i++)
            Assert.assertEquals(r.byte_array_value[i], tab[i]);

		Table3.Record r2 = table3.getRecords().get(0);
        Assert.assertEquals(0, r2.pk1);
        Assert.assertEquals(3, r2.int_value);
        Assert.assertEquals(r2.byte_value, (byte) 3);
        Assert.assertEquals('x', r2.char_value);
		assertTrue(r2.boolean_value);
        Assert.assertEquals(r2.short_value, (short) 3);
        Assert.assertEquals(300000004556256L, r2.long_value);
        Assert.assertEquals(3.3f, r2.float_value, 0.0);
        Assert.assertEquals(3.3, r2.double_value, 0.0);
        Assert.assertEquals("test string", r2.string_value);
        Assert.assertEquals(3, r2.IntegerNumber_value.intValue());
        Assert.assertEquals(r2.ByteNumber_value.byteValue(), (byte) 3);
        Assert.assertEquals('x', r2.CharacterNumber_value.charValue());
		assertTrue(r2.BooleanNumber_value);
        Assert.assertEquals(r2.ShortNumber_value.shortValue(), (short) 3);
        Assert.assertEquals(300000004556256L, r2.LongNumber_value.longValue());
        Assert.assertEquals(3.3f, r2.FloatNumber_value, 0.0);
        Assert.assertEquals(3.3, r2.DoubleNumber_value, 0.0);
        Assert.assertEquals(r2.BigInteger_value, new BigInteger("5"));
        Assert.assertEquals(r2.BigDecimal_value, new BigDecimal("8.8"));
        Assert.assertEquals(r2.DateValue, date);
        Assert.assertEquals(r2.CalendarValue, calendar);
        Assert.assertEquals(r2.secretKey, secretKey);
        Assert.assertEquals(r2.typeSecretKey, typeSecretKey);
        Assert.assertEquals(r2.file, fileTest);
		assertEquals(r2.subField, subField);
		assertEquals(r2.subSubField, subSubField);

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(r2.byte_array_value[i], tab[i]);
		
		
		pks=new HashMap<>();
		pks.put("pk1", r2.pk1);
		pks.put("pk2", r2.pk2);
		pks.put("pk3", r2.pk3);
		pks.put("pk4", r2.pk4);
		pks.put("pk5", r2.pk5);
		pks.put("pk6", r2.pk6);
		pks.put("pk7", r2.pk7);
		r2=table3.getRecord(pks);
		assertNotNull(r2);
        Assert.assertEquals(0, r2.pk1);
        Assert.assertEquals(3, r2.int_value);
        Assert.assertEquals(r2.byte_value, (byte) 3);
        Assert.assertEquals('x', r2.char_value);
		assertTrue(r2.boolean_value);
        Assert.assertEquals(r2.short_value, (short) 3);
        Assert.assertEquals(300000004556256L, r2.long_value);
        Assert.assertEquals(3.3f, r2.float_value, 0.0);
        Assert.assertEquals(3.3, r2.double_value, 0.0);
        Assert.assertEquals("test string", r2.string_value);
        Assert.assertEquals(3, r2.IntegerNumber_value.intValue());
        Assert.assertEquals(r2.ByteNumber_value.byteValue(), (byte) 3);
        Assert.assertEquals('x', r2.CharacterNumber_value.charValue());
		assertTrue(r2.BooleanNumber_value);
        Assert.assertEquals((short) r2.ShortNumber_value, (short) 3);
        Assert.assertEquals(300000004556256L, (long) r2.LongNumber_value);
        Assert.assertEquals(3.3f, r2.FloatNumber_value, 0.0);
        Assert.assertEquals(3.3, r2.DoubleNumber_value, 0.0);
        Assert.assertEquals(r2.BigInteger_value, new BigInteger("5"));
        Assert.assertEquals(r2.BigDecimal_value, new BigDecimal("8.8"));
        Assert.assertEquals(r2.DateValue, date);
        Assert.assertEquals(r2.CalendarValue, calendar);
        Assert.assertEquals(r2.secretKey, secretKey);
        Assert.assertEquals(r2.typeSecretKey, typeSecretKey);
        Assert.assertEquals(r2.file, fileTest);
		assertEquals(r2.subField, subField);
		assertEquals(r2.subSubField, subSubField);
		

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(r2.byte_array_value[i], tab[i]);
		
	}
	@Test(dependsOnMethods = { "alterRecord" })
	public void testCursor() throws DatabaseException {
		testCursor(table1);
		testCursor(table2);
		testCursor(table3);
		testCursor(table4);
		testCursor(table1, "int_value");
		testCursor(table3, "int_value");
	}

	private static <T extends DatabaseRecord> void testCursor(Table<T> table, String ... fields) throws DatabaseException {
		List<T> expected1=table.getRecords();
		Cursor<T> c=fields.length>0?table.getCursorWithOrderedResults(2, true, fields):table.getCursor(2);

		Assert.assertEquals(c.getTotalRecordsNumber(), expected1.size());



		for(int i=0;;i++)
		{

			T r=c.getRecord(i);
			if (r==null)
				break;
			boolean found=false;
			for (T r2 : expected1)
			{
				if (table.equalsAllFields(r2, r))
				{
					found=true;
					break;
				}
			}
			Assert.assertTrue(found);
		}
	}

	@Test(dependsOnMethods = { "testCursor" })
	public void addSecondRecord() throws DatabaseException {

		try {
			table1.addRecord((Map<String, Object>) null);
            fail();
		} catch (NullPointerException e) {
			assertTrue(true);
		}
		try {
			table1.addRecord(new HashMap<String, Object>());
            fail();
		} catch (DatabaseException e) {
			assertTrue(true);
		}


		table1.getDatabaseWrapper().runSynchronizedTransaction(new SynchronizedTransaction<Void>() {

			@Override
			public Void run() throws Exception {
				HashMap<String, Object> map = new HashMap<>();
				map.put("pk1", 0);
				map.put("int_value", 3);
				map.put("byte_value", (byte) 3);
				map.put("char_value", 'x');
				map.put("boolean_value", Boolean.TRUE);
				map.put("short_value", (short) 3);
				map.put("long_value", 3L);
				map.put("float_value", 3.3f);
				map.put("double_value", 3.3);
				map.put("string_value", "test string");
				map.put("IntegerNumber_value", 3);
				map.put("ByteNumber_value", (byte) 3);
				map.put("CharacterNumber_value", 'x');
				map.put("BooleanNumber_value", Boolean.TRUE);
				map.put("ShortNumber_value", (short) 3);
				map.put("LongNumber_value", (long) 3);
				map.put("FloatNumber_value", 3.3f);
				map.put("DoubleNumber_value", 3.3);
				map.put("BigInteger_value", new BigInteger("5"));
				map.put("BigDecimal_value", new BigDecimal("8.8"));
				map.put("DateValue", date);
				map.put("CalendarValue", calendar);
				map.put("secretKey", secretKey);
				map.put("typeSecretKey", typeSecretKey);
				map.put("subField", subField);
				map.put("subSubField", subSubField);
				map.put("file", fileTest);
				
				byte[] tab = new byte[3];
				tab[0] = 0;
				tab[1] = 1;
				tab[2] = 2;

				map.put("byte_array_value", tab);
				Table1.Record r1=table1.addRecord(map);

				Map<String, Object> pks=new HashMap<>();
				pks.put("pk1", r1.pk1);
				pks.put("pk2", r1.pk2);
				pks.put("pk3", r1.pk3);
				pks.put("pk4", r1.pk4);
				pks.put("pk5", r1.pk5);
				pks.put("pk6", r1.pk6);
				pks.put("pk7", r1.pk7);
				assertNotNull(table1.getRecord(pks));

				Table3.Record r3=table3.addRecord(map);

				pks=new HashMap<>();
				pks.put("pk1", r3.pk1);
				pks.put("pk2", r3.pk2);
				pks.put("pk3", r3.pk3);
				pks.put("pk4", r3.pk4);
				pks.put("pk5", r3.pk5);
				pks.put("pk6", r3.pk6);
				pks.put("pk7", r3.pk7);
				assertNotNull(table3.getRecord(pks));

				return null;
			}

			@Override
			public TransactionIsolation getTransactionIsolation() {
				return TransactionIsolation.TRANSACTION_SERIALIZABLE;
			}

			@Override
			public boolean doesWriteData() {
				return true;
			}

			@Override
			public void initOrReset() {
				
			}
		});

		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();


	}

	@Test(dependsOnMethods = { "testFirstAdd" })
	public void alterRecord() throws DatabaseException {
		HashMap<String, Object> map = new HashMap<>();
		map.put("pk1", 1);
		map.put("byte_value", (byte) 6);
		map.put("char_value", 'y');
		map.put("long_value", 300055004556256L);
		map.put("DoubleNumber_value", 6.6);
		map.put("subField", getSubField());
		map.put("subSubField", getSubSubField());
		byte[] tab = new byte[3];
		tab[0] = 3;
		tab[1] = 5;
		tab[2] = 6;
		map.put("byte_array_value", tab);

		Table1.Record r1 = table1.getRecords().get(0);
		table1.updateRecord(r1, map);
		Table3.Record r2 = table3.getRecords().get(0);
		table3.updateRecord(r2, map);

        Assert.assertEquals(1, r1.pk1);
        Assert.assertEquals(3, r1.int_value);
        Assert.assertEquals(r1.byte_value, (byte) 6);
        Assert.assertEquals('y', r1.char_value);
		assertTrue(r1.boolean_value);
        Assert.assertEquals(r1.short_value, (short) 3);
        Assert.assertEquals(300055004556256L, r1.long_value);
        Assert.assertEquals(3.3f, r1.float_value, 0.0);
        Assert.assertEquals(3.3, r1.double_value, 0.0);
        Assert.assertEquals("test string", r1.string_value);
        Assert.assertEquals(3, r1.IntegerNumber_value.intValue());
        Assert.assertEquals(r1.ByteNumber_value.byteValue(), (byte) 3);
        Assert.assertEquals('x', r1.CharacterNumber_value.charValue());
		assertTrue(r1.BooleanNumber_value);
        Assert.assertEquals(r1.ShortNumber_value.shortValue(), (short) 3);
        Assert.assertEquals(300000004556256L, r1.LongNumber_value.longValue());
        Assert.assertEquals(3.3f, r1.FloatNumber_value, 0.0);
        Assert.assertEquals(6.6, r1.DoubleNumber_value, 0.0);
        Assert.assertEquals(r1.BigInteger_value, new BigInteger("5"));
        Assert.assertEquals(r1.BigDecimal_value, new BigDecimal("8.8"));
        Assert.assertEquals(r1.DateValue, date);
        Assert.assertEquals(r1.CalendarValue, calendar);
        Assert.assertEquals(r1.secretKey, secretKey);
        Assert.assertEquals(r1.typeSecretKey, typeSecretKey);
        Assert.assertEquals(r1.file, fileTest);
		assertEquals(r1.subField, subField);
		assertEquals(r1.subSubField, subSubField);

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(r1.byte_array_value[i], tab[i]);

        Assert.assertEquals(1, r2.pk1);
        Assert.assertEquals(3, r2.int_value);
        Assert.assertEquals(r2.byte_value, (byte) 6);
        Assert.assertEquals('y', r2.char_value);
		assertTrue(r2.boolean_value);
        Assert.assertEquals(r2.short_value, (short) 3);
        Assert.assertEquals(300055004556256L, r2.long_value);
        Assert.assertEquals(3.3f, r2.float_value, 0.0);
        Assert.assertEquals(3.3, r2.double_value, 0.0);
        Assert.assertEquals("test string", r2.string_value);
        Assert.assertEquals(3, r2.IntegerNumber_value.intValue());
        Assert.assertEquals(r2.ByteNumber_value.byteValue(), (byte) 3);
        Assert.assertEquals('x', r2.CharacterNumber_value.charValue());
		assertTrue(r2.BooleanNumber_value);
        Assert.assertEquals((short) r2.ShortNumber_value, (short) 3);
        Assert.assertEquals(300000004556256L, (long) r2.LongNumber_value);
        Assert.assertEquals(3.3f, r2.FloatNumber_value, 0.0);
        Assert.assertEquals(6.6, r2.DoubleNumber_value, 0.0);
        Assert.assertEquals(r2.BigInteger_value, new BigInteger("5"));
        Assert.assertEquals(r2.BigDecimal_value, new BigDecimal("8.8"));
        Assert.assertEquals(r2.DateValue, date);
        Assert.assertEquals(r2.CalendarValue, calendar);
        Assert.assertEquals(r2.secretKey, secretKey);
        Assert.assertEquals(r2.typeSecretKey, typeSecretKey);
        Assert.assertEquals(r2.file, fileTest);
		assertEquals(r2.subField, subField);
		assertEquals(r2.subSubField, subSubField);

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(r2.byte_array_value[i], tab[i]);

		r1 = table1.getRecords().get(0);
		r2 = table3.getRecords().get(0);

        Assert.assertEquals(1, r1.pk1);
        Assert.assertEquals(3, r1.int_value);
        Assert.assertEquals(r1.byte_value, (byte) 6);
        Assert.assertEquals('y', r1.char_value);
		assertTrue(r1.boolean_value);
        Assert.assertEquals(r1.short_value, (short) 3);
        Assert.assertEquals(300055004556256L, r1.long_value);
        Assert.assertEquals(3.3f, r1.float_value, 0.0);
        Assert.assertEquals(3.3, r1.double_value, 0.0);
        Assert.assertEquals("test string", r1.string_value);
        Assert.assertEquals(3, r1.IntegerNumber_value.intValue());
        Assert.assertEquals(r1.ByteNumber_value.byteValue(), (byte) 3);
        Assert.assertEquals('x', r1.CharacterNumber_value.charValue());
		assertTrue(r1.BooleanNumber_value);
        Assert.assertEquals((short) r1.ShortNumber_value, (short) 3);
        Assert.assertEquals(300000004556256L, (long) r1.LongNumber_value);
        Assert.assertEquals(3.3f, r1.FloatNumber_value, 0.0);
        Assert.assertEquals(6.6, r1.DoubleNumber_value, 0.0);
        Assert.assertEquals(r1.BigInteger_value, new BigInteger("5"));
        Assert.assertEquals(r1.BigDecimal_value, new BigDecimal("8.8"));
        Assert.assertEquals(r1.DateValue, date);
        Assert.assertEquals(r1.CalendarValue, calendar);
        Assert.assertEquals(r1.secretKey, secretKey);
        Assert.assertEquals(r1.typeSecretKey, typeSecretKey);
        Assert.assertEquals(r1.file, fileTest);
		assertEquals(r1.subField, subField);
		assertEquals(r1.subSubField, subSubField);

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(r1.byte_array_value[i], tab[i]);

        Assert.assertEquals(1, r2.pk1);
        Assert.assertEquals(3, r2.int_value);
        Assert.assertEquals(r2.byte_value, (byte) 6);
        Assert.assertEquals('y', r2.char_value);
		assertTrue(r2.boolean_value);
        Assert.assertEquals(r2.short_value, (short) 3);
        Assert.assertEquals(300055004556256L, r2.long_value);
        Assert.assertEquals(3.3f, r2.float_value, 0.0);
        Assert.assertEquals(3.3, r2.double_value, 0.0);
        Assert.assertEquals("test string", r2.string_value);
        Assert.assertEquals(3, r2.IntegerNumber_value.intValue());
        Assert.assertEquals(r2.ByteNumber_value.byteValue(), (byte) 3);
        Assert.assertEquals('x', r2.CharacterNumber_value.charValue());
		assertTrue(r2.BooleanNumber_value);
        Assert.assertEquals((short) r2.ShortNumber_value, (short) 3);
        Assert.assertEquals(300000004556256L, (long) r2.LongNumber_value);
        Assert.assertEquals(3.3f, r2.FloatNumber_value, 0.0);
        Assert.assertEquals(6.6, r2.DoubleNumber_value, 0.0);
        Assert.assertEquals(r2.BigInteger_value, new BigInteger("5"));
        Assert.assertEquals(r2.BigDecimal_value, new BigDecimal("8.8"));
        Assert.assertEquals(r2.DateValue, date);
        Assert.assertEquals(r2.CalendarValue, calendar);
        Assert.assertEquals(r2.secretKey, secretKey);
        Assert.assertEquals(r2.typeSecretKey, typeSecretKey);
        Assert.assertEquals(r2.file, fileTest);
		assertEquals(r2.subField, subField);
		assertEquals(r2.subSubField, subSubField);

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(r2.byte_array_value[i], tab[i]);
		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	@Test(dependsOnMethods = { "addSecondRecord" })
	public void getRecord() throws DatabaseException {
		Table1.Record r1a = table1.getRecords().get(0);
		Table3.Record r2a = table3.getRecords().get(0);

		HashMap<String, Object> keys1 = new HashMap<>();
		keys1.put("pk1", r1a.pk1);
		keys1.put("pk2", r1a.pk2);
		keys1.put("pk3", r1a.pk3);
		keys1.put("pk4", r1a.pk4);
		keys1.put("pk5", r1a.pk5);
		keys1.put("pk6", r1a.pk6);
		keys1.put("pk7", r1a.pk7);

		HashMap<String, Object> keys2 = new HashMap<>();
		keys2.put("pk1", r2a.pk1);
		keys2.put("pk2", r2a.pk2);
		keys2.put("pk3", r2a.pk3);
		keys2.put("pk4", r2a.pk4);
		keys2.put("pk5", r2a.pk5);
		keys2.put("pk6", r2a.pk6);
		keys2.put("pk7", r2a.pk7);

		Table1.Record r1b = table1.getRecord(keys1);
		Table3.Record r2b = table3.getRecord(keys2);

        Assert.assertEquals(r1a.pk1, r1b.pk1);
        Assert.assertEquals(r1a.pk2, r1b.pk2);
        Assert.assertEquals(r1a.pk3, r1b.pk3);
        Assert.assertEquals(r1a.pk4, r1b.pk4);
        Assert.assertEquals(r1a.pk5, r1b.pk5);
		assertTrue(r1a.pk6.equals(r1b.pk6));
		assertTrue(r1a.pk7.equals(r1b.pk7));
        Assert.assertEquals(r1a.int_value, r1b.int_value);
        Assert.assertEquals(r1a.byte_value, r1b.byte_value);
        Assert.assertEquals(r1a.char_value, r1b.char_value);
        Assert.assertEquals(r1a.boolean_value, r1b.boolean_value);
        Assert.assertEquals(r1a.short_value, r1b.short_value);
        Assert.assertEquals(r1a.long_value, r1b.long_value);
        Assert.assertEquals(r1a.float_value, r1b.float_value, 0.0);
        Assert.assertEquals(r1a.double_value, r1b.double_value, 0.0);
        Assert.assertEquals(r1a.string_value, r1b.string_value);
        Assert.assertEquals(r1a.IntegerNumber_value, r1b.IntegerNumber_value);
        Assert.assertEquals(r1a.ByteNumber_value, r1b.ByteNumber_value);
        Assert.assertEquals(r1a.CharacterNumber_value, r1b.CharacterNumber_value);
        Assert.assertEquals(r1a.BooleanNumber_value, r1b.BooleanNumber_value);
        Assert.assertEquals(r1a.ShortNumber_value, r1b.ShortNumber_value);
        Assert.assertEquals(r1a.LongNumber_value, r1b.LongNumber_value);
        Assert.assertEquals(r1a.FloatNumber_value, r1b.FloatNumber_value);
        Assert.assertEquals(r1a.DoubleNumber_value, r1b.DoubleNumber_value);
        Assert.assertEquals(r1a.BigInteger_value, new BigInteger("5"));
        Assert.assertEquals(r1a.BigDecimal_value, new BigDecimal("8.8"));
        Assert.assertEquals(r1a.DateValue, date);
        Assert.assertEquals(r1a.CalendarValue, calendar);
        Assert.assertEquals(r1a.secretKey, secretKey);
        Assert.assertEquals(r1a.typeSecretKey, typeSecretKey);
        Assert.assertEquals(r1a.file, fileTest);
		assertEquals(r1a.subField, subField);
		assertEquals(r1a.subSubField, subSubField);

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(r1a.byte_array_value[i], r1b.byte_array_value[i]);

        Assert.assertEquals(r2a.pk1, r2b.pk1);
        Assert.assertEquals(r2a.pk2, r2b.pk2);
        Assert.assertEquals(r2a.pk3, r2b.pk3);
        Assert.assertEquals(r2a.pk4, r2b.pk4);
        Assert.assertEquals(r2a.pk5, r2b.pk5);
		assertTrue(r2a.pk6.equals(r2b.pk6));
		assertTrue(r2a.pk7.equals(r2b.pk7));
        Assert.assertEquals(r2a.int_value, r2b.int_value);
        Assert.assertEquals(r2a.byte_value, r2b.byte_value);
        Assert.assertEquals(r2a.char_value, r2b.char_value);
        Assert.assertEquals(r2a.boolean_value, r2b.boolean_value);
        Assert.assertEquals(r2a.short_value, r2b.short_value);
        Assert.assertEquals(r2a.long_value, r2b.long_value);
        Assert.assertEquals(r2a.float_value, r2b.float_value, 0.0);
        Assert.assertEquals(r2a.double_value, r2b.double_value, 0.0);
        Assert.assertEquals(r2a.string_value, r2b.string_value);
        Assert.assertEquals(r2a.IntegerNumber_value, r2b.IntegerNumber_value);
        Assert.assertEquals(r2a.ByteNumber_value, r2b.ByteNumber_value);
        Assert.assertEquals(r2a.CharacterNumber_value, r2b.CharacterNumber_value);
        Assert.assertEquals(r2a.BooleanNumber_value, r2b.BooleanNumber_value);
        Assert.assertEquals(r2a.ShortNumber_value, r2b.ShortNumber_value);
        Assert.assertEquals(r2a.LongNumber_value, r2b.LongNumber_value);
        Assert.assertEquals(r2a.FloatNumber_value, r2b.FloatNumber_value);
        Assert.assertEquals(r2a.DoubleNumber_value, r2b.DoubleNumber_value);
        Assert.assertEquals(r2a.BigInteger_value, new BigInteger("5"));
        Assert.assertEquals(r2a.BigDecimal_value, new BigDecimal("8.8"));
        Assert.assertEquals(r2a.DateValue, date);
        Assert.assertEquals(r2a.CalendarValue, calendar);
        Assert.assertEquals(r2a.secretKey, secretKey);
        Assert.assertEquals(r2a.typeSecretKey, typeSecretKey);
        Assert.assertEquals(r2a.file, fileTest);
		assertEquals(r2a.subField, subField);
		assertEquals(r2a.subSubField, subSubField);

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(r2a.byte_array_value[i], r2b.byte_array_value[i]);
		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	/*
	 * @Test(dependsOnMethods={"getRecord"}) public void getRecordIterator() throws
	 * DatabaseException { try(TableIterator<Table1.Record>
	 * it1=table1.getIterator()) { try(TableIterator<Table3.Record>
	 * it2=table3.getIterator()) { Table1.Record r1a=null; Table3.Record r2a=null;
	 * Assert.assertTrue(it1.hasNext()); Assert.assertTrue(it2.hasNext());
	 * r1a=it1.next(); r2a=it2.next();
	 * 
	 * HashMap<String, Object> keys1=new HashMap<String, Object>(); keys1.put("pk1",
	 * new Integer(r1a.pk1)); keys1.put("pk2", new Long(r1a.pk2)); keys1.put("pk3",
	 * r1a.pk3); keys1.put("pk4", new Long(r1a.pk4)); keys1.put("pk5", r1a.pk5);
	 * keys1.put("pk6", r1a.pk6); keys1.put("pk7", r1a.pk7);
	 * 
	 * HashMap<String, Object> keys2=new HashMap<String, Object>(); keys2.put("pk1",
	 * new Integer(r2a.pk1)); keys2.put("pk2", new Long(r2a.pk2)); keys2.put("pk3",
	 * r2a.pk3); keys2.put("pk4", new Long(r2a.pk4)); keys2.put("pk5", r2a.pk5);
	 * keys2.put("pk6", r2a.pk6); keys2.put("pk7", r2a.pk7);
	 * 
	 * Table1.Record r1b=table1.getRecord(keys1); Table3.Record
	 * r2b=table3.getRecord(keys2);
	 * 
	 * Assert.assertTrue(r1a.pk1==r1b.pk1); Assert.assertTrue(r1a.pk2==r1b.pk2);
	 * Assert.assertTrue(r1a.pk3.equals(r1b.pk3));
	 * Assert.assertTrue(r1a.pk4==r1b.pk4);
	 * Assert.assertTrue(r1a.pk5.equals(r1b.pk5));
	 * Assert.assertTrue(r1a.pk6.equals(r1b.pk6));
	 * Assert.assertTrue(r1a.pk7.equals(r1b.pk7));
	 * Assert.assertTrue(r1a.int_value==r1b.int_value);
	 * Assert.assertTrue(r1a.byte_value==r1b.byte_value);
	 * Assert.assertTrue(r1a.char_value==r1b.char_value);
	 * Assert.assertTrue(r1a.boolean_value==r1b.boolean_value);
	 * Assert.assertTrue(r1a.short_value==r1b.short_value);
	 * Assert.assertTrue(r1a.long_value==r1b.long_value);
	 * Assert.assertTrue(r1a.float_value==r1b.float_value);
	 * Assert.assertTrue(r1a.double_value==r1b.double_value);
	 * Assert.assertTrue(r1a.string_value.equals(r1b.string_value));
	 * Assert.assertTrue(r1a.IntegerNumber_value.equals(r1b.IntegerNumber_value));
	 * Assert.assertTrue(r1a.ByteNumber_value.equals(r1b.ByteNumber_value));
	 * Assert.assertTrue(r1a.CharacterNumber_value.equals(r1b.CharacterNumber_value)
	 * );
	 * Assert.assertTrue(r1a.BooleanNumber_value.equals(r1b.BooleanNumber_value));
	 * Assert.assertTrue(r1a.ShortNumber_value.equals(r1b.ShortNumber_value));
	 * Assert.assertTrue(r1a.LongNumber_value.equals(r1b.LongNumber_value));
	 * Assert.assertTrue(r1a.FloatNumber_value.equals(r1b.FloatNumber_value));
	 * Assert.assertTrue(r1a.DoubleNumber_value.equals(r1b.DoubleNumber_value));
	 * Assert.assertTrue(r1a.BigInteger_value.equals(new BigInteger("5")));
	 * Assert.assertTrue(r1a.BigDecimal_value.equals(new BigDecimal("8.8")));
	 * Assert.assertTrue(r1a.DateValue.equals(date));
	 * Assert.assertTrue(r1a.CalendarValue.equals(calendar));
	 * Assert.assertTrue(r1a.secretKey.equals(secretKey));
	 * Assert.assertTrue(r1a.typeSecretKey.equals(typeSecretKey));
	 * assertEquals(r1a.subField, subField); assertEquals(r1a.subSubField,
	 * subSubField);
	 * 
	 * for (int i=0;i<3;i++)
	 * Assert.assertTrue(r1a.byte_array_value[i]==r1b.byte_array_value[i]);
	 * 
	 * Assert.assertTrue(r2a.pk1==r2b.pk1); Assert.assertTrue(r2a.pk2==r2b.pk2);
	 * Assert.assertTrue(r2a.pk3.equals(r2b.pk3));
	 * Assert.assertTrue(r2a.pk4==r2b.pk4);
	 * Assert.assertTrue(r2a.pk5.equals(r2b.pk5));
	 * Assert.assertTrue(r2a.pk6.equals(r2b.pk6));
	 * Assert.assertTrue(r2a.pk7.equals(r2b.pk7));
	 * Assert.assertTrue(r2a.int_value==r2b.int_value);
	 * Assert.assertTrue(r2a.byte_value==r2b.byte_value);
	 * Assert.assertTrue(r2a.char_value==r2b.char_value);
	 * Assert.assertTrue(r2a.boolean_value==r2b.boolean_value);
	 * Assert.assertTrue(r2a.short_value==r2b.short_value);
	 * Assert.assertTrue(r2a.long_value==r2b.long_value);
	 * Assert.assertTrue(r2a.float_value==r2b.float_value);
	 * Assert.assertTrue(r2a.double_value==r2b.double_value);
	 * Assert.assertTrue(r2a.string_value.equals(r2b.string_value));
	 * Assert.assertTrue(r2a.IntegerNumber_value.equals(r2b.IntegerNumber_value));
	 * Assert.assertTrue(r2a.ByteNumber_value.equals(r2b.ByteNumber_value));
	 * Assert.assertTrue(r2a.CharacterNumber_value.equals(r2b.CharacterNumber_value)
	 * );
	 * Assert.assertTrue(r2a.BooleanNumber_value.equals(r2b.BooleanNumber_value));
	 * Assert.assertTrue(r2a.ShortNumber_value.equals(r2b.ShortNumber_value));
	 * Assert.assertTrue(r2a.LongNumber_value.equals(r2b.LongNumber_value));
	 * Assert.assertTrue(r2a.FloatNumber_value.equals(r2b.FloatNumber_value));
	 * Assert.assertTrue(r2a.DoubleNumber_value.equals(r2b.DoubleNumber_value));
	 * Assert.assertTrue(r2a.BigInteger_value.equals(new BigInteger("5")));
	 * Assert.assertTrue(r2a.BigDecimal_value.equals(new BigDecimal("8.8")));
	 * Assert.assertTrue(r2a.DateValue.equals(date));
	 * Assert.assertTrue(r2a.CalendarValue.equals(calendar));
	 * Assert.assertTrue(r2a.secretKey.equals(secretKey));
	 * Assert.assertTrue(r2a.typeSecretKey.equals(typeSecretKey));
	 * assertEquals(r2a.subField, subField); assertEquals(r2a.subSubField,
	 * subSubField);
	 * 
	 * for (int i=0;i<3;i++)
	 * Assert.assertTrue(r2a.byte_array_value[i]==r2b.byte_array_value[i]);
	 * 
	 * } } table1.checkDataIntegrity(); table3.checkDataIntegrity();
	 * table2.checkDataIntegrity(); table4.checkDataIntegrity();
	 * table5.checkDataIntegrity(); table6.checkDataIntegrity(); }
	 */

	@Test(dependsOnMethods = { "addSecondRecord" })
	public void getRecordFilter() throws DatabaseException {
		final Table1.Record r1a = table1.getRecords().get(0);
		final Table3.Record r2a = table3.getRecords().get(0);

		ArrayList<Table1.Record> col1 = table1.getRecords(new Filter<Table1.Record>() {

			@Override
			public boolean nextRecord(Record _record) {
                return _record.pk1 == r1a.pk1 && _record.pk2 == r1a.pk2 && _record.pk4 == r1a.pk4;
            }
		});
        Assert.assertEquals(1, col1.size());
		Table1.Record r1b = col1.get(0);

		ArrayList<Table3.Record> col2 = table3.getRecords(new Filter<Table3.Record>() {

			@Override
			public boolean nextRecord(Table3.Record _record) {
                return _record.pk1 == r2a.pk1 && _record.pk2 == r2a.pk2 && _record.pk4 == r2a.pk4;
            }
		});
        Assert.assertEquals(1, col2.size());
		Table3.Record r2b = col2.get(0);

        Assert.assertEquals(r1a.pk1, r1b.pk1);
        Assert.assertEquals(r1a.pk2, r1b.pk2);
        Assert.assertEquals(r1a.pk3, r1b.pk3);
        Assert.assertEquals(r1a.pk4, r1b.pk4);
        Assert.assertEquals(r1a.pk5, r1b.pk5);
		assertTrue(r1a.pk6.equals(r1b.pk6));
		assertTrue(r1a.pk7.equals(r1b.pk7));
        Assert.assertEquals(r1a.int_value, r1b.int_value);
        Assert.assertEquals(r1a.byte_value, r1b.byte_value);
        Assert.assertEquals(r1a.char_value, r1b.char_value);
        Assert.assertEquals(r1a.boolean_value, r1b.boolean_value);
        Assert.assertEquals(r1a.short_value, r1b.short_value);
        Assert.assertEquals(r1a.long_value, r1b.long_value);
        Assert.assertEquals(r1a.float_value, r1b.float_value, 0.0);
        Assert.assertEquals(r1a.double_value, r1b.double_value, 0.0);
        Assert.assertEquals(r1a.string_value, r1b.string_value);
        Assert.assertEquals(r1a.IntegerNumber_value, r1b.IntegerNumber_value);
        Assert.assertEquals(r1a.ByteNumber_value, r1b.ByteNumber_value);
        Assert.assertEquals(r1a.CharacterNumber_value, r1b.CharacterNumber_value);
        Assert.assertEquals(r1a.BooleanNumber_value, r1b.BooleanNumber_value);
        Assert.assertEquals(r1a.ShortNumber_value, r1b.ShortNumber_value);
        Assert.assertEquals(r1a.LongNumber_value, r1b.LongNumber_value);
        Assert.assertEquals(r1a.FloatNumber_value, r1b.FloatNumber_value);
        Assert.assertEquals(r1a.DoubleNumber_value, r1b.DoubleNumber_value);
        Assert.assertEquals(r1a.BigInteger_value, r1b.BigInteger_value);
        Assert.assertEquals(r1a.BigDecimal_value, r1b.BigDecimal_value);
        Assert.assertEquals(r1a.DateValue, r1b.DateValue);
        Assert.assertEquals(r1a.CalendarValue, r1b.CalendarValue);
        Assert.assertEquals(r1a.secretKey, secretKey);
        Assert.assertEquals(r1a.typeSecretKey, typeSecretKey);
        Assert.assertEquals(r1a.file, fileTest);
		assertEquals(r1a.subField, subField);
		assertEquals(r1a.subSubField, subSubField);

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(r1a.byte_array_value[i], r1b.byte_array_value[i]);

        Assert.assertEquals(r2a.pk1, r2b.pk1);
        Assert.assertEquals(r2a.pk2, r2b.pk2);
        Assert.assertEquals(r2a.pk3, r2b.pk3);
        Assert.assertEquals(r2a.pk4, r2b.pk4);
        Assert.assertEquals(r2a.pk5, r2b.pk5);
		assertTrue(r2a.pk6.equals(r2b.pk6));
		assertTrue(r2a.pk7.equals(r2b.pk7));
        Assert.assertEquals(r2a.int_value, r2b.int_value);
        Assert.assertEquals(r2a.byte_value, r2b.byte_value);
        Assert.assertEquals(r2a.char_value, r2b.char_value);
        Assert.assertEquals(r2a.boolean_value, r2b.boolean_value);
        Assert.assertEquals(r2a.short_value, r2b.short_value);
        Assert.assertEquals(r2a.long_value, r2b.long_value);
        Assert.assertEquals(r2a.float_value, r2b.float_value, 0.0);
        Assert.assertEquals(r2a.double_value, r2b.double_value, 0.0);
        Assert.assertEquals(r2a.string_value, r2b.string_value);
        Assert.assertEquals(r2a.IntegerNumber_value, r2b.IntegerNumber_value);
        Assert.assertEquals(r2a.ByteNumber_value, r2b.ByteNumber_value);
        Assert.assertEquals(r2a.CharacterNumber_value, r2b.CharacterNumber_value);
        Assert.assertEquals(r2a.BooleanNumber_value, r2b.BooleanNumber_value);
        Assert.assertEquals(r2a.ShortNumber_value, r2b.ShortNumber_value);
        Assert.assertEquals(r2a.LongNumber_value, r2b.LongNumber_value);
        Assert.assertEquals(r2a.FloatNumber_value, r2b.FloatNumber_value);
        Assert.assertEquals(r2a.DoubleNumber_value, r2b.DoubleNumber_value);
        Assert.assertEquals(r2a.BigInteger_value, r2b.BigInteger_value);
        Assert.assertEquals(r2a.BigDecimal_value, r2b.BigDecimal_value);
        Assert.assertEquals(r2a.DateValue, r2b.DateValue);
        Assert.assertEquals(r2a.CalendarValue, r2b.CalendarValue);
        Assert.assertEquals(r2a.secretKey, secretKey);
        Assert.assertEquals(r2a.typeSecretKey, typeSecretKey);
        Assert.assertEquals(r2a.file, fileTest);
		assertEquals(r2a.subField, subField);
		assertEquals(r2a.subSubField, subSubField);

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(r2a.byte_array_value[i], r2b.byte_array_value[i]);

		HashMap<String, Object> map = new HashMap<>();
		map.put("fr1_pk1", table1.getRecords().get(0));
		map.put("int_value", 0);
		table2.addRecord(map);

		table1.getRecords(new Filter<Table1.Record>() {

			@Override
			public boolean nextRecord(Record _record) throws DatabaseException {
				table1.getRecords();
				return true;
			}
		});

		table1.getRecords(new Filter<Table1.Record>() {

			@Override
			public boolean nextRecord(Record _record) throws DatabaseException {
				table2.getRecords();
				return true;
			}
		});
		table2.getRecords(new Filter<Table2.Record>() {

			@Override
			public boolean nextRecord(Table2.Record _record) throws DatabaseException {
				table1.getRecords();
				return true;
			}
		});

		try {
			table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

				@Override
				public boolean nextRecord(Record _record) throws DatabaseException {
					table1.getRecords();
					return false;
				}
			});
			assertTrue(true);
		} catch (ConcurentTransactionDatabaseException e) {
			fail();
		}

		try {
			table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

				@Override
				public boolean nextRecord(Record _record) throws DatabaseException {
					table2.getRecords();
					return false;
				}
			});
			assertTrue(true);
		} catch (ConcurentTransactionDatabaseException e) {
			fail();
		}
		try {
			table2.removeRecordsWithCascade(new Filter<Table2.Record>() {

				@Override
				public boolean nextRecord(Table2.Record _record) throws DatabaseException {
					table1.getRecords();
					return false;
				}
			});
		} catch (ConcurentTransactionDatabaseException e) {
			assertTrue(true);
		}

		try {
			table1.getRecords(new Filter<Table1.Record>() {
				@Override
				public boolean nextRecord(Record _record) throws DatabaseException {
					table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

						@Override
						public boolean nextRecord(Record _record) {
							return false;
						}
					});
					return false;
				}
			});
            fail();
		} catch (ConcurentTransactionDatabaseException e) {
			assertTrue(true);
		}

		table1.getRecords(new Filter<Table1.Record>() {
			@Override
			public boolean nextRecord(Record _record) throws DatabaseException {
				table2.removeRecordsWithCascade(new Filter<Table2.Record>() {
					@Override
					public boolean nextRecord(Table2.Record _record) {
						return false;
					}
				});
				return false;
			}
		});

		try {
			table2.getRecords(new Filter<Table2.Record>() {

				@Override
				public boolean nextRecord(Table2.Record _record) throws DatabaseException {
					table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

						@Override
						public boolean nextRecord(Record _record) {
							return false;
						}
					});
					return false;
				}
			});
            fail();
		} catch (ConcurentTransactionDatabaseException e) {
			assertTrue(true);
		}
		table2.removeRecords(table2.getRecords());
		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	@Test(dependsOnMethods = { "getRecordFilter", "getRecord", "alterRecord" })
	public void removeRecord() throws DatabaseException {
		Table1.Record r1 = table1.getRecords().get(0);
		Table3.Record r2 = table3.getRecords().get(0);
		table1.removeRecord(r1);
		table3.removeRecord(r2);
        Assert.assertEquals(1, table1.getRecordsNumber());
        Assert.assertEquals(1, table3.getRecordsNumber());
        Assert.assertEquals(1, table1.getRecords().size());
        Assert.assertEquals(1, table3.getRecords().size());
		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	@Test(dependsOnMethods = { "removeRecord" })
	public void testArrayRecordParameters() throws DatabaseException {
		byte[] tab = new byte[3];
		tab[0] = 0;
		tab[1] = 1;
		tab[2] = 2;
		Object[] parameters = { "pk1", 4356, "int_value", 3, "byte_value", (byte) 3,
				"char_value", 'x', "boolean_value", Boolean.TRUE, "short_value",
                (short) 3, "long_value", 3L, "float_value", 3.3f, "double_value",
                3.3, "string_value", "test string", "IntegerNumber_value", 3,
				"ByteNumber_value", (byte) 3, "CharacterNumber_value", 'x',
				"BooleanNumber_value", Boolean.TRUE, "ShortNumber_value", (short) 3, "LongNumber_value",
                (long) 3, "FloatNumber_value", 3.3f, "DoubleNumber_value", 3.3,
				"BigInteger_value", new BigInteger("5"), "BigDecimal_value", new BigDecimal("8.8"), "DateValue", date,
				"CalendarValue", calendar, "secretKey", secretKey, "typeSecretKey", typeSecretKey, "byte_array_value",
				tab, "subField", subField, "subSubField", subSubField, "file", fileTest };
		Table1.Record r1 = table1.addRecord(parameters);
		Table3.Record r2 = table3.addRecord(parameters);
        Assert.assertEquals(4356, r1.pk1);
        Assert.assertEquals(4356, r2.pk1);
		table1.removeRecord(r1);
		table3.removeRecord(r2);

		Object[] p2 = { "rert" };
		Object[] p3 = {125, "rert" };
		try {
			table1.addRecord(p2);
            fail();
		} catch (Exception e) {
			assertTrue(true);
		}
		try {
			table1.addRecord(p3);
            fail();
		} catch (Exception e) {
			assertTrue(true);
		}
		try {
			table3.addRecord(p2);
            fail();
		} catch (Exception e) {
			assertTrue(true);
		}
		try {
			table3.addRecord(p3);
            fail();
		} catch (Exception e) {
			assertTrue(true);
		}
	}

	@Test(dependsOnMethods = { "testArrayRecordParameters" })
	public void removeRecords() throws DatabaseException {
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		ArrayList<Table1.Record> r1 = table1.getRecords();
		ArrayList<Table3.Record> r2 = table3.getRecords();
		table1.removeRecords(r1);
		table3.removeRecords(r2);
        Assert.assertEquals(0, table1.getRecords().size());
        Assert.assertEquals(0, table1.getRecordsNumber());
        Assert.assertEquals(0, table3.getRecords().size());
        Assert.assertEquals(0, table3.getRecordsNumber());
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		long size1 = table1.getRecordsNumber();
		long size2 = table3.getRecordsNumber();
		table1.removeRecords(new Filter<Table1.Record>() {

			@Override
			public boolean nextRecord(Record _record) {
				return false;
			}
		});
		table3.removeRecords(new Filter<Table3.Record>() {

			@Override
			public boolean nextRecord(com.distrimind.ood.database.database.Table3.Record _record) {
				return false;
			}
		});
        Assert.assertEquals(size1, table1.getRecordsNumber());
        Assert.assertEquals(size1, table1.getRecords().size());
        Assert.assertEquals(size2, table3.getRecordsNumber());
        Assert.assertEquals(size2, table3.getRecords().size());
		table1.removeRecords(new Filter<Table1.Record>() {

			@Override
			public boolean nextRecord(Record _record) {
				return true;
			}
		});
		table3.removeRecords(new Filter<Table3.Record>() {

			@Override
			public boolean nextRecord(com.distrimind.ood.database.database.Table3.Record _record) {
				return true;
			}
		});
        Assert.assertEquals(0, table1.getRecordsNumber());
        Assert.assertEquals(0, table1.getRecords().size());

        Assert.assertEquals(0, table3.getRecordsNumber());
        Assert.assertEquals(0, table3.getRecords().size());
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		table1.updateRecords(new AlterRecordFilter<Table1.Record>() {

			@Override
			public void nextRecord(Record _record) {
				this.remove();

			}
		});
		table3.updateRecords(new AlterRecordFilter<Table3.Record>() {

			@Override
			public void nextRecord(Table3.Record _record) {
				this.remove();

			}
		});
		Assert.assertEquals(table1.getRecordsNumber(), 0);
		Assert.assertEquals(table1.getRecords().size(), 0);

		Assert.assertEquals(table3.getRecordsNumber(), 0);
		Assert.assertEquals(table3.getRecords().size(), 0);
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		addSecondRecord();
		table1.updateRecords(new AlterRecordFilter<Table1.Record>() {

			@Override
			public void nextRecord(Record _record) {
				this.removeWithCascade();
			}
		});
		table3.updateRecords(new AlterRecordFilter<Table3.Record>() {

			@Override
			public void nextRecord(Table3.Record _record) {
				this.removeWithCascade();
			}
		});
        Assert.assertEquals(0, table1.getRecordsNumber());
        Assert.assertEquals(0, table1.getRecords().size());

        Assert.assertEquals(0, table3.getRecordsNumber());
        Assert.assertEquals(0, table3.getRecords().size());

		addSecondRecord();
		r1 = table1.getRecords();
		r2 = table3.getRecords();
		Table1.Record rec1=r1.get(0);
		Table3.Record rec2=r2.get(0);
		Assert.assertTrue(table1.removeRecord("pk1", rec1.pk1,"pk2", rec1.pk2,"pk3", rec1.pk3,"pk4", rec1.pk4,"pk5", rec1.pk5,"pk6", rec1.pk6,"pk7", rec1.pk7));
		Assert.assertTrue(table3.removeRecord("pk1", rec2.pk1,"pk2", rec2.pk2,"pk3", rec2.pk3,"pk4", rec2.pk4,"pk5", rec2.pk5,"pk6", rec2.pk6,"pk7", rec2.pk7));
		Assert.assertFalse(table1.removeRecord("pk1", rec1.pk1,"pk2", rec1.pk2,"pk3", rec1.pk3,"pk4", rec1.pk4,"pk5", rec1.pk5,"pk6", rec1.pk6,"pk7", rec1.pk7));
		Assert.assertFalse(table3.removeRecord("pk1", rec2.pk1,"pk2", rec2.pk2,"pk3", rec2.pk3,"pk4", rec2.pk4,"pk5", rec2.pk5,"pk6", rec2.pk6,"pk7", rec2.pk7));
		Assert.assertEquals(0, table1.getRecords().size());
		Assert.assertEquals(0, table1.getRecordsNumber());
		Assert.assertEquals(0, table3.getRecords().size());
		Assert.assertEquals(0, table3.getRecordsNumber());
		try
		{
			Assert.assertTrue(table1.removeRecord("pk1", rec1.pk1,"pk2", rec1.pk2,"pk3", rec1.pk3,"pk4", rec1.pk4,"pk5", rec1.pk5,"pk6", rec1.pk6));
			Assert.fail();
		}
		catch (FieldDatabaseException e)
		{

		}
		try
		{
			Assert.assertTrue(table3.removeRecord("pk1", rec1.pk1,"pk2", rec1.pk2,"pk3", rec1.pk3,"pk4", rec1.pk4,"pk5", rec1.pk5,"pk6", rec1.pk6));
			Assert.fail();
		}
		catch (FieldDatabaseException e)
		{

		}



		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	@Test(dependsOnMethods = { "removeRecords" })
	public void testFilters() throws DatabaseException {
		HashMap<String, Object> map = new HashMap<>();
		map.put("val1", 1);
		map.put("val2", 2);
		map.put("val3", 3);
		table7.addRecord(map);
		HashMap<String, Object> map2 = new HashMap<>();
		map2.put("val1", 0);
		map2.put("val2", 2);
		map2.put("val3", 3);
		table7.addRecord(map2);
		HashMap<String, Object> map3 = new HashMap<>();
		map3.put("val1", 0);
		map3.put("val2", 2);
		map3.put("val3", 4);
		table7.addRecord(map3);

		HashMap<String, Object> mg0 = new HashMap<>();
		mg0.put("val2", 2);
		mg0.put("val3", 4);
		HashMap<String, Object> mg1 = new HashMap<>();
		mg1.put("val2", 2);
		mg1.put("val3", 3);
		assertTrue(table7.hasRecordsWithAllFields(mg1));
		assertTrue(table7.hasRecordsWithOneOfFields(mg1));
		ArrayList<Table7.Record> res = table7.getRecordsWithAllFields(mg1);
        Assert.assertEquals(2, res.size());
        Assert.assertEquals(1, res.get(0).val1);
        Assert.assertEquals(2, res.get(0).val2);
        Assert.assertEquals(3, res.get(0).val3);
        Assert.assertEquals(0, res.get(1).val1);
        Assert.assertEquals(2, res.get(1).val2);
        Assert.assertEquals(3, res.get(1).val3);
		res = table7.getRecordsWithOneOfFields(mg1);
        Assert.assertEquals(3, res.size());
		HashMap<String, Object> mg2 = new HashMap<>();
		mg2.put("val1", 1);
		mg2.put("val3", 4);
		res = table7.getRecordsWithOneOfFields(mg2);
		assertTrue(table7.hasRecordsWithOneOfFields(mg2));
        Assert.assertEquals(2, res.size());
        Assert.assertEquals(1, res.get(0).val1);
        Assert.assertEquals(2, res.get(0).val2);
        Assert.assertEquals(3, res.get(0).val3);
        Assert.assertEquals(0, res.get(1).val1);
        Assert.assertEquals(2, res.get(1).val2);
        Assert.assertEquals(4, res.get(1).val3);

		assertTrue(table7.hasRecordsWithAllFields(mg1, mg0));
		assertTrue(table7.hasRecordsWithOneOfFields(mg1, mg0));
        Assert.assertEquals(3, table7.getRecordsWithAllFields(mg1, mg0).size());
        Assert.assertEquals(3, table7.getRecordsWithOneOfFields(mg1, mg0).size());


        addSecondRecord();
		Table1.Record rec1a=table1.getRecords().get(0);
		rec1a.string_value="string_for_subfield1";
		table1.updateRecord(rec1a);
		List<Table1.Record> l1=table1.getRecordsWithAllFields("subField.string_value", rec1a.subField.string_value);
		Assert.assertEquals(l1.size(), 1);
		Table1.Record rec1b=l1.get(0);
		Assert.assertTrue(table1.equals(rec1a, rec1b));
		l1=table1.getRecordsWithOneOfFields("subField.string_value", rec1a.subField.string_value);
		Assert.assertEquals(l1.size(), 1);
		rec1b=l1.get(0);
		Assert.assertTrue(table1.equals(rec1a, rec1b));
		table1.removeRecordsWithAllFields("subField.string_value", rec1a.subField.string_value);
		Assert.assertEquals(table1.getRecords().size(), 0);

		Table3.Record rec3a=table3.getRecords().get(0);
		rec3a.string_value="string_for_subfield3";
		table3.updateRecord(rec3a);
		List<Table3.Record> l3=table3.getRecordsWithAllFields("subField.string_value", rec3a.subField.string_value);
		Assert.assertEquals(l3.size(), 1);
		Table3.Record rec3b=l3.get(0);
		Assert.assertTrue(table3.equals(rec3a, rec3b));
		l3=table3.getRecordsWithOneOfFields("subField.string_value", rec3a.subField.string_value);
		Assert.assertEquals(l3.size(), 1);
		rec3b=l3.get(0);
		Assert.assertTrue(table3.equals(rec3a, rec3b));
		table3.removeRecordsWithAllFields("subField.string_value", rec3a.subField.string_value);
		Assert.assertEquals(table3.getRecords().size(), 0);


		addSecondRecord();

		rec1a=table1.getRecords().get(0);
		rec1a.string_value="string_for_subfield1";
		table1.updateRecord(rec1a);
		table1.removeRecordsWithOneOfFields("subField.string_value", rec1a.subField.string_value);
		Assert.assertEquals(table1.getRecords().size(), 0);

		rec3a=table3.getRecords().get(0);
		rec3a.string_value="string_for_subfield3";
		table3.updateRecord(rec3a);
		table3.removeRecordsWithOneOfFields("subField.string_value", rec3a.subField.string_value);
		Assert.assertEquals(table3.getRecords().size(), 0);



		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	@Test(dependsOnMethods = { "testFilters" })
	public void testRemoveFilters() throws DatabaseException {
		HashMap<String, Object> mg0 = new HashMap<>();
		mg0.put("val2", 9);
		mg0.put("val3", 9);
		HashMap<String, Object> mg1 = new HashMap<>();
		mg1.put("val2", 2);
		mg1.put("val3", 3);
		HashMap<String, Object> mg2 = new HashMap<>();
		mg2.put("val1", 2);
		mg2.put("val3", 4);

        Assert.assertEquals(0, table7.removeRecordsWithAllFields(mg0));
        Assert.assertEquals(3, table7.getRecordsNumber());
        Assert.assertEquals(0, table7.removeRecordsWithOneOfFields(mg0));
        Assert.assertEquals(3, table7.getRecordsNumber());
        Assert.assertEquals(0, table7.removeRecordsWithAllFields(mg2));
        Assert.assertEquals(3, table7.getRecordsNumber());
        Assert.assertEquals(1, table7.removeRecordsWithOneOfFields(mg2));
        Assert.assertEquals(2, table7.getRecordsNumber());
        Assert.assertEquals(2, table7.removeRecordsWithAllFields(mg1));
        Assert.assertEquals(0, table7.getRecordsNumber());
		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	@Test(dependsOnMethods = { "testRemoveFilters" })
	public void addForeignKeyAndTestUniqueKeys() throws DatabaseException {
		addSecondRecord();
		addSecondRecord();
		Table1.Record r1 = table1.getRecords().get(0);
		Table3.Record r2 = table3.getRecords().get(0);
		Table1.Record r1b = table1.getRecords().get(1);
		Table3.Record r2b = table3.getRecords().get(1);
        Assert.assertEquals(2, table1.getRecordsNumber());
        Assert.assertEquals(2, table3.getRecordsNumber());
        Assert.assertEquals(2, table1.getRecords().size());
        Assert.assertEquals(2, table3.getRecords().size());

		HashMap<String, Object> map1 = new HashMap<>();
		map1.put("fr1_pk1", r1);
		map1.put("int_value", 0);
		HashMap<String, Object> map2 = new HashMap<>();
		map2.put("fr1_pk1", r2);
		map2.put("int_value", 0);
		try {
			table2.addRecord(map2);
			fail();
		} catch (DatabaseException e) {
			assertTrue(true);
		}
		Assert.assertEquals(table4.getRecords().size(), 0);
		Assert.assertEquals(table2.addRecord(map1).int_value, 0);
		Assert.assertEquals(table4.addRecord(map2).int_value, 0);
		Assert.assertEquals(table5.addRecord(map2).int_value, 0);

		Assert.assertEquals(table2.getRecords().get(0).int_value, 0);
		Assert.assertEquals(table5.getRecords().get(0).int_value, 0);
		Assert.assertEquals(table4.getRecords().get(0).int_value, 0);

		Table1.Record r1fr2 = table2.getRecords().get(0).fr1_pk1;
		Table3.Record r2fr4 = table4.getRecords().get(0).fr1_pk1;
		Table3.Record r2fr5 = table5.getRecords().get(0).fr1_pk1;



        Assert.assertEquals(r1.pk1, r1fr2.pk1);
        Assert.assertEquals(r1.pk2, r1fr2.pk2);
        Assert.assertEquals(r1.pk3, r1fr2.pk3);
        Assert.assertEquals(r1.pk4, r1fr2.pk4);
        Assert.assertEquals(r1.pk5, r1fr2.pk5);
		assertTrue(r1.pk6.equals(r1fr2.pk6));
		assertTrue(r1.pk7.equals(r1fr2.pk7));
        Assert.assertEquals(r1.int_value, r1fr2.int_value);
        Assert.assertEquals(r1.byte_value, r1fr2.byte_value);
        Assert.assertEquals(r1.char_value, r1fr2.char_value);
        Assert.assertEquals(r1.boolean_value, r1fr2.boolean_value);
        Assert.assertEquals(r1.short_value, r1fr2.short_value);
        Assert.assertEquals(r1.long_value, r1fr2.long_value);
        Assert.assertEquals(r1.float_value, r1fr2.float_value, 0.0);
        Assert.assertEquals(r1.double_value, r1fr2.double_value, 0.0);
        Assert.assertEquals(r1.string_value, r1fr2.string_value);
        Assert.assertEquals(r1.IntegerNumber_value, r1fr2.IntegerNumber_value);
        Assert.assertEquals(r1.ByteNumber_value, r1fr2.ByteNumber_value);
        Assert.assertEquals(r1.CharacterNumber_value, r1fr2.CharacterNumber_value);
        Assert.assertEquals(r1.BooleanNumber_value, r1fr2.BooleanNumber_value);
        Assert.assertEquals(r1.ShortNumber_value, r1fr2.ShortNumber_value);
        Assert.assertEquals(r1.LongNumber_value, r1fr2.LongNumber_value);
        Assert.assertEquals(r1.FloatNumber_value, r1fr2.FloatNumber_value);
        Assert.assertEquals(r1.DoubleNumber_value, r1fr2.DoubleNumber_value);
        Assert.assertEquals(r1.BigInteger_value, r1fr2.BigInteger_value);
        Assert.assertEquals(r1.BigDecimal_value, r1fr2.BigDecimal_value);
        Assert.assertEquals(r1.DateValue, r1fr2.DateValue);
        Assert.assertEquals(r1.CalendarValue, r1fr2.CalendarValue);
        Assert.assertEquals(r1.secretKey, r1fr2.secretKey);
        Assert.assertEquals(r1.typeSecretKey, r1fr2.typeSecretKey);
        Assert.assertEquals(r1.file, fileTest);
		assertEquals(r1.subField, subField);
		assertEquals(r1.subSubField, subSubField);

        Assert.assertEquals(r2.pk1, r2fr4.pk1);
        Assert.assertEquals(r2.pk2, r2fr4.pk2);
        Assert.assertEquals(r2.pk3, r2fr4.pk3);
        Assert.assertEquals(r2.pk4, r2fr4.pk4);
        Assert.assertEquals(r2.pk5, r2fr4.pk5);
		assertTrue(r2.pk6.equals(r2fr4.pk6));
		assertTrue(r2.pk7.equals(r2fr4.pk7));
        Assert.assertEquals(r2.int_value, r2fr4.int_value);
        Assert.assertEquals(r2.byte_value, r2fr4.byte_value);
        Assert.assertEquals(r2.char_value, r2fr4.char_value);
        Assert.assertEquals(r2.boolean_value, r2fr4.boolean_value);
        Assert.assertEquals(r2.short_value, r2fr4.short_value);
        Assert.assertEquals(r2.long_value, r2fr4.long_value);
        Assert.assertEquals(r2.float_value, r2fr4.float_value, 0.0);
        Assert.assertEquals(r2.double_value, r2fr4.double_value, 0.0);
        Assert.assertEquals(r2.string_value, r2fr4.string_value);
        Assert.assertEquals(r2.IntegerNumber_value, r2fr4.IntegerNumber_value);
        Assert.assertEquals(r2.ByteNumber_value, r2fr4.ByteNumber_value);
        Assert.assertEquals(r2.CharacterNumber_value, r2fr4.CharacterNumber_value);
        Assert.assertEquals(r2.BooleanNumber_value, r2fr4.BooleanNumber_value);
        Assert.assertEquals(r2.ShortNumber_value, r2fr4.ShortNumber_value);
        Assert.assertEquals(r2.LongNumber_value, r2fr4.LongNumber_value);
        Assert.assertEquals(r2.FloatNumber_value, r2fr4.FloatNumber_value);
        Assert.assertEquals(r2.DoubleNumber_value, r2fr4.DoubleNumber_value);
        Assert.assertEquals(r2.BigInteger_value, r2fr4.BigInteger_value);
        Assert.assertEquals(r2.BigDecimal_value, r2fr4.BigDecimal_value);
        Assert.assertEquals(r2.DateValue, r2fr4.DateValue);
        Assert.assertEquals(r2.CalendarValue, r2fr4.CalendarValue);
        Assert.assertEquals(r2.secretKey, r2fr4.secretKey);
        Assert.assertEquals(r2.typeSecretKey, r2fr4.typeSecretKey);
        Assert.assertEquals(r2.file, fileTest);
		assertEquals(r2.subField, subField);
		assertEquals(r2.subSubField, subSubField);

        Assert.assertEquals(r2.pk1, r2fr5.pk1);
        Assert.assertEquals(r2.pk2, r2fr5.pk2);
        Assert.assertEquals(r2.pk3, r2fr5.pk3);
        Assert.assertEquals(r2.pk4, r2fr5.pk4);
        Assert.assertEquals(r2.pk5, r2fr5.pk5);
		assertTrue(r2.pk6.equals(r2fr5.pk6));
		assertTrue(r2.pk7.equals(r2fr5.pk7));
        Assert.assertEquals(r2.int_value, r2fr5.int_value);
        Assert.assertEquals(r2.byte_value, r2fr5.byte_value);
        Assert.assertEquals(r2.char_value, r2fr5.char_value);
        Assert.assertEquals(r2.boolean_value, r2fr5.boolean_value);
        Assert.assertEquals(r2.short_value, r2fr5.short_value);
        Assert.assertEquals(r2.long_value, r2fr5.long_value);
        Assert.assertEquals(r2.float_value, r2fr5.float_value, 0.0);
        Assert.assertEquals(r2.double_value, r2fr5.double_value, 0.0);
        Assert.assertEquals(r2.string_value, r2fr5.string_value);
        Assert.assertEquals(r2.IntegerNumber_value, r2fr5.IntegerNumber_value);
        Assert.assertEquals(r2.ByteNumber_value, r2fr5.ByteNumber_value);
        Assert.assertEquals(r2.CharacterNumber_value, r2fr5.CharacterNumber_value);
        Assert.assertEquals(r2.BooleanNumber_value, r2fr5.BooleanNumber_value);
        Assert.assertEquals(r2.ShortNumber_value, r2fr5.ShortNumber_value);
        Assert.assertEquals(r2.LongNumber_value, r2fr5.LongNumber_value);
        Assert.assertEquals(r2.FloatNumber_value, r2fr5.FloatNumber_value);
        Assert.assertEquals(r2.DoubleNumber_value, r2fr5.DoubleNumber_value);
        Assert.assertEquals(r2.BigInteger_value, r2fr5.BigInteger_value);
        Assert.assertEquals(r2.BigDecimal_value, r2fr5.BigDecimal_value);
        Assert.assertEquals(r2.DateValue, r2fr5.DateValue);
        Assert.assertEquals(r2.CalendarValue, r2fr5.CalendarValue);
        Assert.assertEquals(r2.secretKey, r2fr5.secretKey);
        Assert.assertEquals(r2.typeSecretKey, r2fr5.typeSecretKey);
        Assert.assertEquals(r2.file, fileTest);
		assertEquals(r2.subField, subField);
		assertEquals(r2.subSubField, subSubField);

		HashMap<String, Object> map1b = new HashMap<>();
		map1b.put("fr1_pk1", r1);
		map1b.put("int_value", 1);
		HashMap<String, Object> map2b = new HashMap<>();
		map2b.put("fr1_pk1", r2);
		map2b.put("int_value", 1);
		try {
			table2.addRecord(map1b);
            fail();
		} catch (DatabaseException e) {
			assertTrue(true);
		}
		try {
			table4.addRecord(map2b);
            fail();
		} catch (DatabaseException e) {
			assertTrue(true);
		}
		try {
			table5.addRecord(map2b);
            fail();
		} catch (DatabaseException e) {
			assertTrue(true);
		}
		HashMap<String, Object> map1c = new HashMap<>();
		map1c.put("fr1_pk1", r1b);
		map1c.put("int_value", 0);
		HashMap<String, Object> map2c = new HashMap<>();
		map2c.put("fr1_pk1", r2b);
		map2c.put("int_value", 0);
		try {
			table2.addRecord(map1c);
            fail();
		} catch (DatabaseException e) {
			assertTrue(true);
		}
		try {
			table4.addRecord(map2c);
            fail();
		} catch (DatabaseException e) {
			assertTrue(true);
		}
		try {
			table5.addRecord(map2c);
            fail();
		} catch (DatabaseException e) {
			assertTrue(true);
		}

        Assert.assertEquals(1, table2.getRecordsNumber());
        Assert.assertEquals(1, table4.getRecordsNumber());
        Assert.assertEquals(1, table5.getRecordsNumber());
		HashMap<String, Object> map6 = new HashMap<>();
		map6.put("fk1_pk1", table2.getRecords().get(0));
		map6.put("fk2", table5.getRecords().get(0));
		Table6.Record r6 = table6.addRecord(map6);
		assertTrue(equals(r6.fk1_pk1, table2.getRecords().get(0)));
		assertTrue(equals(r6.fk2, table5.getRecords().get(0)));
        Assert.assertEquals(1, table6.getRecordsNumber());
		r6 = table6.getRecords().get(0);
		assertTrue(equals(r6.fk1_pk1, table2.getRecords().get(0)));
		assertTrue(equals(r6.fk2, table5.getRecords().get(0)));

		try {
			table6.addRecord(map6);
            fail();
		} catch (DatabaseException e) {
			assertTrue(true);
		}
		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	@Test(dependsOnMethods = { "addForeignKeyAndTestUniqueKeys" })
	public void alterRecordWithCascade() throws DatabaseException, NoSuchAlgorithmException, NoSuchProviderException {
		HashMap<String, Object> map = new HashMap<>();
		map.put("pk1", 10);
		map.put("pk2", 1526345L);
		BigInteger val;
		do {
			val = BigInteger.valueOf(random.nextLong());
			if (val.longValue() < 0)
				val = null;
			else if (table1.getRecords().get(0).pk3.equals(val))
				val = null;
			else if (table1.getRecords().get(1).pk3.equals(val))
				val = null;
			else if (table3.getRecords().get(0).pk3.equals(val))
				val = null;
			else if (table3.getRecords().get(1).pk3.equals(val))
				val = null;
		} while (val == null);
		map.put("pk3", val);
		map.put("pk5", new SecuredDecentralizedID(new RenforcedDecentralizedIDGenerator(),
				SecureRandomType.DEFAULT.getSingleton(null)));
		map.put("pk6", new DecentralizedIDGenerator());
		map.put("pk7", new RenforcedDecentralizedIDGenerator());
		map.put("byte_value", (byte) 9);
		map.put("char_value", 's');
		map.put("DoubleNumber_value", 7.7);
		map.put("subField", subField);
		map.put("subSubField", subSubField);
		byte[] tab = new byte[3];
		tab[0] = 7;
		tab[1] = 8;
		tab[2] = 9;
		map.put("byte_array_value", tab);

		Table1.Record r1 = table1.getRecords().get(0);
		table1.updateRecord(r1, map);
		table1.updateRecord(r1, map);
		Table3.Record r2 = table3.getRecords().get(0);
		table3.updateRecord(r2, map);
		table3.updateRecord(r2, map);

		Table1.Record r1a = table1.getRecords().get(0);
		Table3.Record r2a = table3.getRecords().get(0);

		map.remove("pk2");
		map.remove("pk3");
		map.remove("pk5");
		map.remove("pk6");
		map.remove("pk7");
		map.put("pk3", table1.getRecords().get(1).pk3);
		try {
			table1.updateRecord(r1a, map);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		map.remove("pk3");
		map.put("pk3", table3.getRecords().get(1).pk3);
		try {
			table3.updateRecord(r2a, map);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}

		r1a = table1.getRecords().get(0);
		r2a = table3.getRecords().get(0);

		map.remove("pk3");
		map.put("pk2", table1.getRecords().get(1).pk2);
		try {
			table1.updateRecord(r1a, map);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		map.put("pk2", table3.getRecords().get(1).pk2);
		try {
			table3.updateRecord(r2a, map);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}

        Assert.assertEquals(10, r1.pk1);
        Assert.assertEquals(3, r1.int_value);
        Assert.assertEquals(r1.byte_value, (byte) 9);
        Assert.assertEquals('s', r1.char_value);
		assertTrue(r1.boolean_value);
        Assert.assertEquals(r1.short_value, (short) 3);
        Assert.assertEquals(r1.long_value, 3);
        Assert.assertEquals(3.3f, r1.float_value, 0.0);
        Assert.assertEquals(3.3, r1.double_value, 0.0);
        Assert.assertEquals("test string", r1.string_value);
        Assert.assertEquals(3, r1.IntegerNumber_value.intValue());
        Assert.assertEquals(r1.ByteNumber_value.byteValue(), (byte) 3);
        Assert.assertEquals('x', r1.CharacterNumber_value.charValue());
		assertTrue(r1.BooleanNumber_value);
        Assert.assertEquals(r1.ShortNumber_value.shortValue(), (short) 3);
        Assert.assertEquals(r1.LongNumber_value.longValue(), 3);
        Assert.assertEquals(3.3f, r1.FloatNumber_value, 0.0);
        Assert.assertEquals(7.7, r1.DoubleNumber_value, 0.0);
        Assert.assertEquals(r1.BigInteger_value, new BigInteger("5"));
        Assert.assertEquals(r1.BigDecimal_value, new BigDecimal("8.8"));
        Assert.assertEquals(r1.DateValue, date);
        Assert.assertEquals(r1.CalendarValue, calendar);
        Assert.assertEquals(r1.secretKey, secretKey);
        Assert.assertEquals(r1.typeSecretKey, typeSecretKey);
        Assert.assertEquals(r1.file, fileTest);
		assertEquals(r1.subField, subField);
		assertEquals(r1.subSubField, subSubField);

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(r1.byte_array_value[i], tab[i]);

        Assert.assertEquals(10, r2.pk1);
        Assert.assertEquals(3, r2.int_value);
        Assert.assertEquals(r2.byte_value, (byte) 9);
        Assert.assertEquals('s', r2.char_value);
		assertTrue(r2.boolean_value);
        Assert.assertEquals(r2.short_value, (short) 3);
        Assert.assertEquals(r2.long_value, 3);
        Assert.assertEquals(3.3f, r2.float_value, 0.0);
        Assert.assertEquals(3.3, r2.double_value, 0.0);
        Assert.assertEquals("test string", r2.string_value);
        Assert.assertEquals(3, r2.IntegerNumber_value.intValue());
        Assert.assertEquals(r2.ByteNumber_value.byteValue(), (byte) 3);
        Assert.assertEquals('x', r2.CharacterNumber_value.charValue());
		assertTrue(r2.BooleanNumber_value);
        Assert.assertEquals(r2.ShortNumber_value.shortValue(), (short) 3);
        Assert.assertEquals(r2.LongNumber_value.longValue(), 3);
        Assert.assertEquals(3.3f, r2.FloatNumber_value, 0.0);
        Assert.assertEquals(7.7, r2.DoubleNumber_value, 0.0);
        Assert.assertEquals(r2.BigInteger_value, new BigInteger("5"));
        Assert.assertEquals(r2.BigDecimal_value, new BigDecimal("8.8"));
        Assert.assertEquals(r2.DateValue, date);
        Assert.assertEquals(r2.CalendarValue, calendar);
        Assert.assertEquals(r2.secretKey, secretKey);
        Assert.assertEquals(r2.typeSecretKey, typeSecretKey);
        Assert.assertEquals(r2.file, fileTest);
		assertEquals(r2.subField, subField);
		assertEquals(r2.subSubField, subSubField);

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(r2.byte_array_value[i], tab[i]);

		Table1.Record ra = table2.getRecords().get(0).fr1_pk1;
		Table3.Record rb = table4.getRecords().get(0).fr1_pk1;
		Table3.Record rc = table5.getRecords().get(0).fr1_pk1;
		Table1.Record rd = table6.getRecords().get(0).fk1_pk1.fr1_pk1;
		Table3.Record re = table6.getRecords().get(0).fk2.fr1_pk1;

        Assert.assertEquals(r1.pk1, ra.pk1);
        Assert.assertEquals(r1.pk2, ra.pk2);
        Assert.assertEquals(r1.pk3, ra.pk3);
        Assert.assertEquals(r1.pk4, ra.pk4);
        Assert.assertEquals(r1.pk5, ra.pk5);
		assertTrue(r1.pk6.equals(ra.pk6));
		assertTrue(r1.pk7.equals(ra.pk7));
        Assert.assertEquals(r1.int_value, ra.int_value);
        Assert.assertEquals(r1.byte_value, ra.byte_value);
        Assert.assertEquals(r1.char_value, ra.char_value);
        Assert.assertEquals(r1.boolean_value, ra.boolean_value);
        Assert.assertEquals(r1.short_value, ra.short_value);
        Assert.assertEquals(r1.long_value, ra.long_value);
        Assert.assertEquals(r1.float_value, ra.float_value, 0.0);
        Assert.assertEquals(r1.double_value, ra.double_value, 0.0);
        Assert.assertEquals(r1.string_value, ra.string_value);
        Assert.assertEquals(r1.IntegerNumber_value, ra.IntegerNumber_value);
        Assert.assertEquals(r1.ByteNumber_value, ra.ByteNumber_value);
        Assert.assertEquals(r1.CharacterNumber_value, ra.CharacterNumber_value);
        Assert.assertEquals(r1.BooleanNumber_value, ra.BooleanNumber_value);
        Assert.assertEquals(r1.ShortNumber_value, ra.ShortNumber_value);
        Assert.assertEquals(r1.LongNumber_value, ra.LongNumber_value);
        Assert.assertEquals(r1.FloatNumber_value, ra.FloatNumber_value);
        Assert.assertEquals(r1.DoubleNumber_value, ra.DoubleNumber_value);
        Assert.assertEquals(r1.BigInteger_value, ra.BigInteger_value);
        Assert.assertEquals(r1.BigDecimal_value, ra.BigDecimal_value);
        Assert.assertEquals(r1.DateValue, ra.DateValue);
        Assert.assertEquals(r1.CalendarValue, ra.CalendarValue);
        Assert.assertEquals(r1.secretKey, ra.secretKey);
        Assert.assertEquals(r1.typeSecretKey, ra.typeSecretKey);
        Assert.assertEquals(r1.file, ra.file);
		assertEquals(r1.subField, subField);
		assertEquals(r1.subSubField, subSubField);

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(ra.byte_array_value[i], tab[i]);

        Assert.assertEquals(r1.pk1, rd.pk1);
        Assert.assertEquals(r1.pk2, rd.pk2);
        Assert.assertEquals(r1.pk3, rd.pk3);
        Assert.assertEquals(r1.pk4, rd.pk4);
        Assert.assertEquals(r1.pk5, rd.pk5);
		assertTrue(r1.pk6.equals(rd.pk6));
		assertTrue(r1.pk7.equals(rd.pk7));
        Assert.assertEquals(r1.int_value, rd.int_value);
        Assert.assertEquals(r1.byte_value, rd.byte_value);
        Assert.assertEquals(r1.char_value, rd.char_value);
        Assert.assertEquals(r1.boolean_value, rd.boolean_value);
        Assert.assertEquals(r1.short_value, rd.short_value);
        Assert.assertEquals(r1.long_value, rd.long_value);
        Assert.assertEquals(r1.float_value, rd.float_value, 0.0);
        Assert.assertEquals(r1.double_value, rd.double_value, 0.0);
        Assert.assertEquals(r1.string_value, rd.string_value);
        Assert.assertEquals(r1.IntegerNumber_value, rd.IntegerNumber_value);
        Assert.assertEquals(r1.ByteNumber_value, rd.ByteNumber_value);
        Assert.assertEquals(r1.CharacterNumber_value, rd.CharacterNumber_value);
        Assert.assertEquals(r1.BooleanNumber_value, rd.BooleanNumber_value);
        Assert.assertEquals(r1.ShortNumber_value, rd.ShortNumber_value);
        Assert.assertEquals(r1.LongNumber_value, rd.LongNumber_value);
        Assert.assertEquals(r1.FloatNumber_value, rd.FloatNumber_value);
        Assert.assertEquals(r1.DoubleNumber_value, rd.DoubleNumber_value);
        Assert.assertEquals(r1.BigInteger_value, rd.BigInteger_value);
        Assert.assertEquals(r1.BigDecimal_value, rd.BigDecimal_value);
        Assert.assertEquals(r1.DateValue, rd.DateValue);
        Assert.assertEquals(r1.CalendarValue, rd.CalendarValue);
        Assert.assertEquals(r1.secretKey, rd.secretKey);
        Assert.assertEquals(r1.typeSecretKey, rd.typeSecretKey);
        Assert.assertEquals(r2.file, rd.file);
		assertEquals(r1.subField, subField);
		assertEquals(r1.subSubField, subSubField);

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(rd.byte_array_value[i], tab[i]);

        Assert.assertEquals(r2.pk1, rb.pk1);
        Assert.assertEquals(r2.pk2, rb.pk2);
        Assert.assertEquals(r2.pk3, rb.pk3);
        Assert.assertEquals(r2.pk4, rb.pk4);
        Assert.assertEquals(r2.pk5, rb.pk5);
		assertTrue(r2.pk6.equals(rb.pk6));
		assertTrue(r2.pk7.equals(rb.pk7));
        Assert.assertEquals(r2.int_value, rb.int_value);
        Assert.assertEquals(r2.byte_value, rb.byte_value);
        Assert.assertEquals(r2.char_value, rb.char_value);
        Assert.assertEquals(r2.boolean_value, rb.boolean_value);
        Assert.assertEquals(r2.short_value, rb.short_value);
        Assert.assertEquals(r2.long_value, rb.long_value);
        Assert.assertEquals(r2.float_value, rb.float_value, 0.0);
        Assert.assertEquals(r2.double_value, rb.double_value, 0.0);
        Assert.assertEquals(r2.string_value, rb.string_value);
        Assert.assertEquals(r2.IntegerNumber_value, rb.IntegerNumber_value);
        Assert.assertEquals(r2.ByteNumber_value, rb.ByteNumber_value);
        Assert.assertEquals(r2.CharacterNumber_value, rb.CharacterNumber_value);
        Assert.assertEquals(r2.BooleanNumber_value, rb.BooleanNumber_value);
        Assert.assertEquals(r2.ShortNumber_value, rb.ShortNumber_value);
        Assert.assertEquals(r2.LongNumber_value, rb.LongNumber_value);
        Assert.assertEquals(r2.FloatNumber_value, rb.FloatNumber_value);
        Assert.assertEquals(r2.DoubleNumber_value, rb.DoubleNumber_value);
        Assert.assertEquals(r2.BigInteger_value, rb.BigInteger_value);
        Assert.assertEquals(r2.BigDecimal_value, rb.BigDecimal_value);
        Assert.assertEquals(r2.DateValue, rb.DateValue);
        Assert.assertEquals(r2.CalendarValue, rb.CalendarValue);
        Assert.assertEquals(r2.secretKey, rb.secretKey);
        Assert.assertEquals(r2.typeSecretKey, rb.typeSecretKey);
        Assert.assertEquals(r2.file, rb.file);

		for (int i = 0; i < 3; i++)
            Assert.assertEquals(rb.byte_array_value[i], tab[i]);

        Assert.assertEquals(r2.pk1, rc.pk1);
        Assert.assertEquals(r2.pk2, rc.pk2);
        Assert.assertEquals(r2.pk3, rc.pk3);
        Assert.assertEquals(r2.pk4, rc.pk4);
        Assert.assertEquals(r2.pk5, rc.pk5);
		assertTrue(r2.pk6.equals(rc.pk6));
		assertTrue(r2.pk7.equals(rc.pk7));
        Assert.assertEquals(r2.int_value, rc.int_value);
        Assert.assertEquals(r2.byte_value, rc.byte_value);
        Assert.assertEquals(r2.char_value, rc.char_value);
        Assert.assertEquals(r2.boolean_value, rc.boolean_value);
        Assert.assertEquals(r2.short_value, rc.short_value);
        Assert.assertEquals(r2.long_value, rc.long_value);
        Assert.assertEquals(r2.float_value, rc.float_value, 0.0);
        Assert.assertEquals(r2.double_value, rc.double_value, 0.0);
        Assert.assertEquals(r2.string_value, rc.string_value);
        Assert.assertEquals(r2.IntegerNumber_value, rc.IntegerNumber_value);
        Assert.assertEquals(r2.ByteNumber_value, rc.ByteNumber_value);
        Assert.assertEquals(r2.CharacterNumber_value, rc.CharacterNumber_value);
        Assert.assertEquals(r2.BooleanNumber_value, rc.BooleanNumber_value);
        Assert.assertEquals(r2.ShortNumber_value, rc.ShortNumber_value);
        Assert.assertEquals(r2.LongNumber_value, rc.LongNumber_value);
        Assert.assertEquals(r2.FloatNumber_value, rc.FloatNumber_value);
        Assert.assertEquals(r2.DoubleNumber_value, rc.DoubleNumber_value);
        Assert.assertEquals(r2.BigInteger_value, rc.BigInteger_value);
        Assert.assertEquals(r2.BigDecimal_value, rc.BigDecimal_value);
        Assert.assertEquals(r2.DateValue, rc.DateValue);
        Assert.assertEquals(r2.CalendarValue, rc.CalendarValue);
        Assert.assertEquals(r1.secretKey, rc.secretKey);
        Assert.assertEquals(r1.typeSecretKey, rc.typeSecretKey);
        Assert.assertEquals(r2.file, rc.file);
		for (int i = 0; i < 3; i++)
            Assert.assertEquals(rc.byte_array_value[i], tab[i]);

        Assert.assertEquals(r2.pk1, re.pk1);
        Assert.assertEquals(r2.pk2, re.pk2);
        Assert.assertEquals(r2.pk3, re.pk3);
        Assert.assertEquals(r2.pk4, re.pk4);
        Assert.assertEquals(r2.pk5, re.pk5);
		assertTrue(r2.pk6.equals(re.pk6));
		assertTrue(r2.pk7.equals(re.pk7));
        Assert.assertEquals(r2.int_value, re.int_value);
        Assert.assertEquals(r2.byte_value, re.byte_value);
        Assert.assertEquals(r2.char_value, re.char_value);
        Assert.assertEquals(r2.boolean_value, re.boolean_value);
        Assert.assertEquals(r2.short_value, re.short_value);
        Assert.assertEquals(r2.long_value, re.long_value);
        Assert.assertEquals(r2.float_value, re.float_value, 0.0);
        Assert.assertEquals(r2.double_value, re.double_value, 0.0);
        Assert.assertEquals(r2.string_value, re.string_value);
        Assert.assertEquals(r2.IntegerNumber_value, re.IntegerNumber_value);
        Assert.assertEquals(r2.ByteNumber_value, re.ByteNumber_value);
        Assert.assertEquals(r2.CharacterNumber_value, re.CharacterNumber_value);
        Assert.assertEquals(r2.BooleanNumber_value, re.BooleanNumber_value);
        Assert.assertEquals(r2.ShortNumber_value, re.ShortNumber_value);
        Assert.assertEquals(r2.LongNumber_value, re.LongNumber_value);
        Assert.assertEquals(r2.FloatNumber_value, re.FloatNumber_value);
        Assert.assertEquals(r2.DoubleNumber_value, re.DoubleNumber_value);
        Assert.assertEquals(r2.BigInteger_value, re.BigInteger_value);
        Assert.assertEquals(r2.BigDecimal_value, re.BigDecimal_value);
        Assert.assertEquals(r2.DateValue, re.DateValue);
        Assert.assertEquals(r2.CalendarValue, re.CalendarValue);
        Assert.assertEquals(r2.secretKey, re.secretKey);
        Assert.assertEquals(r2.typeSecretKey, re.typeSecretKey);
        Assert.assertEquals(r2.file, re.file);
		for (int i = 0; i < 3; i++)
            Assert.assertEquals(re.byte_array_value[i], tab[i]);

		Table2.Record t2 = table2.getRecords().get(0);
		HashMap<String, Object> t2map = new HashMap<>();
		t2map.put("fr1_pk1", table1.getRecords().get(1));
		table2.updateRecord(t2, t2map);

		Table2.Record t2bis = table6.getRecords().get(0).fk1_pk1;
		Table1.Record t1 = table1.getRecords().get(1);

        Assert.assertEquals(t2.fr1_pk1.pk1, t2bis.fr1_pk1.pk1);
        Assert.assertEquals(t2.fr1_pk1.pk2, t2bis.fr1_pk1.pk2);
        Assert.assertEquals(t2.fr1_pk1.pk4, t2bis.fr1_pk1.pk4);

        Assert.assertEquals(t1.pk1, t2bis.fr1_pk1.pk1);
        Assert.assertEquals(t1.pk2, t2bis.fr1_pk1.pk2);
        Assert.assertEquals(t1.pk4, t2bis.fr1_pk1.pk4);

		HashMap<String, Object> t2map2;
        t2map2 = new HashMap<>();
        t2map2.put("int_value", t2.int_value);

		table2.updateRecord(t2, t2map2);
		t2map2.put("fr1_pk1", table1.getRecords().get(1));

		try {
			table2.addRecord(t2map2);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		t2map.put("int_value", t2.int_value + 1);
		try {
			table2.addRecord(t2map);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		t2map.remove("fr1_pk1");
		t2map.put("fr1_pk1", table1.getRecords().get(0));
		table2.addRecord(t2map);
		t2map.remove("fr1_pk1");
		try {
			table2.updateRecord(t2, t2map);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}

		table1.updateRecords(new AlterRecordFilter<Table1.Record>() {

			@Override
			public void nextRecord(Record _record) {
				HashMap<String, Object> m = new HashMap<>();
				m.put("int_value", 15);
				this.update(m);
			}
		});

		for (Table1.Record r : table1.getRecords())
            Assert.assertEquals(15, r.int_value);

		table3.updateRecords(new AlterRecordFilter<Table3.Record>() {

			@Override
			public void nextRecord(Table3.Record _record) {
				HashMap<String, Object> m = new HashMap<>();
				m.put("int_value", 15);

				this.update(m);
			}
		});

		for (Table3.Record r : table3.getRecords())
            Assert.assertEquals(15, r.int_value);

		try {
			table1.updateRecords(new AlterRecordFilter<Table1.Record>() {

				@Override
				public void nextRecord(Record _record) {
					HashMap<String, Object> m = new HashMap<>();
					m.put("pk1", 15);
					this.update(m);
				}
			});
            fail();
		} catch (FieldDatabaseException e) {
			assertTrue(true);
		}
		try {
			table3.updateRecords(new AlterRecordFilter<Table3.Record>() {

				@Override
				public void nextRecord(Table3.Record _record) {
					HashMap<String, Object> m = new HashMap<>();
					m.put("pk1", 15);
					this.update(m);
				}
			});
            fail();
		} catch (FieldDatabaseException e) {
			assertTrue(true);
		}
		try {
			table2.updateRecords(new AlterRecordFilter<Table2.Record>() {

				@Override
				public void nextRecord(Table2.Record _record) {
					HashMap<String, Object> m = new HashMap<>();
					m.put("int_value", 15);
					this.update(m);
				}
			});
            fail();
		} catch (FieldDatabaseException e) {
			assertTrue(true);
		}

		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	private boolean equals(DatabaseRecord _instance1, DatabaseRecord _instance2) throws DatabaseException {
		if (_instance1 == null || _instance2 == null)
			return _instance1 == _instance2;
		if (_instance1 == _instance2)
			return true;
		Table<?> t = sql_db.getTableInstance(Table.getTableClass(_instance1.getClass()));

		for (FieldAccessor fa : t.getPrimaryKeysFieldAccessors()) {
			if (!fa.equals(_instance1, fa.getValue(_instance2)))
				return false;
		}
		return true;

	}

	@Test(dependsOnMethods = { "alterRecordWithCascade" })
	public void removePointedRecords() throws DatabaseException {
        Assert.assertEquals(2, table1.getRecordsNumber());
        Assert.assertEquals(2, table3.getRecordsNumber());
        Assert.assertEquals(2, table2.getRecordsNumber());
        Assert.assertEquals(1, table4.getRecordsNumber());
        Assert.assertEquals(1, table5.getRecordsNumber());
        Assert.assertEquals(2, table1.getRecords().size());
        Assert.assertEquals(2, table2.getRecords().size());
        Assert.assertEquals(2, table3.getRecords().size());
        Assert.assertEquals(1, table4.getRecords().size());
        Assert.assertEquals(1, table5.getRecords().size());
        Assert.assertEquals(1, table6.getRecords().size());

		table1.removeRecords(new Filter<Table1.Record>() {

			@Override
			public boolean nextRecord(Record _record) {
				return true;
			}
		});
		table3.removeRecords(new Filter<Table3.Record>() {

			@Override
			public boolean nextRecord(Table3.Record _record) {
				return true;
			}
		});

        Assert.assertEquals(2, table1.getRecordsNumber());
        Assert.assertEquals(1, table3.getRecordsNumber());
        Assert.assertEquals(2, table2.getRecordsNumber());
        Assert.assertEquals(1, table4.getRecordsNumber());
        Assert.assertEquals(1, table5.getRecordsNumber());
        Assert.assertEquals(2, table1.getRecords().size());
        Assert.assertEquals(2, table2.getRecords().size());
        Assert.assertEquals(1, table3.getRecords().size());
        Assert.assertEquals(1, table4.getRecords().size());
        Assert.assertEquals(1, table5.getRecords().size());

		try {
			table1.removeRecords(table1.getRecords());
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		try {
			table3.removeRecords(table3.getRecords());
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
        Assert.assertEquals(2, table1.getRecordsNumber());
        Assert.assertEquals(1, table3.getRecordsNumber());
        Assert.assertEquals(2, table1.getRecords().size());
        Assert.assertEquals(1, table3.getRecords().size());
		try {
			table1.removeRecord(table1.getRecords().get(0));
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		try {
			table3.removeRecord(table3.getRecords().get(0));
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	@Test(dependsOnMethods = { "removePointedRecords" })
	public void removeForeignKeyRecords() throws DatabaseException {
		table1.removeRecords(new Filter<Table1.Record>() {

			@Override
			public boolean nextRecord(com.distrimind.ood.database.database.Table1.Record _record) {
				return true;
			}
		});
		table3.removeRecords(new Filter<Table3.Record>() {

			@Override
			public boolean nextRecord(com.distrimind.ood.database.database.Table3.Record _record) {
				return true;
			}
		});
		table2.removeRecords(new Filter<Table2.Record>() {

			@Override
			public boolean nextRecord(com.distrimind.ood.database.database.Table2.Record _record) {
				return true;
			}
		});
		table4.removeRecords(new Filter<Table4.Record>() {

			@Override
			public boolean nextRecord(com.distrimind.ood.database.database.Table4.Record _record) {
				return true;
			}
		});
		table5.removeRecords(new Filter<Table5.Record>() {

			@Override
			public boolean nextRecord(com.distrimind.ood.database.database.Table5.Record _record) {
				return true;
			}
		});
        Assert.assertEquals(2, table1.getRecordsNumber());
        Assert.assertEquals(1, table3.getRecordsNumber());
        Assert.assertEquals(1, table2.getRecordsNumber());
        Assert.assertEquals(0, table4.getRecordsNumber());
        Assert.assertEquals(1, table5.getRecordsNumber());
        Assert.assertEquals(2, table1.getRecords().size());
        Assert.assertEquals(1, table3.getRecords().size());
        Assert.assertEquals(1, table2.getRecords().size());
        Assert.assertEquals(0, table4.getRecords().size());
        Assert.assertEquals(1, table5.getRecords().size());

		table6.removeRecords(new Filter<Table6.Record>() {

			@Override
			public boolean nextRecord(com.distrimind.ood.database.database.Table6.Record _record) {
				return true;
			}
		});
        Assert.assertEquals(0, table6.getRecordsNumber());
		HashMap<String, Object> map6 = new HashMap<>();
		map6.put("fk1_pk1", table2.getRecords().get(0));
		map6.put("fk2", table5.getRecords().get(0));
		table6.addRecord(map6);
		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	@Test(invocationCount = 0)
	private void addRecords() throws DatabaseException {
		addSecondRecord();
		addSecondRecord();
		Table1.Record r1 = table1.getRecords().get(0);
		Table3.Record r2 = table3.getRecords().get(0);
		HashMap<String, Object> map1 = new HashMap<>();
		map1.put("fr1_pk1", r1);
		map1.put("int_value", 0);
		HashMap<String, Object> map2 = new HashMap<>();
		map2.put("fr1_pk1", r2);
		map2.put("int_value", 0);
		Table2.Record r22 = table2.addRecord(map1);
		table4.addRecord(map2);
		Table5.Record r55 = table5.addRecord(map2);

		HashMap<String, Object> map6 = new HashMap<>();
		map6.put("fk1_pk1", r22);
		map6.put("fk2", r55);
		table6.addRecord(map6);
		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	@Test(dependsOnMethods = { "removeForeignKeyRecords" })
	public void testIsPointed() throws DatabaseException {
		boolean b1 = false;
		Table1.Record r1 = table1.getRecords().get(0);
		for (Table2.Record r2 : table2.getRecords()) {
			if (table1.equals(r2.fr1_pk1, r1)) {
				b1 = true;
				break;
			}
		}
		boolean b3 = false;
		Table3.Record r3 = table3.getRecords().get(0);
		for (Table4.Record r4 : table4.getRecords()) {
			if (table3.equals(r4.fr1_pk1, r3)) {
				b3 = true;
				break;
			}
		}
		for (Table5.Record r5 : table5.getRecords()) {
			if (table3.equals(r5.fr1_pk1, r3)) {
				b3 = true;
				break;
			}
		}
        Assert.assertEquals(table1.isRecordPointedByForeignKeys(r1), b1);
        Assert.assertEquals(table3.isRecordPointedByForeignKeys(r3), b3);
		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	@Test(dependsOnMethods = { "testIsPointed" })
	public void removeWithCascade() throws DatabaseException {
		table2.removeRecordsWithCascade(new Filter<Table2.Record>() {

			@Override
			public boolean nextRecord(com.distrimind.ood.database.database.Table2.Record _record) {
				return true;
			}
		});
		table5.removeRecordsWithCascade(new Filter<Table5.Record>() {

			@Override
			public boolean nextRecord(com.distrimind.ood.database.database.Table5.Record _record) {
				return true;
			}
		});
		Assert.assertEquals(table2.getRecordsNumber(), 0);
		Assert.assertEquals(table5.getRecordsNumber(), 0);
		Assert.assertEquals(table6.getRecordsNumber(), 0);
		Assert.assertEquals(table2.getRecords().size(), 0);
		Assert.assertEquals(table5.getRecords().size(), 0);
		Assert.assertEquals(table6.getRecords().size(), 0);

		addRecords();
		long oldSize1=table1.getRecordsNumber();
		long oldSize2=table3.getRecordsNumber();
		Table1.Record rec1=table1.getRecords().get(0);
		Table3.Record rec2=table3.getRecords().get(0);
		Assert.assertTrue(table1.removeRecordWithCascade("pk1", rec1.pk1,"pk2", rec1.pk2,"pk3", rec1.pk3,"pk4", rec1.pk4,"pk5", rec1.pk5,"pk6", rec1.pk6,"pk7", rec1.pk7));
		Assert.assertTrue(table3.removeRecordWithCascade("pk1", rec2.pk1,"pk2", rec2.pk2,"pk3", rec2.pk3,"pk4", rec2.pk4,"pk5", rec2.pk5,"pk6", rec2.pk6,"pk7", rec2.pk7));
		Assert.assertFalse(table1.removeRecordWithCascade("pk1", rec1.pk1,"pk2", rec1.pk2,"pk3", rec1.pk3,"pk4", rec1.pk4,"pk5", rec1.pk5,"pk6", rec1.pk6,"pk7", rec1.pk7));
		Assert.assertFalse(table3.removeRecordWithCascade("pk1", rec2.pk1,"pk2", rec2.pk2,"pk3", rec2.pk3,"pk4", rec2.pk4,"pk5", rec2.pk5,"pk6", rec2.pk6,"pk7", rec2.pk7));
		Assert.assertEquals(table1.getRecordsNumber(), oldSize1-1);
		Assert.assertEquals(table3.getRecordsNumber(), oldSize2-1);
		try
		{
			Assert.assertTrue(table1.removeRecordWithCascade("pk1", rec1.pk1,"pk2", rec1.pk2,"pk3", rec1.pk3,"pk4", rec1.pk4,"pk5", rec1.pk5,"pk6", rec1.pk6));
			Assert.fail();
		}
		catch (FieldDatabaseException e)
		{

		}
		try
		{
			Assert.assertTrue(table3.removeRecordWithCascade("pk1", rec1.pk1,"pk2", rec1.pk2,"pk3", rec1.pk3,"pk4", rec1.pk4,"pk5", rec1.pk5,"pk6", rec1.pk6));
			Assert.fail();
		}
		catch (FieldDatabaseException e)
		{

		}
		addRecords();
		table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

			@Override
			public boolean nextRecord(Record _record) {
				return true;
			}
		});
		table3.removeRecordsWithCascade(new Filter<Table3.Record>() {

			@Override
			public boolean nextRecord(Table3.Record _record) {
				return true;
			}
		});
		Assert.assertEquals(table1.getRecordsNumber(), 0);
		Assert.assertEquals(table3.getRecordsNumber(), 0);
		Assert.assertEquals(table2.getRecordsNumber(), 0);
		Assert.assertEquals(table4.getRecordsNumber(), 0);
		Assert.assertEquals(table5.getRecordsNumber(), 0);
		Assert.assertEquals(table6.getRecordsNumber(), 0);
		Assert.assertEquals(table1.getRecords().size(), 0);
		Assert.assertEquals(table2.getRecords().size(), 0);
		Assert.assertEquals(table3.getRecords().size(), 0);
		Assert.assertEquals(table4.getRecords().size(), 0);
		Assert.assertEquals(table5.getRecords().size(), 0);
		Assert.assertEquals(table6.getRecords().size(), 0);

		addRecords();
		table1.removeRecordWithCascade(table1.getRecords().get(0));
		table3.removeRecordWithCascade(table3.getRecords().get(0));
		Assert.assertEquals(table1.getRecordsNumber(), 1);
		Assert.assertEquals(table2.getRecordsNumber(), 0);
		Assert.assertEquals(table3.getRecordsNumber(), 1);
		Assert.assertEquals(table4.getRecordsNumber(), 0);
		Assert.assertEquals(table5.getRecordsNumber(), 0);
		Assert.assertEquals(table6.getRecordsNumber(), 0);
		Assert.assertEquals(table1.getRecords().size(), 1);
		Assert.assertEquals(table2.getRecords().size(), 0);
		Assert.assertEquals(table3.getRecords().size(), 1);
		Assert.assertEquals(table4.getRecords().size(), 0);
		Assert.assertEquals(table5.getRecords().size(), 0);
		Assert.assertEquals(table6.getRecords().size(), 0);
		table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

			@Override
			public boolean nextRecord(Record _record) {
				return true;
			}
		});
		table3.removeRecordsWithCascade(new Filter<Table3.Record>() {

			@Override
			public boolean nextRecord(Table3.Record _record) {
				return true;
			}
		});
		Assert.assertEquals(table1.getRecordsNumber(), 0);
		Assert.assertEquals(table3.getRecordsNumber(), 0);
		Assert.assertEquals(table2.getRecordsNumber(), 0);
		Assert.assertEquals(table4.getRecordsNumber(), 0);
		Assert.assertEquals(table5.getRecordsNumber(), 0);
		Assert.assertEquals(table6.getRecordsNumber(), 0);
		Assert.assertEquals(table1.getRecords().size(), 0);
		Assert.assertEquals(table2.getRecords().size(), 0);
		Assert.assertEquals(table3.getRecords().size(), 0);
		Assert.assertEquals(table4.getRecords().size(), 0);
		Assert.assertEquals(table5.getRecords().size(), 0);
		Assert.assertEquals(table6.getRecords().size(), 0);
		addRecords();
		table1.removeRecordsWithCascade(table1.getRecords());
		table3.removeRecordsWithCascade(table3.getRecords());
		Assert.assertEquals(table1.getRecordsNumber(), 0);
		Assert.assertEquals(table3.getRecordsNumber(), 0);
		Assert.assertEquals(table2.getRecordsNumber(), 0);
		Assert.assertEquals(table4.getRecordsNumber(), 0);
		Assert.assertEquals(table5.getRecordsNumber(), 0);
		Assert.assertEquals(table6.getRecordsNumber(), 0);
		Assert.assertEquals(table1.getRecords().size(), 0);
		Assert.assertEquals(table2.getRecords().size(), 0);
		Assert.assertEquals(table3.getRecords().size(), 0);
		Assert.assertEquals(table4.getRecords().size(), 0);
		Assert.assertEquals(table5.getRecords().size(), 0);
		Assert.assertEquals(table6.getRecords().size(), 0);




		prepareMultipleTest();

		table1.updateRecords(new AlterRecordFilter<Table1.Record>() {

			@Override
			public void nextRecord(Record _record) {
				this.removeWithCascade();
			}
		});
		table3.updateRecords(new AlterRecordFilter<Table3.Record>() {

			@Override
			public void nextRecord(Table3.Record _record) {
				this.removeWithCascade();
			}
		});
		Assert.assertEquals(0, table1.getRecordsNumber());
		Assert.assertEquals(0, table1.getRecords().size());

		Assert.assertEquals(0, table3.getRecordsNumber());
		Assert.assertEquals(0, table3.getRecords().size());




		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();
	}

	@SuppressWarnings("unchecked")
	@Test(dependsOnMethods = { "removeWithCascade" })
	public void setAutoRandomFields() throws DatabaseException {
		HashMap<String, Object> map = new HashMap<>();
		map.put("pk1", 0);
		map.put("pk2", 1L);
		map.put("pk3", new BigInteger("0"));
		map.put("pk4", 0L);
		map.put("pk5", new DecentralizedIDGenerator());
		map.put("pk6", new DecentralizedIDGenerator());
		map.put("pk7", new RenforcedDecentralizedIDGenerator());

		map.put("int_value", 3);
		map.put("byte_value", (byte) 3);
		map.put("char_value", 'x');
		map.put("boolean_value", Boolean.FALSE);
		map.put("short_value", (short) 3);
		map.put("long_value", 3L);
		map.put("float_value", 3.3f);
		map.put("double_value", 3.3);
		map.put("string_value", "test string");
		map.put("IntegerNumber_value", 3);
		map.put("ByteNumber_value", (byte) 3);
		map.put("CharacterNumber_value", 'x');
		map.put("BooleanNumber_value", Boolean.FALSE);
		map.put("ShortNumber_value", (short) 3);
		map.put("LongNumber_value", (long) 3);
		map.put("FloatNumber_value", 3.3f);
		map.put("DoubleNumber_value", 3.3);
		map.put("BigInteger_value", new BigInteger("5"));
		map.put("BigDecimal_value", new BigDecimal("8.8"));
		map.put("DateValue", date);
		map.put("CalendarValue", calendar);
		map.put("secretKey", secretKey);
		map.put("typeSecretKey", typeSecretKey);
		map.put("subField", subField);
		map.put("subSubField", subSubField);
		map.put("file", fileTest);
		table1.addRecord(map);
		table3.addRecord(map);
		Table1.Record r1 = table1.getRecords().get(0);

		Assert.assertEquals(r1.pk1, map.get("pk1"));
		Assert.assertEquals(r1.pk2, map.get("pk2"));
		Assert.assertEquals(r1.pk3, map.get("pk3"));
		Assert.assertEquals(r1.pk4, map.get("pk4"));
		Assert.assertEquals(r1.pk5, map.get("pk5"));
		Assert.assertEquals(r1.pk6, map.get("pk6"));
		Assert.assertEquals(r1.pk7, map.get("pk7"));
		Assert.assertEquals(r1.int_value, map.get("int_value"));
		Assert.assertEquals(r1.byte_value, map.get("byte_value"));
		Assert.assertEquals(r1.char_value, map.get("char_value"));
		Assert.assertEquals(r1.boolean_value, map.get("boolean_value"));
		Assert.assertEquals(r1.short_value, map.get("short_value"));
		Assert.assertEquals(r1.long_value, map.get("long_value"));
        Assert.assertEquals(map.get("float_value"), r1.float_value);
        Assert.assertEquals(map.get("double_value"), r1.double_value);
        Assert.assertEquals(map.get("string_value"), r1.string_value);
        Assert.assertEquals(map.get("IntegerNumber_value"), r1.IntegerNumber_value);
        Assert.assertEquals(map.get("ByteNumber_value"), r1.ByteNumber_value);
        Assert.assertEquals(map.get("CharacterNumber_value"), r1.CharacterNumber_value);
        Assert.assertEquals(map.get("BooleanNumber_value"), r1.BooleanNumber_value);
        Assert.assertEquals(map.get("ShortNumber_value"), r1.ShortNumber_value);
        Assert.assertEquals(map.get("LongNumber_value"), r1.LongNumber_value);
        Assert.assertEquals(map.get("FloatNumber_value"), r1.FloatNumber_value);
        Assert.assertEquals(map.get("DoubleNumber_value"), r1.DoubleNumber_value);
        Assert.assertEquals(map.get("BigInteger_value"), r1.BigInteger_value);
        Assert.assertEquals(map.get("BigDecimal_value"), r1.BigDecimal_value);
        Assert.assertEquals(map.get("DateValue"), r1.DateValue);
        Assert.assertEquals(map.get("CalendarValue"), r1.CalendarValue);
        Assert.assertEquals(map.get("secretKey"), r1.secretKey);
        Assert.assertEquals(map.get("typeSecretKey"), r1.typeSecretKey);
        Assert.assertEquals(map.get("file"), r1.file);
		assertEquals((SubField) map.get("subField"), r1.subField);
		assertEquals((SubSubField) map.get("subSubField"), r1.subSubField);

		Table3.Record r3 = table3.getRecords().get(0);

		assertTrue(map.get("pk1").equals(r3.pk1));
		assertTrue(map.get("pk2").equals(r3.pk2));
		assertTrue(map.get("pk3").equals(r3.pk3));
		assertTrue(map.get("pk4").equals(r3.pk4));
		assertTrue(map.get("pk5").equals(r3.pk5));
		assertTrue(map.get("pk6").equals(r3.pk6));
		assertTrue(map.get("pk7").equals(r3.pk7));
		assertTrue(map.get("int_value").equals(r3.int_value));
		assertTrue(map.get("byte_value").equals(r3.byte_value));
		assertTrue(map.get("char_value").equals(r3.char_value));
		assertTrue(map.get("boolean_value").equals(r3.boolean_value));
		assertTrue(map.get("short_value").equals(r3.short_value));
		assertTrue(map.get("long_value").equals(r3.long_value));
        Assert.assertEquals(map.get("float_value"), r3.float_value);
        Assert.assertEquals(map.get("double_value"), r3.double_value);
        Assert.assertEquals(map.get("string_value"), r3.string_value);
        Assert.assertEquals(map.get("IntegerNumber_value"), r3.IntegerNumber_value);
        Assert.assertEquals(map.get("ByteNumber_value"), r3.ByteNumber_value);
        Assert.assertEquals(map.get("CharacterNumber_value"), r3.CharacterNumber_value);
        Assert.assertEquals(map.get("BooleanNumber_value"), r3.BooleanNumber_value);
        Assert.assertEquals(map.get("ShortNumber_value"), r3.ShortNumber_value);
        Assert.assertEquals(map.get("LongNumber_value"), r3.LongNumber_value);
        Assert.assertEquals(map.get("FloatNumber_value"), r3.FloatNumber_value);
        Assert.assertEquals(map.get("DoubleNumber_value"), r3.DoubleNumber_value);
        Assert.assertEquals(map.get("BigInteger_value"), r3.BigInteger_value);
        Assert.assertEquals(map.get("BigDecimal_value"), r3.BigDecimal_value);
        Assert.assertEquals(map.get("DateValue"), r3.DateValue);
        Assert.assertEquals(map.get("CalendarValue"), r3.CalendarValue);
        Assert.assertEquals(map.get("secretKey"), r3.secretKey);
        Assert.assertEquals(map.get("typeSecretKey"), r3.typeSecretKey);
        Assert.assertEquals(map.get("file"), r3.file);
		assertEquals((SubField) map.get("subField"), r3.subField);
		assertEquals((SubSubField) map.get("subSubField"), r3.subSubField);

		try {
			table1.addRecord(map);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		try {
			table3.addRecord(map);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		map.put("pk4", 1L);
		map.put("pk5", new DecentralizedIDGenerator());
		map.put("pk6", new DecentralizedIDGenerator());
		map.put("pk7", new RenforcedDecentralizedIDGenerator());
		try {
			table1.addRecord(map);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		try {
			table3.addRecord(map);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}

		map.put("pk3", new BigInteger("1"));
		table1.addRecord(map);
		table3.addRecord(map);
		map.put("pk2", 2L);
		try {
			table1.addRecord(map);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		try {
			table3.addRecord(map);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		map.put("pk2", Long.valueOf("2"));
		map.put("pk3", new BigInteger("2"));
		Map<String, Object> maps[] = new Map[2];
		maps[0] = map;
		maps[1] = map;
		try {
			table1.addRecords(maps);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		try {
			table3.addRecords(maps);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		maps[1] = (HashMap<String, Object>) map.clone();
		maps[1].remove("pk2");
		try {
			table1.addRecords(maps);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		try {
			table3.addRecords(maps);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}

		maps[1].remove("pk3");

		try {
			table1.addRecords(maps);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		try {
			table3.addRecords(maps);
            fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}

		Assert.assertEquals(2, table1.getRecordsNumber());
		Assert.assertEquals(2, table3.getRecordsNumber());
		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

		removeWithCascade();

		maps[1].remove("pk4");
		maps[1].remove("pk5");
		maps[1].remove("pk6");
		maps[1].remove("pk7");

		table1.addRecords(maps);
		table3.addRecords(maps);
		Assert.assertEquals(2, table1.getRecordsNumber());
		Assert.assertEquals(2, table3.getRecordsNumber());
		r1 = table1.getRecords().get(0);

		Assert.assertEquals(map.get("pk1"), r1.pk1);
		Assert.assertEquals(map.get("pk2"), r1.pk2);
		Assert.assertEquals(map.get("pk3"), r1.pk3);
		Assert.assertEquals(map.get("pk4"), r1.pk4);
		Assert.assertEquals(map.get("pk5"), r1.pk5);
		Assert.assertEquals(map.get("pk6"), r1.pk6);
		Assert.assertEquals(map.get("pk7"), r1.pk7);
		Assert.assertEquals(map.get("int_value"), r1.int_value);
		Assert.assertEquals(map.get("byte_value"), r1.byte_value);
		Assert.assertEquals(map.get("char_value"), r1.char_value);
		Assert.assertEquals(map.get("boolean_value"), r1.boolean_value);
		Assert.assertEquals(map.get("short_value"), r1.short_value);
		Assert.assertEquals(map.get("long_value"), r1.long_value);
		Assert.assertEquals(map.get("float_value"), r1.float_value);
		Assert.assertEquals(map.get("double_value"), r1.double_value);
		Assert.assertEquals(map.get("string_value"), r1.string_value);
		Assert.assertEquals(map.get("IntegerNumber_value"), r1.IntegerNumber_value);
		Assert.assertEquals(map.get("ByteNumber_value"), r1.ByteNumber_value);
		Assert.assertEquals(map.get("CharacterNumber_value"), r1.CharacterNumber_value);
		Assert.assertEquals(map.get("BooleanNumber_value"), r1.BooleanNumber_value);
		Assert.assertEquals(map.get("ShortNumber_value"), r1.ShortNumber_value);
		Assert.assertEquals(map.get("LongNumber_value"), r1.LongNumber_value);
		Assert.assertEquals(map.get("FloatNumber_value"), r1.FloatNumber_value);
		Assert.assertEquals(map.get("DoubleNumber_value"), r1.DoubleNumber_value);
		Assert.assertEquals(map.get("BigInteger_value"), r1.BigInteger_value);
		Assert.assertEquals(map.get("BigDecimal_value"), r1.BigDecimal_value);
		Assert.assertEquals(map.get("file"), r1.file);
		assertEquals((SubField) map.get("subField"), r1.subField);
		assertEquals((SubSubField) map.get("subSubField"), r1.subSubField);

		r3 = table3.getRecords().get(0);

		Assert.assertEquals(r3.pk1, map.get("pk1"));
		Assert.assertEquals(r3.pk2, map.get("pk2"));
		Assert.assertEquals(r3.pk3, map.get("pk3"));
		Assert.assertEquals(r3.pk4, map.get("pk4"));
		Assert.assertEquals(r3.pk5, map.get("pk5"));
		Assert.assertEquals(r3.pk6, map.get("pk6"));
		Assert.assertEquals(r3.pk7, map.get("pk7"));
		Assert.assertEquals(r3.int_value, map.get("int_value"));
		Assert.assertEquals(r3.byte_value, map.get("byte_value"));
		Assert.assertEquals(r3.char_value, map.get("char_value"));
		Assert.assertEquals(r3.boolean_value, map.get("boolean_value"));
		Assert.assertEquals(r3.short_value, map.get("short_value"));
		Assert.assertEquals(r3.long_value, map.get("long_value"));
		Assert.assertEquals(r3.float_value, map.get("float_value"));
		Assert.assertEquals(r3.double_value, map.get("double_value"));
		Assert.assertEquals(r3.string_value, map.get("string_value"));
		Assert.assertEquals(r3.IntegerNumber_value, map.get("IntegerNumber_value"));
		Assert.assertEquals(r3.ByteNumber_value, map.get("ByteNumber_value"));
		Assert.assertEquals(r3.CharacterNumber_value, map.get("CharacterNumber_value"));
		Assert.assertEquals(r3.BooleanNumber_value, map.get("BooleanNumber_value"));
		Assert.assertEquals(r3.ShortNumber_value, map.get("ShortNumber_value"));
		Assert.assertEquals(r3.LongNumber_value, map.get("LongNumber_value"));
		Assert.assertEquals(r3.FloatNumber_value, map.get("FloatNumber_value"));
		Assert.assertEquals(r3.DoubleNumber_value, map.get("DoubleNumber_value"));
		Assert.assertEquals(r3.BigInteger_value, map.get("BigInteger_value"));
		Assert.assertEquals(r3.BigDecimal_value, map.get("BigDecimal_value"));
		Assert.assertEquals(r3.file, map.get("file"));
		assertEquals((SubField) map.get("subField"), r3.subField);
		assertEquals((SubSubField) map.get("subSubField"), r3.subSubField);

		Map<String, Object> maps2[] = new Map[1];
		maps2[0] = maps[0];

		try {
			table1.addRecords(maps2);
			fail();
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		try {
			table3.addRecords(maps2);
			assertTrue(false);
		} catch (ConstraintsNotRespectedDatabaseException e) {
			assertTrue(true);
		}
		table1.checkDataIntegrity();
		table3.checkDataIntegrity();
		table2.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();

	}

	private ArrayList<Object> getExpectedParameter(Class<?> type, Object o) throws IOException, DatabaseException {
		ArrayList<Object> res = new ArrayList<>();
		
		if (o==null && type==DecentralizedIDGenerator.class)
		{
			res.add(null);
			res.add(null);
		}	
		else {
            assert o != null;
            if (o.getClass() == DecentralizedIDGenerator.class) {
                DecentralizedIDGenerator id = (DecentralizedIDGenerator) o;
                res.add(id.getTimeStamp());
                res.add(id.getWorkerIDAndSequence());
            } else if (o.getClass() == RenforcedDecentralizedIDGenerator.class) {
                RenforcedDecentralizedIDGenerator id = (RenforcedDecentralizedIDGenerator) o;
                res.add(id.getTimeStamp());
                res.add(id.getWorkerIDAndSequence());
            } else if (DecentralizedIDGenerator.class.isAssignableFrom(type)) {
                DecentralizedIDGenerator d = (DecentralizedIDGenerator) o;
                res.add(d.getTimeStamp());
                res.add(d.getWorkerIDAndSequence());
            } else if (AbstractDecentralizedID.class.isAssignableFrom(type)) {
                AbstractDecentralizedID id = (AbstractDecentralizedID) o;
                if (sql_db.isVarBinarySupported())
                    res.add(id.encode());
                else {
                    byte[] bytes = id.encode();
                    BigInteger r = BigInteger.valueOf(1);
                    for (byte aByte : bytes) {
                        r = r.shiftLeft(8).or(BigInteger.valueOf(aByte & 0xFF));
                    }
                    res.add(new BigDecimal(r));
                }
            } else if (o.getClass() == Integer.class
                    || o.getClass() == Byte.class || o.getClass() == Character.class || o.getClass() == Boolean.class
                    || o.getClass() == Short.class || o.getClass() == Long.class || o.getClass() == Float.class
                    || o.getClass() == Double.class || o.getClass() == byte[].class || o.getClass() == String.class) {
                res.add(o);

            } else if (o.getClass() == BigInteger.class) {
				String t=getDatabaseWrapperInstanceA().getBigIntegerType(128);

                res.add(t.contains("CHAR")?o.toString():(((sql_db.isVarBinarySupported() && t.contains(sql_db.getBinaryBaseWord())) || (t.contains(sql_db.getBlobBaseWord())))?((BigInteger) o).toByteArray():new BigDecimal((BigInteger)o)));
            } else if (o.getClass() == BigDecimal.class) {
				String t=getDatabaseWrapperInstanceA().getBigDecimalType(128);
				res.add(t.contains("CHAR")?o.toString():(((sql_db.isVarBinarySupported() && t.contains(sql_db.getBinaryBaseWord())) || (t.contains(sql_db.getBlobBaseWord())))?BigDecimalFieldAccessor.bigDecimalToBytes((BigDecimal)o):o));
			} else if (o instanceof Date) {
                res.add(new Timestamp(((Date) o).getTime()));
            }
            else if (o instanceof File)
            {
                res.add(((File)o).getPath().getBytes(StandardCharsets.UTF_8));
            }
            else if (o instanceof Serializable) {
				String s=sql_db.getBlobType(70000);

                if (s!=null && s.contains(sql_db.getBlobBaseWord())) {
                    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                        try (ObjectOutputStream os = new ObjectOutputStream(baos)) {
                            os.writeObject(o);
                            res.add(baos.toByteArray());
                        }
                    }
                } else
                    res.add(o);
            }
        }
		return res;
	}

	private ArrayList<String> getExpectedParametersName(String variableName, Class<?> objectClass) {
		ArrayList<String> res = new ArrayList<>();

		if (objectClass == DecentralizedIDGenerator.class) {
			res.add(variableName + "_ts");
			res.add(variableName + "_widseq");
		} else if (objectClass == RenforcedDecentralizedIDGenerator.class) {
			res.add(variableName + "_ts");
			res.add(variableName + "_widseq");
		} else {
			res.add(variableName);

		}

		return res;
	}

	@SuppressWarnings("unused")
	private <T extends Record> void subInterpreterCommandProvider(SymbolType op_cond, SymbolType op_comp, String cs,
																  HashMap<String, Object> parametersTable1Equallable, StringBuffer command, StringBuffer expectedCommand,
																  String fieldName, Object value, HashMap<Integer, Object> expectedParameters, boolean useParameter,
																  AtomicInteger expectedParamterIndex, AtomicInteger openedParenthesis, Table<T> table, T record,
																  AtomicBoolean whereResult) throws IOException, DatabaseException {
		if (op_comp == SymbolType.LIKE || op_comp == SymbolType.NOTLIKE) {
			if (!(value instanceof String))
				return;
		} else if (op_comp == SymbolType.GREATEROPERATOR || op_comp == SymbolType.GREATEROREQUALOPERATOR
				|| op_comp == SymbolType.LOWEROPERATOR || op_comp == SymbolType.LOWEROREQUALOPERATOR) {
			if (!(value instanceof Number) || value.getClass() == Boolean.class)
				return;
		} else if (!useParameter) {
			if (!(value instanceof String))
				return;
		}
		boolean startCommand = true;
		if (command.length() > 0) {
			startCommand = false;
			command.append(" ");
			command.append(op_cond.getContent());
			command.append(" ");

			expectedCommand.append(" ");
			expectedCommand.append(op_cond.getContent());
			expectedCommand.append(" ");
		}
		if (Math.random() < 0.5) {
			command.append("(");
			expectedCommand.append("(");
			openedParenthesis.incrementAndGet();
		}
		command.append(fieldName);
		command.append(cs);

		boolean test = false;

		ArrayList<String> sqlVariablesName = getExpectedParametersName(fieldName, value==null?DecentralizedIDGenerator.class:value.getClass());

		Table.FieldAccessorValue fieldAccessorAndValue =table.getFieldAccessorAndValue(record, fieldName);
		FieldAccessor fa=fieldAccessorAndValue.getFieldAccessor();
		Object nearestObjectInstance=fieldAccessorAndValue.getValue();
		if (useParameter) {
			command.append("%");
			command.append(fieldName.replace(".", "_"));

			if (op_comp == SymbolType.EQUALOPERATOR)
				test = fa.equals(nearestObjectInstance, value);
			else if (op_comp == SymbolType.NOTEQUALOPERATOR)
				test = !fa.equals(nearestObjectInstance, value);
			else if (op_comp == SymbolType.LIKE)
				test = fa.equals(nearestObjectInstance, value);
			else if (op_comp == SymbolType.NOTLIKE)
				test = !fa.equals(nearestObjectInstance, value);
			else {
				Table1.Record r2 = new Table1.Record();
				r2.subField=new SubField();
				Object record2=table1.getFieldAccessorAndValue(r2, fa.getFieldName()).getValue();
				if (record2==null)
					throw new NullPointerException(fa.getFieldName());

				fa.setValue(record2, value);
				int comp = fa.compare(nearestObjectInstance, record2);
				if (op_comp == SymbolType.GREATEROPERATOR)
					test = comp > 0;
				else if (op_comp == SymbolType.GREATEROREQUALOPERATOR)
					test = comp >= 0;
				else if (op_comp == SymbolType.LOWEROPERATOR)
					test = comp < 0;
				else if (op_comp == SymbolType.LOWEROREQUALOPERATOR)
					test = comp <= 0;
			}

			ArrayList<Object> sqlInstance = getExpectedParameter(fa.getFieldClassType(), value);
			if (op_comp == SymbolType.EQUALOPERATOR || op_comp == SymbolType.NOTEQUALOPERATOR)
				expectedCommand.append("(");
			for (int i = 0; i < sqlInstance.size(); i++) {
				if (i > 0)
					expectedCommand.append(" AND ");
				expectedCommand.append("%Table1Name%");
				expectedCommand.append(".`");
				expectedCommand.append(sqlVariablesName.get(i).replace(".", "_").toUpperCase());
				expectedCommand.append("`");
				expectedCommand.append(op_comp.getContent());
				if (value==null)
				{
					expectedCommand.append("NULL");
				}
				else
				{
					expectedCommand.append("?");
					expectedParameters.put(expectedParamterIndex.getAndIncrement(), sqlInstance.get(i));
				}
			}
			if (op_comp == SymbolType.EQUALOPERATOR || op_comp == SymbolType.NOTEQUALOPERATOR)
				expectedCommand.append(")");
		} else {
			if (op_comp == SymbolType.EQUALOPERATOR) {
				assert fa!=null;
				test = fa.getValue(nearestObjectInstance).toString().equals(value.toString());
			}
			else if (op_comp == SymbolType.NOTEQUALOPERATOR)
				test = !fa.getValue(nearestObjectInstance).toString().equals(value.toString());
			else if (op_comp == SymbolType.LIKE)
				test = fa.getValue(nearestObjectInstance).toString().equals(value.toString());
			else if (op_comp == SymbolType.NOTLIKE)
				test = !fa.getValue(nearestObjectInstance).toString().equals(value.toString());
			else {
				BigDecimal v = new BigDecimal(fa.getValue(nearestObjectInstance).toString());

				int comp = v.compareTo(new BigDecimal(value.toString()));
				if (op_comp == SymbolType.GREATEROPERATOR)
					test = comp > 0;
				else if (op_comp == SymbolType.GREATEROREQUALOPERATOR)
					test = comp >= 0;
				else if (op_comp == SymbolType.LOWEROPERATOR)
					test = comp < 0;
				else if (op_comp == SymbolType.LOWEROREQUALOPERATOR)
					test = comp <= 0;
			}

			if (value instanceof CharSequence)
				command.append("\"");
			command.append(value.toString());
			if (value instanceof CharSequence)
				command.append("\"");
			expectedCommand.append("%Table1Name%");
			expectedCommand.append(".`");
			expectedCommand.append(sqlVariablesName.get(0).replace(".", "_").toUpperCase());
			expectedCommand.append("`");
			expectedCommand.append(op_comp.getContent());
			if (value instanceof CharSequence)
				expectedCommand.append("\"");
			expectedCommand.append(value.toString());
			if (value instanceof CharSequence)
				expectedCommand.append("\"");
		}
		if (openedParenthesis.get() > 0 && Math.random() < 0.5) {
			command.append(")");
			expectedCommand.append(")");
			openedParenthesis.decrementAndGet();
		}
		if (startCommand)
			whereResult.set(test);
		else if (op_cond == SymbolType.ANDCONDITION)
			whereResult.set(whereResult.get() && test);
		else if (op_cond == SymbolType.ORCONDITION)
			whereResult.set(whereResult.get() || test);

	}

	@DataProvider(name = "interpreterCommandsProvider")
	public Object[][] interpreterCommandsProvider()
			throws IOException, NoSuchAlgorithmException, NoSuchProviderException, DatabaseException {
		HashMap<String, Object> parametersTable1Equallable = new HashMap<>();

		AbstractSecureRandom rand = SecureRandomType.DEFAULT.getSingleton(null);
		ArrayList<Object[]> res = new ArrayList<>();
		parametersTable1Equallable.put("pk5", new SecuredDecentralizedID(new DecentralizedIDGenerator(), rand));
		parametersTable1Equallable.put("pk6", new DecentralizedIDGenerator());
		parametersTable1Equallable.put("pk7", new RenforcedDecentralizedIDGenerator());
		parametersTable1Equallable.put("int_value", 1);
		parametersTable1Equallable.put("byte_value", (byte) 1);
		parametersTable1Equallable.put("char_value", 'a');
		parametersTable1Equallable.put("boolean_value", Boolean.TRUE);
		parametersTable1Equallable.put("short_value", (short) 1);
		parametersTable1Equallable.put("long_value", 1L);
		parametersTable1Equallable.put("float_value", 1.0f);
		parametersTable1Equallable.put("double_value", 1.0);
		parametersTable1Equallable.put("string_value", "string");

		parametersTable1Equallable.put("IntegerNumber_value", 1);
		parametersTable1Equallable.put("ByteNumber_value", (byte) 1);
		parametersTable1Equallable.put("CharacterNumber_value", 'a');
		parametersTable1Equallable.put("BooleanNumber_value", Boolean.TRUE);
		parametersTable1Equallable.put("ShortNumber_value", (short) 1);
		parametersTable1Equallable.put("LongNumber_value", 1L);
		parametersTable1Equallable.put("FloatNumber_value", 1.0f);
		parametersTable1Equallable.put("DoubleNumber_value", 1.0);
		

		Calendar calendar = Calendar.getInstance(Locale.CANADA);
		calendar.set(2045, 7, 29, 18, 32, 43);

		parametersTable1Equallable.put("byte_array_value", new byte[] { (byte) 0, (byte) 1 });
		parametersTable1Equallable.put("BigInteger_value", BigInteger.valueOf(3));
		parametersTable1Equallable.put("BigDecimal_value", BigDecimal.valueOf(4));
		parametersTable1Equallable.put("DateValue", date);
		parametersTable1Equallable.put("CalendarValue", calendar);
		parametersTable1Equallable.put("nullField", null);
		parametersTable1Equallable.put("file", fileTest);
		parametersTable1Equallable.put("subField.string_value", "string_for_sql_interpreter");
		parametersTable1Equallable.put("subField.int_value", 15);


		Table1.Record record = new Table1.Record();
		record.BigDecimal_value = (BigDecimal) parametersTable1Equallable.get("BigDecimal_value");
		record.BigInteger_value = (BigInteger) parametersTable1Equallable.get("BigInteger_value");
		record.boolean_value = (Boolean) parametersTable1Equallable.get("boolean_value");
		record.BooleanNumber_value = (Boolean) parametersTable1Equallable.get("BooleanNumber_value");
		record.byte_array_value = (byte[]) parametersTable1Equallable.get("byte_array_value");
		record.byte_value = (Byte) parametersTable1Equallable.get("byte_value");
		record.ByteNumber_value = (Byte) parametersTable1Equallable.get("ByteNumber_value");
		record.CalendarValue = (Calendar) parametersTable1Equallable.get("CalendarValue");
		record.char_value = (Character) parametersTable1Equallable.get("char_value");
		record.CharacterNumber_value = (Character) parametersTable1Equallable.get("CharacterNumber_value");
		record.DateValue = (Date) parametersTable1Equallable.get("DateValue");
		record.double_value = (Double) parametersTable1Equallable.get("double_value");
		record.DoubleNumber_value = (Double) parametersTable1Equallable.get("DoubleNumber_value");
		record.float_value = (Float) parametersTable1Equallable.get("float_value");
		record.FloatNumber_value = (Float) parametersTable1Equallable.get("FloatNumber_value");
		record.int_value = (Integer) parametersTable1Equallable.get("int_value");
		record.IntegerNumber_value = (Integer) parametersTable1Equallable.get("IntegerNumber_value");
		record.long_value = (Long) parametersTable1Equallable.get("long_value");
		record.LongNumber_value = ((Long) parametersTable1Equallable.get("LongNumber_value"));
		record.pk5 = (AbstractDecentralizedID) parametersTable1Equallable.get("pk5");
		record.pk6 = (DecentralizedIDGenerator) parametersTable1Equallable.get("pk6");
		record.pk7 = (RenforcedDecentralizedIDGenerator) parametersTable1Equallable.get("pk7");
		record.secretKey = (SymmetricSecretKey) parametersTable1Equallable.get("secretKey");
		record.typeSecretKey = (SymmetricEncryptionType) parametersTable1Equallable.get("typeSecretKey");
		record.short_value = (Short) parametersTable1Equallable.get("short_value");
		record.ShortNumber_value = (Short) parametersTable1Equallable.get("ShortNumber_value");
		record.string_value = (String) parametersTable1Equallable.get("string_value");
		record.nullField= (DecentralizedIDGenerator)parametersTable1Equallable.get("nullField");
		record.file=(File)parametersTable1Equallable.get("file");
		record.subField=new SubField();
		record.subField.string_value=(String)parametersTable1Equallable.get("subField.string_value");
		record.subField.int_value=(Integer)parametersTable1Equallable.get("subField.int_value");

		SymbolType[] ops_cond = new SymbolType[] { SymbolType.ANDCONDITION, SymbolType.ORCONDITION };
		SymbolType[] ops_comp = new SymbolType[] { SymbolType.EQUALOPERATOR, SymbolType.NOTEQUALOPERATOR,
				SymbolType.LIKE, SymbolType.NOTLIKE, SymbolType.GREATEROPERATOR, SymbolType.GREATEROREQUALOPERATOR,
				SymbolType.LOWEROPERATOR, SymbolType.LOWEROREQUALOPERATOR };

		for (SymbolType op_cond : ops_cond) {
			for (SymbolType op_comp : ops_comp) {
				for (String cs : op_comp.getMatches()) {
					for (boolean useParameter : new boolean[] { false, true }) {
						StringBuffer command = new StringBuffer();
						StringBuffer expectedCommand = new StringBuffer();

						HashMap<Integer, Object> expectedParameters = new HashMap<>();
						AtomicInteger expectedParamterIndex = new AtomicInteger(1);
						AtomicInteger openedParenthesis = new AtomicInteger(0);
						AtomicBoolean expectedTestResult = new AtomicBoolean();
						HashMap<String, Object> parametersTable1Equallable2 = new HashMap<>();
						for (Map.Entry<String, Object> m : parametersTable1Equallable.entrySet()) {
							if (m.getValue()!=null || op_comp==SymbolType.EQUALOPERATOR || op_comp==SymbolType.NOTEQUALOPERATOR)
								subInterpreterCommandProvider(op_cond, op_comp, cs, parametersTable1Equallable, command,
									expectedCommand, m.getKey(), m.getValue(), expectedParameters, useParameter,
									expectedParamterIndex, openedParenthesis, table1, record, expectedTestResult);
							parametersTable1Equallable2.put(m.getKey().replace(".", "_"), m.getValue());
						}


						while (openedParenthesis.get() > 0) {
							command.append(")");
							expectedCommand.append(")");
							openedParenthesis.decrementAndGet();
						}
						if (command.length() > 0) {
							res.add(new Object[] { Table1.class, command.toString(), parametersTable1Equallable2,
									expectedCommand.toString(), expectedParameters, record,
                                    expectedTestResult.get()});
						}
					}
				}
			}
		}

		Object[][] resO = new Object[res.size()][];
		for (int i = 0; i < res.size(); i++) {
			resO[i] = res.get(i);
		}
		return resO;
	}

	// @Test(dependsOnMethods={"setAutoRandomFields"}, dataProvider =
	// "interpreterCommandsProvider")
	public <T extends DatabaseRecord> void testIsConcernedInterpreterFunction(Table<T> table, T record, String command,
			Map<String, Object> parameters, boolean expectedBoolean) throws DatabaseException {
		boolean bool = Interpreter.getRuleInstance(command).isConcernedBy(table, parameters, record);
		Assert.assertEquals(bool, expectedBoolean);
	}

	@Test(dependsOnMethods = { "firstReload" }, dataProvider = "interpreterCommandsProvider")
	public void testCommandTranslatorInterpreter(Class<? extends Table<?>> tableClass, String command, Map<String, Object> parameters,
			String expectedSqlCommand, Map<Integer, Object> expectedSqlParameters, Table1.Record record,
			boolean expectedTestResult) throws DatabaseException, IOException, SQLException {
		Table<?> table=sql_db.getTableInstance(tableClass);
		Table1 table1=(Table1)sql_db.getTableInstance(Table1.class);
		if (expectedSqlCommand!=null)
			expectedSqlCommand=expectedSqlCommand.replace("%Table1Name%", table1.getSqlTableName());
		HashMap<Integer, Object> sqlParameters = new HashMap<>();
		RuleInstance rule = Interpreter.getRuleInstance(command);
		String sqlCommand = rule.translateToSqlQuery(table, parameters, sqlParameters, new HashSet<TableJunction>())
				.toString();
		if (!table.getDatabaseWrapper().supportsItalicQuotesWithTableAndFieldNames()) {
			assert expectedSqlCommand != null;
			expectedSqlCommand=expectedSqlCommand.replace("`", "");
		}
		Assert.assertEquals(sqlCommand, expectedSqlCommand);
		for (Map.Entry<Integer, Object> e : sqlParameters.entrySet()) {
			if (e.getValue() == null)
				throw new NullPointerException();
			if (e.getKey() == null)
				throw new NullPointerException();
			
			Object ep=expectedSqlParameters.get(e.getKey());
			Object value=e.getValue();
			if (ep instanceof byte[] && value instanceof BigDecimal)
			{
				value=ByteTabFieldAccessor.getByteTab((BigDecimal)value);
			}
			assertEqualsParameters(value, ep, "Class type source " + e.getValue().getClass()+", expected class "+ep.getClass());
		}
		Assert.assertEquals(rule.isConcernedBy(table1, parameters, record), expectedTestResult, command);
	}
	
	private void assertEqualsParameters(Object parameter, Object expectedParameter, String message) throws IOException, SQLException
	{
		if (expectedParameter==null)
		{
			assertNull(parameter);
			return;
		}
		if (expectedParameter.getClass()==byte[].class)
		{
			if (parameter instanceof ByteArrayInputStream)
			{
				ByteArrayInputStream bais=(ByteArrayInputStream)parameter;
				try(ByteArrayOutputStream baos=new ByteArrayOutputStream())
				{
					int val=bais.read();
					while (val!=-1)
					{
						baos.write(val);
						val=bais.read();
					}
					parameter=baos.toByteArray();
				}
			}
			else if (parameter instanceof Blob)
			{
				Blob blob=((Blob)parameter);
				
				parameter=blob.getBytes(1, (int)blob.length());
			}
		}
		Assert.assertEquals(parameter, expectedParameter, message);
	}

	private static final AtomicInteger next_unique = new AtomicInteger(0);
	private static AtomicInteger number_thread_test = new AtomicInteger(0);

	@Test(dependsOnMethods = { "setAutoRandomFields" })
	public void prepareMultipleTest() throws DatabaseException {

		HashMap<String, Object> map = new HashMap<>();

		map.put("pk1", random.nextInt());
		map.put("int_value", 3);
		map.put("byte_value", (byte) 3);
		map.put("char_value", 'x');
		map.put("boolean_value", Boolean.TRUE);
		map.put("short_value", (short) random.nextInt());
		map.put("long_value", 3000L);
		map.put("float_value", 3.3f);
		map.put("double_value", 3.3);
		map.put("string_value", "test string");
		map.put("IntegerNumber_value", 3);
		map.put("ByteNumber_value", (byte) 3);
		map.put("CharacterNumber_value", 'x');
		map.put("BooleanNumber_value", Boolean.TRUE);
		map.put("ShortNumber_value", (short) 3);
		map.put("LongNumber_value", random.nextLong());
		map.put("FloatNumber_value", 3.3f);
		map.put("DoubleNumber_value", 3.3);
		map.put("BigInteger_value", new BigInteger("6"));
		map.put("BigDecimal_value", new BigDecimal("9.9"));
		map.put("DateValue", Calendar.getInstance().getTime());
		map.put("CalendarValue", Calendar.getInstance());
		map.put("secretKey", secretKey);
		map.put("typeSecretKey", typeSecretKey);
		map.put("subField", subField);
		map.put("subSubField", subSubField);
		map.put("file", fileTest);
		byte[] tab = new byte[3];
		tab[0] = 0;
		tab[1] = 1;
		tab[2] = 2;
		map.put("byte_array_value", tab);
		table1.addRecord(map);
		table3.addRecord(map);
		@SuppressWarnings("unchecked")
		HashMap<String, Object> list[] = new HashMap[10];
		for (int i = 0; i < list.length; i++)
			list[i] = map;
		table1.addRecords(list);
		table3.addRecords(list);

		for (int i = 0; i < 8; i++) {
			try {
				ArrayList<Table1.Record> records = table1.getRecords();
				HashMap<String, Object> map2 = new HashMap<>();
				map2.put("fr1_pk1", records.get(random.nextInt(records.size())));
				map2.put("int_value", random.nextInt());
				table2.addRecord(map2);
			} catch (ConstraintsNotRespectedDatabaseException e) {
			} catch (RecordNotFoundDatabaseException e) {
				if (no_thread)
					throw e;
			}
		}
		for (int i = 0; i < 8; i++) {
			try {
				ArrayList<Table3.Record> records = table3.getRecords();
				HashMap<String, Object> map2 = new HashMap<>();
				map2.put("fr1_pk1", records.get(random.nextInt(records.size())));
				map2.put("int_value", random.nextInt());

				table4.addRecord(map2);
				table5.addRecord(map2);
			} catch (ConstraintsNotRespectedDatabaseException e) {

			} catch (RecordNotFoundDatabaseException e) {
				if (no_thread)
					throw e;
			}
		}
		for (int i = 0; i < 6; i++) {
			try {
				synchronized(this) {
					ArrayList<Table2.Record> records2 = table2.getRecords();
					ArrayList<Table5.Record> records5 = table5.getRecords();
					HashMap<String, Object> map2 = new HashMap<>();

					if (records2.size()>0 && records5.size()>0) {
						map2.put("fk1_pk1", records2.get(random.nextInt(records2.size())));
						map2.put("fk2", records5.get(random.nextInt(records5.size())));
						table6.addRecord(map2);
					}
				}

			} catch (ConstraintsNotRespectedDatabaseException e) {
			} catch (RecordNotFoundDatabaseException e) {
				if (no_thread)
					throw e;
			}
		}
	}

	@Test(dependsOnMethods = { "prepareMultipleTest" })
	public void multipleTests() throws DatabaseException {
		System.out.println("No thread=" + no_thread);
		for (int i = 0; i < getMultiTestsNumber(); i++)
			subMultipleTests();
	}

	public abstract boolean isTestEnabled(int testNumber);

	public static volatile boolean no_thread = true;
	public final ThreadLocalRandom random = ThreadLocalRandom.current();

	@Test(invocationCount = 0)
	public void subMultipleTests() throws DatabaseException {
		Table1 table1;
		Table2 table2;
		Table3 table3;
		Table4 table4;
		Table5 table5;
		Table6 table6;
		if (!isMultiConcurrentDatabase() || random.nextInt(5) != 0) {
			table1 = TestDatabase.table1;
			table2 = TestDatabase.table2;
			table3 = TestDatabase.table3;
			table4 = TestDatabase.table4;
			table5 = TestDatabase.table5;
			table6 = TestDatabase.table6;
		} else {
			table1 = TestDatabase.table1b;
			table2 = TestDatabase.table2b;
			table3 = TestDatabase.table3b;
			table4 = TestDatabase.table4b;
			table5 = TestDatabase.table5b;
			table6 = TestDatabase.table6b;
		}
		int r = random.nextInt(33);

		int number = number_thread_test.incrementAndGet();

		if (isTestEnabled(r)) {
			System.out.println("Test " + number + " number " + r + " in progress.");
			switch (r) {
			case 0: {
				table1.getRecords();
				table2.getRecords();
				table3.getRecords();
				table4.getRecords();
				table5.getRecords();
				table6.getRecords();
			}
				break;
			case 1: {
				HashMap<String, Object> map = new HashMap<>();
				map.put("pk1", random.nextInt());
				map.put("int_value", 3);
				map.put("byte_value", (byte) 3);
				map.put("char_value", 'x');
				map.put("boolean_value", Boolean.TRUE);
				map.put("short_value", (short) random.nextInt());
				map.put("long_value", 3L);
				map.put("float_value", 3.3f);
				map.put("double_value", 3.3);
				map.put("string_value", "test string");
				map.put("IntegerNumber_value", 3);
				map.put("ByteNumber_value", (byte) 3);
				map.put("CharacterNumber_value", 'x');
				map.put("BooleanNumber_value", Boolean.TRUE);
				map.put("ShortNumber_value", (short) 3);
				map.put("LongNumber_value", random.nextLong());
				map.put("FloatNumber_value", 3.3f);
				map.put("DoubleNumber_value", 3.3);
				map.put("BigInteger_value", new BigInteger("6"));
				map.put("BigDecimal_value", new BigDecimal("1.10"));
				map.put("DateValue", Calendar.getInstance().getTime());
				map.put("CalendarValue", Calendar.getInstance());
				map.put("secretKey", secretKey);
				map.put("typeSecretKey", typeSecretKey);
				map.put("subField", subField);
				map.put("subSubField", subSubField);
				map.put("file", fileTest);
				byte[] tab = new byte[3];
				tab[0] = 0;
				tab[1] = 1;
				tab[2] = 2;
				map.put("byte_array_value", tab);
				try {
					Table1.Record r1 = table1.addRecord(map);
					Table3.Record r3 = table3.addRecord(map);
					if (no_thread) {
						assertTrue(table1.contains(r1));
						assertTrue(table3.contains(r3));
					}

                    Assert.assertEquals(r1.pk1, (int) (Integer) map.get("pk1"));
                    Assert.assertEquals(r1.int_value, (int) (Integer) map.get("int_value"));
                    Assert.assertEquals(r1.byte_value, (byte) (Byte) map.get("byte_value"));
                    Assert.assertEquals(r1.char_value, (char) (Character) map.get("char_value"));
                    Assert.assertEquals(r1.boolean_value, (boolean) (Boolean) map.get("boolean_value"));
                    Assert.assertEquals(r1.short_value, (short) (Short) map.get("short_value"));
                    Assert.assertEquals(r1.long_value, (long) (Long) map.get("long_value"));
                    Assert.assertEquals(r1.float_value, (Float) map.get("float_value"), 0.0);
                    Assert.assertEquals(r1.double_value, (Double) map.get("double_value"), 0.0);
                    Assert.assertEquals(r1.string_value, map.get("string_value"));
                    Assert.assertEquals(r1.IntegerNumber_value, map.get("IntegerNumber_value"));
                    Assert.assertEquals(r1.ByteNumber_value, map.get("ByteNumber_value"));
                    Assert.assertEquals(r1.CharacterNumber_value, map.get("CharacterNumber_value"));
                    Assert.assertEquals(r1.BooleanNumber_value, map.get("BooleanNumber_value"));
                    Assert.assertEquals(r1.ShortNumber_value, map.get("ShortNumber_value"));
					assertTrue(r1.LongNumber_value.equals(map.get("LongNumber_value")));
					assertTrue(r1.FloatNumber_value.equals(map.get("FloatNumber_value")));
					assertTrue(r1.DoubleNumber_value.equals(map.get("DoubleNumber_value")));
					assertTrue(r1.BigInteger_value.equals(map.get("BigInteger_value")));
					assertTrue(r1.BigDecimal_value.equals(map.get("BigDecimal_value")));
					assertTrue(r1.secretKey.equals(map.get("secretKey")));
					assertTrue(r1.typeSecretKey.equals(map.get("typeSecretKey")));
					assertTrue(r1.file.equals(map.get("file")));
					assertEquals((SubField) map.get("subField"), r1.subField);
					assertEquals((SubSubField) map.get("subSubField"), r1.subSubField);

					for (int i = 0; i < 3; i++)
						assertTrue(r1.byte_array_value[i] == ((byte[]) map.get("byte_array_value"))[i]);

                    Assert.assertEquals(r3.pk1, (int) (Integer) map.get("pk1"));
                    Assert.assertEquals(r3.int_value, (int) (Integer) map.get("int_value"));
                    Assert.assertEquals(r3.byte_value, (byte) (Byte) map.get("byte_value"));
                    Assert.assertEquals(r3.char_value, (char) (Character) map.get("char_value"));
                    Assert.assertEquals(r3.boolean_value, (boolean) (Boolean) map.get("boolean_value"));
                    Assert.assertEquals(r3.short_value, (short) (Short) map.get("short_value"));
                    Assert.assertEquals(r3.long_value, (long) (Long) map.get("long_value"));
                    Assert.assertEquals(r3.float_value, (Float) map.get("float_value"), 0.0);
                    Assert.assertEquals(r3.double_value, (Double) map.get("double_value"), 0.0);
                    Assert.assertEquals(r3.string_value, map.get("string_value"));
                    Assert.assertEquals(r3.IntegerNumber_value, map.get("IntegerNumber_value"));
                    Assert.assertEquals(r3.ByteNumber_value, map.get("ByteNumber_value"));
                    Assert.assertEquals(r3.CharacterNumber_value, map.get("CharacterNumber_value"));
                    Assert.assertEquals(r3.BooleanNumber_value, map.get("BooleanNumber_value"));
                    Assert.assertEquals(r3.ShortNumber_value, map.get("ShortNumber_value"));
                    Assert.assertEquals(r3.LongNumber_value, map.get("LongNumber_value"));
                    Assert.assertEquals(r3.FloatNumber_value, map.get("FloatNumber_value"));
                    Assert.assertEquals(r3.DoubleNumber_value, map.get("DoubleNumber_value"));
                    Assert.assertEquals(r3.BigInteger_value, map.get("BigInteger_value"));
                    Assert.assertEquals(r3.BigDecimal_value, map.get("BigDecimal_value"));
                    Assert.assertEquals(r3.secretKey, map.get("secretKey"));
                    Assert.assertEquals(r3.typeSecretKey, map.get("typeSecretKey"));
                    Assert.assertEquals(r3.file, map.get("file"));
					assertEquals((SubField) map.get("subField"), r3.subField);
					assertEquals((SubSubField) map.get("subSubField"), r3.subSubField);

					for (int i = 0; i < 3; i++)
                        Assert.assertEquals(r3.byte_array_value[i], ((byte[]) map.get("byte_array_value"))[i]);
				} catch (ConstraintsNotRespectedDatabaseException e) {
					if (no_thread) {
						throw e;
					}
				}
			}
				break;
			case 2: {
				int next;
				synchronized (next_unique) {
					next = next_unique.intValue();
					next_unique.set(next + 1);
				}
				ArrayList<Table1.Record> records = table1.getRecords();
				if (records.size() > 0) {
					HashMap<String, Object> map = new HashMap<>();
					map.put("fr1_pk1", records.get(random.nextInt(records.size())));
					map.put("int_value", next);
					try {
						ArrayList<Table2.Record> records2 = table2.getRecords();
						boolean found = false;
						for (Table2.Record r2 : records2) {
							if (table1.equals(r2.fr1_pk1, (Table1.Record) map.get("fr1_pk1"))) {
								found = true;
								break;
							}
						}
						if (!found) {
							Table2.Record r2 = table2.addRecord(map);
							if (no_thread) {
								assertTrue(table1.equals(r2.fr1_pk1, (Table1.Record) map.get("fr1_pk1")));
                                Assert.assertEquals(r2.int_value, (int) (Integer) map.get("int_value"));
							}
						}
					} catch (RecordNotFoundDatabaseException | ConstraintsNotRespectedDatabaseException e) {
						if (no_thread)
							throw e;
					}

                }
			}
				break;
			case 3: {
				int next;
				synchronized (next_unique) {
					next = next_unique.intValue();
					next_unique.set(next + 1);
				}
				ArrayList<Table3.Record> records = table3.getRecords();
				if (records.size() > 0) {
					Table3.Record rec1 = records.get(random.nextInt(records.size()));
					HashMap<String, Object> map = new HashMap<>();
					map.put("fr1_pk1", rec1);
					map.put("int_value", next);
					try {
						ArrayList<Table4.Record> records4 = table4.getRecords();
						boolean found = false;
						for (Table4.Record r4 : records4) {
							if (table3.equals(r4.fr1_pk1, (Table3.Record) map.get("fr1_pk1"))) {
								found = true;
								break;
							}
						}
						if (!found) {
							Table4.Record r4 = table4.addRecord(map);
							if (no_thread) {
								assertTrue(table3.equals(r4.fr1_pk1, (Table3.Record) map.get("fr1_pk1")));
                                Assert.assertEquals(r4.int_value, (int) (Integer) map.get("int_value"));
							}
						}
					} catch (RecordNotFoundDatabaseException | ConstraintsNotRespectedDatabaseException e) {
						if (no_thread)
							throw e;
					}
                }
			}
				break;
			case 4: {
				int next;
				synchronized (next_unique) {
					next = next_unique.intValue();
					next_unique.set(next + 1);
				}
				ArrayList<Table3.Record> records = table3.getRecords();
				if (records.size() > 0) {
					Table3.Record rec1 = records.get(random.nextInt(records.size()));
					HashMap<String, Object> map = new HashMap<>();
					map.put("fr1_pk1", rec1);
					map.put("int_value", next);
					try {
						ArrayList<Table5.Record> records5 = table5.getRecords();
						boolean found = false;
						for (Table5.Record r5 : records5) {
							if (table3.equals(r5.fr1_pk1, (Table3.Record) map.get("fr1_pk1"))) {
								found = true;
								break;
							}
						}
						if (!found) {
							Table5.Record r5 = table5.addRecord(map);
							if (no_thread) {
								assertTrue(table3.equals(r5.fr1_pk1, (Table3.Record) map.get("fr1_pk1")));
                                Assert.assertEquals(r5.int_value, (int) (Integer) map.get("int_value"));
							}
						}
					} catch (RecordNotFoundDatabaseException | ConstraintsNotRespectedDatabaseException e) {
						if (no_thread)
							throw e;
					}

                }
			}
				break;
			case 5: {
				final ArrayList<Table1.Record> removed_records = new ArrayList<>();
				table1.removeRecords(new Filter<Table1.Record>() {

					@Override
					public boolean nextRecord(Record _record) {
						boolean toremove = random.nextInt(2) == 0;
						if (toremove)
							removed_records.add(_record);
						return toremove;
					}
				});
				if (no_thread) {
					for (Table1.Record r1 : removed_records) {
						assertFalse(table1.contains(r1));
						for (Table2.Record r2 : table2.getRecords()) {
							assertFalse(table1.equals(r2.fr1_pk1, r1));
						}
					}
				} else
					table2.getRecords();
				table6.getRecords();
			}
				break;
			case 6: {
				final ArrayList<Table3.Record> removed_records = new ArrayList<>();
				table3.removeRecords(new Filter<Table3.Record>() {

					@Override
					public boolean nextRecord(Table3.Record _record) {
						boolean toremove = random.nextInt(2) == 0;
						if (toremove)
							removed_records.add(_record);
						return toremove;
					}
				});
				if (no_thread) {
					for (Table3.Record r3 : removed_records) {
						assertFalse(table3.contains(r3));
						for (Table4.Record r4 : table4.getRecords()) {
							assertFalse(table3.equals(r4.fr1_pk1, r3));
						}
						for (Table5.Record r5 : table5.getRecords()) {
							assertFalse(table3.equals(r5.fr1_pk1, r3));
						}
					}
				} else {
					table4.getRecords();
					table5.getRecords();
				}
				table6.getRecords();
			}
				break;
			case 7: {

				final ArrayList<Table4.Record> removed_records = new ArrayList<>();
				table4.removeRecords(new Filter<Table4.Record>() {

					@Override
					public boolean nextRecord(Table4.Record _record) {
						boolean toremove = random.nextInt(2) == 0;

						if (toremove)
							removed_records.add(_record);
						return toremove;
					}
				});
				if (no_thread) {
					for (Table4.Record r4 : removed_records) {
						assertFalse(table4.contains(r4));
					}
				}
			}
				break;
			case 8: {
				final ArrayList<Table2.Record> removed_records = new ArrayList<>();
				table2.removeRecords(new Filter<Table2.Record>() {

					@Override
					public boolean nextRecord(Table2.Record _record) {
						boolean toremove = random.nextInt(2) == 0;

						if (toremove)
							removed_records.add(_record);
						return toremove;
					}
				});
				if (no_thread) {
					for (Table2.Record r2 : removed_records) {
						assertFalse(table2.contains(r2));
						for (Table6.Record r6 : table6.getRecords()) {
							assertFalse(table2.equals(r6.fk1_pk1, r2));
						}
					}
				}
				table6.getRecords();
			}
				break;
			case 9: {
				final ArrayList<Table5.Record> removed_records = new ArrayList<>();
				table5.removeRecords(new Filter<Table5.Record>() {

					@Override
					public boolean nextRecord(Table5.Record _record) {
						boolean toremove = random.nextInt(2) == 0;

						if (toremove)
							removed_records.add(_record);
						return toremove;
					}
				});
				if (no_thread) {
					for (Table5.Record r5 : removed_records) {
						assertFalse(table5.contains(r5));
						for (Table6.Record r6 : table6.getRecords()) {
							assertFalse(table5.equals(r6.fk2, r5));
						}
					}
				}
				table6.getRecords();
			}
				break;
			case 10: {
				final ArrayList<Table1.Record> removed_records = new ArrayList<>();
				table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

					@Override
					public boolean nextRecord(Record _record) {
						boolean toremove = random.nextInt(2) == 0;

						if (toremove)
							removed_records.add(_record);
						return toremove;
					}
				});
				if (no_thread) {
					for (Table1.Record r1 : removed_records) {
						assertFalse(table1.contains(r1));
						for (Table2.Record r2 : table2.getRecords()) {
							assertFalse(table1.equals(r2.fr1_pk1, r1));
						}
					}
				} else {
					table2.getRecords();
				}
				table6.getRecords();
			}
				break;
			case 11: {
				table6.getRecords();
				final ArrayList<Table3.Record> removed_records = new ArrayList<>();
				table3.removeRecordsWithCascade(new Filter<Table3.Record>() {

					@Override
					public boolean nextRecord(Table3.Record _record) {
						boolean toremove = random.nextInt(2) == 0;

						if (toremove)
							removed_records.add(_record);
						return toremove;
					}
				});
				if (no_thread) {
					for (Table3.Record r3 : removed_records) {
						assertFalse(table3.contains(r3));
						for (Table4.Record r4 : table4.getRecords()) {
							assertFalse(table3.equals(r4.fr1_pk1, r3));
						}
						for (Table5.Record r5 : table5.getRecords()) {
							assertFalse(table3.equals(r5.fr1_pk1, r3));

						}
					}
				} else {
					table4.getRecords();
					table5.getRecords();
				}
				table6.getRecords();
			}
				break;
			case 12: {
				ArrayList<Table1.Record> records = table1.getRecords();
				if (records.size() > 0) {
					Table1.Record rec1 = records.get(random.nextInt(records.size()));
					HashMap<String, Object> map = new HashMap<>();
					map.put("pk1", random.nextInt());
					map.put("byte_value", (byte) 9);
					map.put("char_value", 's');
					map.put("DoubleNumber_value", 7.7);
					try {
						table1.updateRecord(rec1, map);
                        Assert.assertEquals(rec1.pk1, (int) (Integer) map.get("pk1"));
                        Assert.assertEquals(rec1.byte_value, (byte) (Byte) map.get("byte_value"));
                        Assert.assertEquals(rec1.char_value, (char) (Character) map.get("char_value"));
                        Assert.assertEquals(rec1.DoubleNumber_value, map.get("DoubleNumber_value"));
					} catch (RecordNotFoundDatabaseException | ConstraintsNotRespectedDatabaseException e) {
						if (no_thread)
							throw e;
					}
                    table2.getRecords();
					table6.getRecords();
				}
			}
				break;
			case 13: {
				ArrayList<Table3.Record> records = table3.getRecords();
				if (records.size() > 0) {
					Table3.Record rec1 = records.get(random.nextInt(records.size()));
					HashMap<String, Object> map = new HashMap<>();
					map.put("pk1", random.nextInt());
					map.put("byte_value", (byte) 9);
					map.put("char_value", 's');
					map.put("DoubleNumber_value", 7.7);
					try {
						table3.updateRecord(rec1, map);
                        Assert.assertEquals(((Integer) map.get("pk1")).intValue(), rec1.pk1);
                        Assert.assertEquals(((Byte) map.get("byte_value")).byteValue(), rec1.byte_value);
                        Assert.assertEquals(rec1.char_value, (char) (Character) map.get("char_value"));
                        Assert.assertEquals(rec1.DoubleNumber_value, map.get("DoubleNumber_value"));
					} catch (RecordNotFoundDatabaseException | ConstraintsNotRespectedDatabaseException e) {
						if (no_thread)
							throw e;
					}
                    table4.getRecords();
					table5.getRecords();
					table6.getRecords();
				}
			}
				break;
			case 14: {
				long l = table1.getRecordsNumber();
                Assert.assertEquals(0, table1.removeRecordsWithCascade(new Filter<Record>() {

                    @Override
                    public boolean nextRecord(Record _record) {
                        return false;
                    }
                }));
				if (no_thread)
					assertTrue(l == table1.getRecordsNumber());
				table2.getRecords();
				table6.getRecords();
			}
				break;
			case 15: {
				long l = table3.getRecordsNumber();
				assertTrue(table3.removeRecordsWithCascade(new Filter<Table3.Record>() {

					@Override
					public boolean nextRecord(Table3.Record _record) {
						return false;
					}
				}) == 0);
				if (no_thread)
					assertTrue(l == table3.getRecordsNumber());
				table4.getRecords();
				table5.getRecords();
				table6.getRecords();
			}
				break;
			case 16: {
				ArrayList<Table1.Record> records = table1.getRecords();
				if (records.size() > 0) {
					Table1.Record rec = records.get(random.nextInt(records.size()));
					try {
						table1.removeRecordWithCascade(rec);
						if (no_thread) {
							assertFalse(table1.contains(rec));
							for (Table2.Record r2 : table2.getRecords()) {
								assertFalse(table1.equals(r2.fr1_pk1, rec));
							}
						} else
							table2.getRecords();
						table6.getRecords();
					} catch (RecordNotFoundDatabaseException e) {
						if (no_thread) {
							throw e;
						}
					}
				}
			}
				break;
			case 17: {
				ArrayList<Table3.Record> records = table3.getRecords();
				if (records.size() > 0) {
					Table3.Record rec = records.get(random.nextInt(records.size()));
					try {
						table3.removeRecordWithCascade(rec);
						if (no_thread) {
							assertFalse(table3.contains(rec));
							for (Table4.Record r4 : table4.getRecords()) {
								assertFalse(table3.equals(r4.fr1_pk1, rec));
							}
							for (Table5.Record r5 : table5.getRecords()) {
								assertFalse(table3.equals(r5.fr1_pk1, rec));
							}
						} else {
							table4.getRecords();
							table5.getRecords();
						}
						table6.getRecords();
					} catch (RecordNotFoundDatabaseException e) {
						if (no_thread) {
							throw e;
						}
					}
				}
			}
				break;
			case 18: {
				ArrayList<Table4.Record> records = table4.getRecords();
				if (records.size() > 0) {
					Table4.Record rec = records.get(random.nextInt(records.size()));
					try {
						table4.removeRecordWithCascade(rec);
						if (no_thread)
							assertFalse(table4.contains(rec));
					} catch (RecordNotFoundDatabaseException e) {
						if (no_thread)
							throw e;
					}
				}
			}
				break;
			case 19: {
				ArrayList<Table4.Record> records = table4.getRecords();
				if (records.size() > 0) {
					Table4.Record rec = records.get(random.nextInt(records.size()));
					try {
						table4.removeRecord(rec);
						if (no_thread)
							assertFalse(table4.contains(rec));
					} catch (RecordNotFoundDatabaseException e) {
						if (no_thread)
							throw e;
					}
				}
			}
				break;
			case 20: {
				ArrayList<Table2.Record> records = table2.getRecords();
				if (records.size() > 0) {
					Table2.Record rec = records.get(random.nextInt(records.size()));
					try {
						table2.removeRecordWithCascade(rec);
						if (no_thread) {
							assertFalse(table2.contains(rec));
							for (Table6.Record r6 : table6.getRecords()) {
								assertFalse(table2.equals(r6.fk1_pk1, rec));
							}
						} else
							table6.getRecords();
					} catch (RecordNotFoundDatabaseException e) {
						if (no_thread)
							throw e;
					}
				}
			}
				break;
			case 21: {
				table1.updateRecords(new AlterRecordFilter<Table1.Record>() {

					@Override
					public void nextRecord(Record _record) {
						HashMap<String, Object> m = new HashMap<>();
						m.put("int_value", 18);
						this.update(m);
					}
				});
				if (no_thread) {
					ArrayList<Table1.Record> records1 = table1.getRecords();
					for (Table1.Record r1 : records1)
                        Assert.assertEquals(18, r1.int_value);
				}

				table3.updateRecords(new AlterRecordFilter<Table3.Record>() {

					@Override
					public void nextRecord(Table3.Record _record) {
						HashMap<String, Object> m = new HashMap<>();
						m.put("int_value", 18);
						this.update(m);
					}
				});
				if (no_thread) {
					ArrayList<Table3.Record> records3 = table3.getRecords();
					for (Table3.Record r3 : records3)
                        Assert.assertEquals(18, r3.int_value);
				}

			}
				break;
			case 22: {
				{
					ArrayList<Table1.Record> recs = table1.getRecords(new Filter<Table1.Record>() {
						private int val = 0;

						@Override
						public boolean nextRecord(Table1.Record _record) {
							if ((val++) % 3 == 0)
								return true;
							return false;
						}
					});
					try {
						if (recs.size() > 0) {
							table1.removeRecordsWithCascade(recs);
							if (no_thread) {
								for (Table1.Record rec : recs) {
									assertFalse(table1.contains(rec));
									for (Table2.Record r2 : table2.getRecords()) {
										assertFalse(table1.equals(r2.fr1_pk1, rec));
									}
								}
							} else
								table2.getRecords();
							table6.getRecords();
						}
					} catch (RecordNotFoundDatabaseException e) {
						if (no_thread) {
							throw e;
						}
					}
				}
			}
				break;
			case 23: {
				{
					ArrayList<Table3.Record> recs = table3.getRecords(new Filter<Table3.Record>() {
						private int val = 0;

						@Override
						public boolean nextRecord(Table3.Record _record) {
							if ((val++) % 3 == 0)
								return true;
							return false;
						}
					});
					try {
						if (recs.size() > 0) {
							table3.removeRecordsWithCascade(recs);
							if (no_thread) {
								for (Table3.Record rec : recs) {
									assertFalse(table3.contains(rec));
									for (Table4.Record r4 : table4.getRecords()) {
										assertFalse(table3.equals(r4.fr1_pk1, rec));
									}
									for (Table5.Record r5 : table5.getRecords()) {
										assertFalse(table3.equals(r5.fr1_pk1, rec));
									}
								}
							} else {
								table4.getRecords();
								table5.getRecords();
							}
							table6.getRecords();
						}
					} catch (RecordNotFoundDatabaseException e) {
						if (no_thread) {
							throw e;
						}
					}
				}
			}
				break;
			case 24: {
				{
					ArrayList<Table1.Record> recs = table1.getRecords(new Filter<Table1.Record>() {
						private int val = 0;

						@Override
						public boolean nextRecord(Table1.Record _record) {
							if ((val++) % 3 == 0)
								return true;
							return false;
						}
					});
					for (Iterator<Table1.Record> it = recs.iterator(); it.hasNext();) {
						if (table1.isRecordPointedByForeignKeys(it.next()))
							it.remove();
					}
					try {
						if (recs.size() > 0) {
							table1.removeRecords(recs);
							if (no_thread) {
								for (Table1.Record rec : recs) {
									assertFalse(table1.contains(rec));
									for (Table2.Record r2 : table2.getRecords()) {
										assertFalse(table1.equals(r2.fr1_pk1, rec));
									}
								}
							} else
								table2.getRecords();
							table6.getRecords();
						}
					} catch (RecordNotFoundDatabaseException | ConstraintsNotRespectedDatabaseException e) {
						if (no_thread) {
							throw e;
						}
					}
                }
			}
				break;
			case 25: {
				{
					ArrayList<Table3.Record> recs = table3.getRecords(new Filter<Table3.Record>() {
						private int val = 0;

						@Override
						public boolean nextRecord(Table3.Record _record) {
							if ((val++) % 3 == 0)
								return true;
							return false;
						}
					});
					for (Iterator<Table3.Record> it = recs.iterator(); it.hasNext();) {
						if (table3.isRecordPointedByForeignKeys(it.next()))
							it.remove();
					}

					try {
						if (recs.size() > 0) {
							table3.removeRecords(recs);
							if (no_thread) {
								for (Table3.Record rec : recs) {
									assertFalse(table3.contains(rec));
									for (Table4.Record r4 : table4.getRecords()) {
										assertFalse(table3.equals(r4.fr1_pk1, rec));
									}
									for (Table5.Record r5 : table5.getRecords()) {
										assertFalse(table3.equals(r5.fr1_pk1, rec));
									}
								}
							} else {
								table4.getRecords();
								table5.getRecords();
							}
							table6.getRecords();
						}
					} catch (RecordNotFoundDatabaseException | ConstraintsNotRespectedDatabaseException e) {
						if (no_thread) {
							throw e;
						}
					}
                }
			}
				break;
			case 26: {
				assertFalse(table1.hasRecords(new Filter<Table1.Record>() {

					@Override
					public boolean nextRecord(Table1.Record _record) {
						return false;
					}
				}));
				if (no_thread) {
					if (table1.getRecordsNumber() > 0) {
						assertTrue(table1.hasRecords(new Filter<Table1.Record>() {

							@Override
							public boolean nextRecord(Table1.Record _record) {
								return true;
							}
						}));
					}
				}
				assertFalse(table2.hasRecords(new Filter<Table2.Record>() {

					@Override
					public boolean nextRecord(Table2.Record _record) {
						return false;
					}
				}));
				if (no_thread) {
					if (table2.getRecordsNumber() > 0) {

						assertTrue(table2.hasRecords(new Filter<Table2.Record>() {

							@Override
							public boolean nextRecord(Table2.Record _record) {
								return true;
							}
						}));
					}
				}
				assertFalse(table3.hasRecords(new Filter<Table3.Record>() {

					@Override
					public boolean nextRecord(Table3.Record _record) {
						return false;
					}
				}));
				if (no_thread) {
					if (table3.getRecordsNumber() > 0) {

						assertTrue(table3.hasRecords(new Filter<Table3.Record>() {

							@Override
							public boolean nextRecord(Table3.Record _record) {
								return true;
							}
						}));
					}
				}
				assertFalse(table4.hasRecords(new Filter<Table4.Record>() {

					@Override
					public boolean nextRecord(Table4.Record _record) {
						return false;
					}
				}));
				if (no_thread) {
					if (table4.getRecordsNumber() > 0) {

						assertTrue(table4.hasRecords(new Filter<Table4.Record>() {

							@Override
							public boolean nextRecord(Table4.Record _record) {
								return true;
							}
						}));
					}
				}
				assertFalse(table5.hasRecords(new Filter<Table5.Record>() {

					@Override
					public boolean nextRecord(Table5.Record _record) {
						return false;
					}
				}));
				if (no_thread) {
					if (table6.getRecordsNumber() > 0) {

						assertTrue(table5.hasRecords(new Filter<Table5.Record>() {

							@Override
							public boolean nextRecord(Table5.Record _record) {
								return true;
							}
						}));
					}
				}
				assertFalse(table6.hasRecords(new Filter<Table6.Record>() {

					@Override
					public boolean nextRecord(Table6.Record _record) {
						return false;
					}
				}));
				if (no_thread) {
					if (table6.getRecordsNumber() > 0) {

						assertTrue(table6.hasRecords(new Filter<Table6.Record>() {

							@Override
							public boolean nextRecord(Table6.Record _record) {
								return true;
							}
						}));
					}
				}

			}
				break;
			case 27: {
				ArrayList<Table1.Record> r1 = table1.getOrderedRecords(new Filter<Table1.Record>() {

					@Override
					public boolean nextRecord(Table1.Record _record) {
						return random.nextInt(2) == 0;
					}
				}, true, "subField.short_value", "LongNumber_value");
				testOrderedTable1(r1);
				r1 = table1.getOrderedRecords(new Filter<Table1.Record>() {

					@Override
					public boolean nextRecord(Table1.Record _record) {
						return random.nextInt(2) == 0;
					}
				}, false, "subField.short_value", "LongNumber_value");
				testInverseOrderedTable1(r1);

				r1 = table1.getOrderedRecords(true, "subField.short_value", "LongNumber_value");
				testOrderedTable1(r1);
				if (no_thread) {
					ArrayList<Table1.Record> r1b = table1.getOrderedRecords(table1.getRecords(), true, "subField.short_value",
							"LongNumber_value");
                    Assert.assertEquals(r1.size(), r1b.size());

					for (int i = 0; i < r1.size(); i++) {
						Assert.assertEquals(r1.get(i).subField.short_value, r1b.get(i).subField.short_value);
						Assert.assertEquals(r1.get(i).LongNumber_value, r1b.get(i).LongNumber_value);
						// Assert.assertTrue(table1.equalsAllFields(r1.get(i), r1b.get(i)),
						// "r1_short_value="+r1.get(i).short_value+";
						// r1LongNumber_value="+r1.get(i).LongNumber_value+"-----------"+"r1b_short_value="+r1b.get(i).short_value+";
						// r1bLongNumber_value="+r1b.get(i).LongNumber_value);
					}
				}
				r1 = table1.getOrderedRecords(false, "subField.short_value", "LongNumber_value");
				testInverseOrderedTable1(r1);
				if (no_thread) {
					ArrayList<Table1.Record> r1b = table1.getOrderedRecords(table1.getRecords(), false, "subField.short_value",
							"LongNumber_value");
					assertTrue(r1.size() == r1b.size());

					for (int i = 0; i < r1.size(); i++) {
						Assert.assertEquals(r1.get(i).subField.short_value, r1b.get(i).subField.short_value);
						Assert.assertEquals(r1.get(i).LongNumber_value, r1b.get(i).LongNumber_value);
						// Assert.assertTrue(table1.equalsAllFields(r1.get(i), r1b.get(i)));
					}
				}
			} {
				ArrayList<Table3.Record> r3 = table3.getOrderedRecords(new Filter<Table3.Record>() {

					@Override
					public boolean nextRecord(Table3.Record _record) {
						return random.nextInt(2) == 0;
					}
				}, true, "subField.short_value", "LongNumber_value");
				testOrderedTable3(r3);
				GroupedResults<Table3.Record> grouped_results = table3.getGroupedResults(r3, "FloatNumber_value");
				for (GroupedResults<Table3.Record>.Group l : grouped_results.getGroupedResults()) {
					testOrderedTable3(l.getResults());
				}

				r3 = table3.getOrderedRecords(new Filter<Table3.Record>() {

					@Override
					public boolean nextRecord(Table3.Record _record) {
						return random.nextInt(2) == 0;
					}
				}, false, "subField.short_value", "LongNumber_value");
				testInverseOrderedTable3(r3);
				grouped_results = table3.getGroupedResults(r3, "FloatNumber_value", "BigInteger_value");
				for (GroupedResults<Table3.Record>.Group l : grouped_results.getGroupedResults()) {
					testInverseOrderedTable3(l.getResults());
				}
				grouped_results = table3.getGroupedResults(r3, "subField.string_value");
				for (GroupedResults<Table3.Record>.Group l : grouped_results.getGroupedResults()) {
					testInverseOrderedTable3(l.getResults());
				}
				table3.getOrderedRecords(true, "DateValue", "LongNumber_value");

				r3 = table3.getOrderedRecords(true, "subField.short_value", "LongNumber_value");
				testOrderedTable3(r3);
				if (no_thread) {
					ArrayList<Table3.Record> r3b = table3.getOrderedRecords(table3.getRecords(), true, "subField.short_value",
							"LongNumber_value");
					assertTrue(r3.size() == r3b.size());

					for (int i = 0; i < r3.size(); i++) {
						assertTrue(table3.equalsAllFields(r3.get(i), r3b.get(i)));
					}
				}
				r3 = table3.getOrderedRecords(false, "subField.short_value", "LongNumber_value");
				testInverseOrderedTable3(r3);
				if (no_thread) {
					ArrayList<Table3.Record> r3b = table3.getOrderedRecords(table3.getRecords(), false, "subField.short_value",
							"LongNumber_value");
                    Assert.assertEquals(r3.size(), r3b.size());

					for (int i = 0; i < r3.size(); i++) {
						assertTrue(table3.equalsAllFields(r3.get(i), r3b.get(i)));
					}
				}
			}
				break;
			case 28: {
				table1.updateRecords(new AlterRecordFilter<Table1.Record>() {

					@Override
					public void nextRecord(Record _record) {
						switch (random.nextInt(4)) {
						case 1:
							this.remove();
							break;
						case 2:
							this.removeWithCascade();
							break;
						}
					}
				});

				table3.updateRecords(new AlterRecordFilter<Table3.Record>() {

					@Override
					public void nextRecord(Table3.Record _record) {
						switch (random.nextInt(4)) {
						case 1:
							this.remove();
							break;
						case 2:
							this.removeWithCascade();
							break;
						}
					}
				});

			}
				break;
			case 29: {
				/*
				 * try(TableIterator<Table1.Record> it=table1.getIterator()) { while
				 * (it.hasNext()) it.next(); try(TableIterator<Table3.Record>
				 * it2=table3.getIterator()) { while (it2.hasNext()) it2.next(); } }
				 * try(TableIterator<Table2.Record> it=table2.getIterator()) { while
				 * (it.hasNext()) it.next(); } try(TableIterator<Table4.Record>
				 * it=table4.getIterator()) { while (it.hasNext()) it.next(); }
				 * try(TableIterator<Table5.Record> it=table5.getIterator()) { while
				 * (it.hasNext()) it.next(); } try(TableIterator<Table6.Record>
				 * it=table6.getIterator()) { while (it.hasNext()) it.next(); }
				 */
			}
				break;
			case 30: {
				byte[] tab = new byte[3];
				tab[0] = 0;
				tab[1] = 1;
				tab[2] = 2;
				Object[] parameters = { "pk1", random.nextInt(), "int_value", 3, "byte_value",
                        (byte) 3, "char_value", 'x', "boolean_value", Boolean.TRUE,
						"short_value", (short) 3, "long_value", 3L, "float_value", 3.3f,
						"double_value", 3.3, "string_value", "test string",
						"IntegerNumber_value", 3, "ByteNumber_value", (byte) 3,
						"CharacterNumber_value", 'x', "BooleanNumber_value", Boolean.TRUE,
						"ShortNumber_value", (short) 3, "LongNumber_value", (long) 3,
						"FloatNumber_value", 3.3f, "DoubleNumber_value", 3.3, "BigInteger_value",
						new BigInteger("5"), "BigDecimal_value", new BigDecimal("8.8"), "DateValue", date.clone(),
						"CalendarValue", calendar.clone(), "secretKey", secretKey, "typeSecretKey", typeSecretKey,
						"byte_array_value", tab.clone(), "subField", subField, "subSubField", subSubField, "file", fileTest };

				try {
					Table1.Record r1 = table1.addRecord(parameters);
					Table3.Record r3 = table3.addRecord(parameters);
					if (no_thread) {
						assertTrue(table1.contains(r1));
						assertTrue(table3.contains(r3));
					}
				} catch (ConstraintsNotRespectedDatabaseException e) {
					if (no_thread) {
						throw e;
					}
				}

				try {
					Table1.Record r1 = table1.addRecord(parameters);
					Table3.Record r2 = table3.addRecord(parameters);
					table1.removeRecord(r1);
					table3.removeRecord(r2);
				} catch (ConstraintsNotRespectedDatabaseException ignored) {

				} catch (RecordNotFoundDatabaseException e) {
					if (no_thread)
						throw e;
				}

				Object[] p2 = { "rert" };
				Object[] p3 = {125, "rert" };
				try {
					table1.addRecord(p2);
                    fail();
				} catch (Exception e) {
					assertTrue(true);
				}
				try {
					table1.addRecord(p3);
                    fail();
				} catch (Exception e) {
					assertTrue(true);
				}
				try {
					table3.addRecord(p2);
                    fail();
				} catch (Exception e) {
					assertTrue(true);
				}
				try {
					table3.addRecord(p3);
                    fail();
				} catch (Exception e) {
					assertTrue(true);
				}
			}
				break;
			case 31: {
				Table1.Record r1 = new Table1.Record();

				r1.pk1 = random.nextInt();
				r1.int_value = 3;
				r1.byte_value = (byte) 3;
				r1.char_value = 'x';
				r1.boolean_value = true;
				r1.short_value = (short) random.nextInt();
				r1.long_value = 3;
				r1.float_value = 3.3f;
				r1.double_value = 3.3;
				r1.string_value = "test string";
				r1.IntegerNumber_value = 3;
				r1.ByteNumber_value = (byte) 3;
				r1.CharacterNumber_value = 'x';
				r1.BooleanNumber_value = Boolean.TRUE;
				r1.ShortNumber_value = (short) 3;
				r1.LongNumber_value = random.nextLong();
				r1.FloatNumber_value = 3.3f;
				r1.DoubleNumber_value = 3.3;
				r1.BigInteger_value = new BigInteger("6");
				r1.BigDecimal_value = new BigDecimal("1.10");
				r1.DateValue = Calendar.getInstance().getTime();
				r1.CalendarValue = Calendar.getInstance();
				r1.secretKey = secretKey;
				r1.typeSecretKey = typeSecretKey;
				r1.subField = subField;
				r1.subSubField = subSubField;
				r1.file=fileTest;

				byte[] tab = new byte[3];
				tab[0] = 0;
				tab[1] = 1;
				tab[2] = 2;
				r1.byte_array_value = tab;

				Table3.Record r3 = new Table3.Record();

				r3.pk1 = random.nextInt();
				r3.int_value = 3;
				r3.byte_value = (byte) 3;
				r3.char_value = 'x';
				r3.boolean_value = true;
				r3.short_value = (short) random.nextInt();
				r3.long_value = 3;
				r3.float_value = 3.3f;
				r3.double_value = 3.3;
				r3.string_value = "test string";
				r3.IntegerNumber_value = 3;
				r3.ByteNumber_value = (byte) 3;
				r3.CharacterNumber_value = 'x';
				r3.BooleanNumber_value = Boolean.TRUE;
				r3.ShortNumber_value = (short) 3;
				r3.LongNumber_value = random.nextLong();
				r3.FloatNumber_value = 3.3f;
				r3.DoubleNumber_value = 3.3;
				r3.BigInteger_value = new BigInteger("6");
				r3.BigDecimal_value = new BigDecimal("1.10");
				r3.DateValue = Calendar.getInstance().getTime();
				r3.CalendarValue = Calendar.getInstance();
				r3.secretKey = secretKey;
				r3.typeSecretKey = typeSecretKey;
				r3.byte_array_value = tab;
				r3.subField = subField;
				r3.subSubField = subSubField;
				r3.file=fileTest;
				try {
					Table1.Record r1b = table1.addRecord(r1);
					Table3.Record r3b = table3.addRecord(r3);
					if (no_thread) {
						assertTrue(table1.contains(r1));
						assertTrue(table3.contains(r3));
					}
                    assertSame(r1, r1b);
                    assertSame(r3, r3b);
				} catch (ConstraintsNotRespectedDatabaseException e) {
					if (no_thread) {
						throw e;
					}
				}
			}
				break;
			case 32: {
				try {
					ArrayList<Table1.Record> records1 = table1.getRecords();
					if (records1.size() > 0) {

						Table1.Record r1 = records1.get(0);
						r1.long_value = 123456789L;
						table1.updateRecord(r1);
						if (no_thread) {
							Table1.Record r1b = table1.getRecord("pk1", r1.pk1, "pk2", r1.pk2, "pk3", r1.pk3, "pk4", r1.pk4, "pk5", r1.pk5, "pk6", r1.pk6, "pk7", r1.pk7);
                            Assert.assertEquals(r1b.long_value, r1.long_value);
						}
					}
					ArrayList<Table3.Record> records3 = table3.getRecords();
					if (records3.size() > 0) {

						Table3.Record r3 = records3.get(0);
						r3.long_value = 123456789L;
						table3.updateRecord(r3);
						if (no_thread) {
							Table3.Record r3b = table3.getRecord("pk1", r3.pk1, "pk2", r3.pk2, "pk3", r3.pk3, "pk4", r3.pk4, "pk5", r3.pk5, "pk6", r3.pk6, "pk7", r3.pk7);
                            Assert.assertEquals(r3b.long_value, r3.long_value);
						}
					}
				} catch (RecordNotFoundDatabaseException e) {
					if (no_thread)
						throw e;
				}
			}
				break;

			}
		} else {
			System.out.println("Test " + number + " number " + r + " skipped.");
		}
		try {
			table1.checkDataIntegrity();
			table3.checkDataIntegrity();
			table2.checkDataIntegrity();
			table4.checkDataIntegrity();
			table5.checkDataIntegrity();
			table6.checkDataIntegrity();
		} catch (DatabaseException e) {
			System.out.println("Exception in test number " + r);
			e.printStackTrace();
			throw e;
		}
		if (table1.getRecordsNumber() == 0 || table2.getRecordsNumber() == 0 || table3.getRecordsNumber() == 0
				|| table4.getRecordsNumber() == 0 || table5.getRecordsNumber() == 0 || table6.getRecordsNumber() == 0) {
			System.out.println("\tAdding new records.");
			prepareMultipleTest();
		}
		System.out.println("\tTest " + number + " number " + r + " OK!!!");
	}

	@Test(threadPoolSize = 4, invocationCount = 4, timeOut = 2000000, dependsOnMethods = { "multipleTests" })
	public void testThreadSafe() {
		try {
			no_thread = false;
			System.out.println("No thread=" + no_thread);
			for (int i = 0; i < getThreadTestsNumber(); i++)
				subMultipleTests();
		} catch (Exception e) {
			e.printStackTrace();
			fail("testThreadSafe", e);
		}
	}

	@Test(threadPoolSize = 1, dependsOnMethods = { "testThreadSafe" })
	public void testCheckPoint() throws DatabaseException {
		if (table1.getDatabaseWrapper() instanceof EmbeddedHSQLDBWrapper) {
			((EmbeddedHSQLDBWrapper) table1.getDatabaseWrapper()).checkPoint(false);
			((EmbeddedHSQLDBWrapper) table1.getDatabaseWrapper()).checkPoint(true);
		}
	}

	@Test(threadPoolSize = 1, dependsOnMethods = { "testCheckPoint" })
	public void testBackup() throws DatabaseException {
		table1.getDatabaseWrapper().nativeBackup(getDatabaseBackupFileName());
		table1.checkDataIntegrity();
		table2.checkDataIntegrity();
		table3.checkDataIntegrity();
		table4.checkDataIntegrity();
		table5.checkDataIntegrity();
		table6.checkDataIntegrity();
		table7.checkDataIntegrity();
	}

	@Test(threadPoolSize = 1, dependsOnMethods = { "testBackup" })
	public void testDatabaseRemove() throws DatabaseException {
		sql_db.deleteDatabase(dbConfig1);
		try {
			sql_db.loadDatabase(dbConfig1, false);
			fail();
		} catch (Exception e) {

		}
	}

	@Test(invocationCount = 0, timeOut = 0)
	private void testOrderedTable1(ArrayList<Table1.Record> res) {
		if (res.size() > 1) {
			for (int i = 1; i < res.size(); i++) {
				assertTrue(res.get(i - 1).subField.short_value <= res.get(i).subField.short_value);
				assertTrue(res.get(i - 1).subField.short_value != res.get(i).subField.short_value
						|| (res.get(i - 1).subField.short_value == res.get(i).subField.short_value) && res.get(i - 1).LongNumber_value <= res.get(i).LongNumber_value);
			}
		}
	}

	@Test(invocationCount = 0, timeOut = 0)
	private void testInverseOrderedTable1(ArrayList<Table1.Record> res) {
		if (res.size() > 1) {
			for (int i = 1; i < res.size(); i++) {
				assertTrue(res.get(i - 1).subField.short_value >= res.get(i).subField.short_value);
				assertTrue(res.get(i - 1).subField.short_value != res.get(i).subField.short_value
						|| (res.get(i - 1).subField.short_value == res.get(i).subField.short_value) && res.get(i - 1).LongNumber_value >= res.get(i).LongNumber_value);
			}
		}
	}

	@Test(invocationCount = 0, timeOut = 0)
	private void testOrderedTable3(ArrayList<Table3.Record> res) {
		if (res.size() > 1) {
			for (int i = 1; i < res.size(); i++) {
				assertTrue(res.get(i - 1).subField.short_value <= res.get(i).subField.short_value);
				assertTrue(res.get(i - 1).subField.short_value != res.get(i).subField.short_value
						|| (res.get(i - 1).subField.short_value == res.get(i).subField.short_value) && res.get(i - 1).LongNumber_value <= res.get(i).LongNumber_value);
			}
		}
	}

	@Test(invocationCount = 0, timeOut = 0)
	private void testInverseOrderedTable3(ArrayList<Table3.Record> res) {
		if (res.size() > 1) {
			for (int i = 1; i < res.size(); i++) {
				assertTrue(res.get(i - 1).subField.short_value >= res.get(i).subField.short_value);
				assertTrue(res.get(i - 1).subField.short_value != res.get(i).subField.short_value
						|| (res.get(i - 1).subField.short_value == res.get(i).subField.short_value) && res.get(i - 1).LongNumber_value >= res.get(i).LongNumber_value);
			}
		}
	}

}
