
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

package com.distrimind.ood.tests;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.distrimind.ood.database.AlterRecordFilter;
import com.distrimind.ood.database.DatabaseConfiguration;
import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.Filter;
import com.distrimind.ood.database.GroupedResults;
import com.distrimind.ood.database.EmbeddedHSQLDBWrapper;
import com.distrimind.ood.database.DatabaseWrapper;
import com.distrimind.ood.database.Table;
import com.distrimind.ood.database.exceptions.ConcurentTransactionDatabaseException;
import com.distrimind.ood.database.exceptions.ConstraintsNotRespectedDatabaseException;
import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.ood.database.exceptions.FieldDatabaseException;
import com.distrimind.ood.database.exceptions.RecordNotFoundDatabaseException;
import com.distrimind.ood.database.fieldaccessors.FieldAccessor;
import com.distrimind.ood.interpreter.Interpreter;
import com.distrimind.ood.interpreter.SymbolType;
import com.distrimind.ood.tests.database.SubField;
import com.distrimind.ood.tests.database.SubSubField;
import com.distrimind.ood.tests.database.Table1;
import com.distrimind.ood.tests.database.Table2;
import com.distrimind.ood.tests.database.Table3;
import com.distrimind.ood.tests.database.Table4;
import com.distrimind.ood.tests.database.Table5;
import com.distrimind.ood.tests.database.Table6;
import com.distrimind.ood.tests.database.Table7;
import com.distrimind.ood.tests.database.Table1.Record;
import com.distrimind.ood.tests.schooldatabase.Lecture;
import com.distrimind.util.AbstractDecentralizedID;
import com.distrimind.util.DecentralizedIDGenerator;
import com.distrimind.util.RenforcedDecentralizedIDGenerator;
import com.distrimind.util.SecuredDecentralizedID;
import com.distrimind.util.crypto.AbstractSecureRandom;
import com.distrimind.util.crypto.SecureRandomType;
import com.distrimind.util.crypto.SymmetricEncryptionType;
import com.distrimind.util.crypto.SymmetricSecretKey;

import gnu.vm.jgnu.security.NoSuchAlgorithmException;
import gnu.vm.jgnu.security.NoSuchProviderException;

/**
 * 
 * @author Jason Mahdjoub
 * @version 2.0
 * @since OOD 1.0
 */
public abstract class TestDatabase
{
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
    
    static DatabaseConfiguration dbConfig1=new DatabaseConfiguration(Table1.class.getPackage());
    static DatabaseConfiguration dbConfig2=new DatabaseConfiguration(Lecture.class.getPackage());
    private static DatabaseWrapper sql_db;
    private static DatabaseWrapper sql_dbb;
    
    
    public abstract File getDatabaseBackupFileName();
    
    public static final AbstractSecureRandom secureRandom;
    static
    {
	AbstractSecureRandom rand=null;
	try
	{
	    rand=SecureRandomType.DEFAULT.getInstance();
	}
	catch(Exception e)
	{
	    e.printStackTrace();
	}
	secureRandom=rand;
    }
    
    public static SubField getSubField() throws DatabaseException 
    {
	try
	{
	    
	SubField res=new SubField();
	res.BigDecimal_value=new BigDecimal(3.0);
	res.BigInteger_value=new BigInteger("54");
	res.boolean_value=false;
	res.BooleanNumber_value=new Boolean(true);
	res.byte_array_value=new byte[]{1,2,3,4,5};
	res.byte_value=25;
	res.ByteNumber_value=new Byte((byte)98);
	res.CalendarValue=Calendar.getInstance();
	res.char_value='f';
	res.CharacterNumber_value=new Character('c');
	res.DateValue=new Date();
	res.double_value=0.245;
	res.DoubleNumber_value=new Double(0.4);
	res.float_value=0.478f;
	res.FloatNumber_value=new Float(0.86f);
	res.int_value=5244;
	res.IntegerNumber_value=new Integer(2214);
	res.long_value=254545;
	res.LongNumber_value=new Long(1452);
	res.secretKey=SymmetricEncryptionType.AES.getKeyGenerator(secureRandom).generateKey();
	res.typeSecretKey=SymmetricEncryptionType.AES;
	res.subField=getSubSubField();
	res.string_value="not null";
	res.ShortNumber_value=new Short((short)12);
	return res;
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
	
	
    }
    
    public static SubSubField getSubSubField() throws DatabaseException
    {
	try
	{
	SubSubField res=new SubSubField();
	res.BigDecimal_value=new BigDecimal(3.0);
	res.BigInteger_value=new BigInteger("54");
	res.boolean_value=false;
	res.BooleanNumber_value=new Boolean(true);
	res.byte_array_value=new byte[]{1,2,3,4,5};
	res.byte_value=25;
	res.ByteNumber_value=new Byte((byte)98);
	res.CalendarValue=Calendar.getInstance();
	res.char_value='f';
	res.CharacterNumber_value=new Character('c');
	res.DateValue=new Date();
	res.double_value=0.245;
	res.DoubleNumber_value=new Double(0.4);
	res.float_value=0.478f;
	res.FloatNumber_value=new Float(0.86f);
	res.int_value=5244;
	res.IntegerNumber_value=new Integer(2214);
	res.long_value=254545;
	res.LongNumber_value=new Long(1452);
	res.secretKey=SymmetricEncryptionType.AES.getKeyGenerator(secureRandom).generateKey();
	res.typeSecretKey=SymmetricEncryptionType.AES;
	res.string_value="not null";
	res.ShortNumber_value=new Short((short)12);
	return res;
	}
	catch(Exception e)
	{
	    throw DatabaseException.getDatabaseException(e);
	}
    }
    
    public static void assertEquals(SubField actual, SubField expected)
    {
	Assert.assertEquals(actual.boolean_value, expected.boolean_value);
	Assert.assertEquals(actual.byte_value, expected.byte_value);
	Assert.assertEquals(actual.char_value, expected.char_value);
	Assert.assertEquals(new Double(actual.double_value), new Double(expected.double_value));
	Assert.assertEquals(new Float(actual.float_value), new Float(expected.float_value));
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
	/*Assert.assertEquals(actual.CalendarValue, expected.CalendarValue);
	Assert.assertEquals(actual.DateValue, expected.DateValue);*/
	//Assert.assertEquals(actual.secretKey, expected.secretKey);
	Assert.assertEquals(actual.typeSecretKey, expected.typeSecretKey);
	assertEquals(actual.subField, expected.subField);
    }

    public static void assertEquals(SubSubField actual, SubSubField expected)
    {
	Assert.assertEquals(actual.boolean_value, expected.boolean_value);
	Assert.assertEquals(actual.byte_value, expected.byte_value);
	Assert.assertEquals(actual.char_value, expected.char_value);
	Assert.assertEquals(new Double(actual.double_value), new Double(expected.double_value));
	Assert.assertEquals(new Float(actual.float_value), new Float(expected.float_value));
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
	/*Assert.assertEquals(actual.CalendarValue, expected.CalendarValue);
	Assert.assertEquals(actual.DateValue, expected.DateValue);*/
	//Assert.assertEquals(actual.secretKey, expected.secretKey);
	Assert.assertEquals(actual.typeSecretKey, expected.typeSecretKey);
    }
    
    public TestDatabase() throws DatabaseException, NoSuchAlgorithmException, gnu.vm.jgnu.security.NoSuchProviderException
    {
	typeSecretKey=SymmetricEncryptionType.AES;
	secretKey=typeSecretKey.getKeyGenerator(SecureRandomType.DEFAULT.getInstance()).generateKey();
	subField=getSubField();
	subSubField=getSubSubField();
    }
    @Override
    public void finalize() throws DatabaseException
    {
	unloadDatabase();
    }
    
    
    @SuppressWarnings("unused")
    @AfterClass 
    public static void unloadDatabase() throws DatabaseException
    {
	System.out.println("Unload database !");
	if (sql_db!=null)
	{
	    sql_db.close();
	    sql_db=null;
	}
	if (sql_dbb!=null)
	{
	    sql_dbb.close();
	    sql_dbb=null;
	}
	
	
	
    }
    
    public abstract DatabaseWrapper getDatabaseWrapperInstanceA() throws IllegalArgumentException, DatabaseException;
    public abstract DatabaseWrapper getDatabaseWrapperInstanceB() throws IllegalArgumentException, DatabaseException;
    public abstract void deleteDatabaseFilesA() throws IllegalArgumentException, DatabaseException;
    public abstract void deleteDatabaseFilesB() throws IllegalArgumentException, DatabaseException;
    
    @Test public void checkUnloadedDatabase() throws IllegalArgumentException, DatabaseException 
    {
	deleteDatabaseFilesA();
	sql_db=getDatabaseWrapperInstanceA();
	try
	{
	    sql_db.loadDatabase(dbConfig1, false);
	    Assert.fail();
	}
	catch(DatabaseException e)
	{
	    
	}
    }
    
    @Test(dependsOnMethods={"checkUnloadedDatabase"}) public void firstLoad() throws IllegalArgumentException, DatabaseException
    {
	
	    
	    
	    sql_db.loadDatabase(dbConfig1, true);
	    table2=(Table2)sql_db.getTableInstance(Table2.class);
	    table1=(Table1)sql_db.getTableInstance(Table1.class);
	    table3=(Table3)sql_db.getTableInstance(Table3.class);
	    table4=(Table4)sql_db.getTableInstance(Table4.class);
	    table5=(Table5)sql_db.getTableInstance(Table5.class);
	    table6=(Table6)sql_db.getTableInstance(Table6.class);
	    table7=(Table7)sql_db.getTableInstance(Table7.class);
    }
    
    @Test(dependsOnMethods={"firstLoad"}) public void isLoadedIntoMemory()
    {
	Assert.assertTrue(!table1.isLoadedInMemory());
	Assert.assertTrue(!table2.isLoadedInMemory());
	Assert.assertTrue(table3.isLoadedInMemory());
	Assert.assertTrue(!table4.isLoadedInMemory());
	Assert.assertTrue(table5.isLoadedInMemory());
    }
    Date date=Calendar.getInstance().getTime();
    Calendar calendar=Calendar.getInstance();
    final SymmetricEncryptionType typeSecretKey;
    final SymmetricSecretKey secretKey;
    final SubField subField;
    final SubSubField subSubField;
    
    @Test(dependsOnMethods={"firstLoad"}) public void firstAdd() throws DatabaseException
    {
	    HashMap<String, Object> map=new HashMap<String, Object>();
	    map.put("pk1", new Integer(0));
	    map.put("int_value", new Integer(3));
	    map.put("byte_value", new Byte((byte)3));
	    map.put("char_value", new Character('x'));
	    map.put("boolean_value", new Boolean(true));
	    map.put("short_value", new Short((short)3));
	    map.put("long_value", new Long(3));
	    map.put("float_value", new Float(3.3f));
	    map.put("double_value", new Double(3.3));
	    map.put("string_value", new String("test string"));
	    map.put("IntegerNumber_value", new Integer(3));
	    map.put("ByteNumber_value", new Byte((byte)3));
	    map.put("CharacterNumber_value", new Character('x'));
	    map.put("BooleanNumber_value", new Boolean(true));
	    map.put("ShortNumber_value", new Short((short)3));
	    map.put("LongNumber_value", new Long((long)3));
	    map.put("FloatNumber_value", new Float(3.3f));
	    map.put("DoubleNumber_value", new Double(3.3));
	    map.put("BigInteger_value", new BigInteger("5"));
	    map.put("BigDecimal_value", new BigDecimal("8.8"));
	    map.put("DateValue", date);
	    map.put("CalendarValue", calendar);
	    map.put("secretKey", secretKey);
	    map.put("typeSecretKey", typeSecretKey);
	    map.put("secretKey", secretKey);
	    map.put("typeSecretKey", typeSecretKey);
	    map.put("subField", subField);
	    map.put("subSubField", subSubField);
	    byte[] tab=new byte[3];
	    tab[0]=0;
	    tab[1]=1;
	    tab[2]=2;
	    map.put("byte_array_value", tab);
	    Table1.Record r1=table1.addRecord(map);
	    Table3.Record r2=table3.addRecord(map);
	    Assert.assertTrue(r1.pk1==0);
	    Assert.assertTrue(r2.pk1==0);
	    
	    Assert.assertTrue(r1.pk2==1);
	    Assert.assertTrue(r2.pk2==1);
    }
    @Test(dependsOnMethods={"firstAdd"}) public void firstTestSize() throws DatabaseException
    {
	Assert.assertTrue(table1.getRecordsNumber()==1);
	Assert.assertTrue(table3.getRecordsNumber()==1);
    }
    @Test(dependsOnMethods={"firstTestSize"}) public void firstReload() throws DatabaseException
    {
	sql_db.close();
	    sql_db=getDatabaseWrapperInstanceA();
	    sql_db.loadDatabase(dbConfig1, false);
	    sql_db.loadDatabase(dbConfig2, true);
	    
	    table2=(Table2)sql_db.getTableInstance(Table2.class);
	    table1=(Table1)sql_db.getTableInstance(Table1.class);
	    table3=(Table3)sql_db.getTableInstance(Table3.class);
	    table4=(Table4)sql_db.getTableInstance(Table4.class);
	    table5=(Table5)sql_db.getTableInstance(Table5.class);
	    table6=(Table6)sql_db.getTableInstance(Table6.class);
	    table7=(Table7)sql_db.getTableInstance(Table7.class);
	    sql_dbb=getDatabaseWrapperInstanceB();
	    sql_dbb.loadDatabase(dbConfig1, true);
	    table2b=(Table2)sql_dbb.getTableInstance(Table2.class);
	    table1b=(Table1)sql_dbb.getTableInstance(Table1.class);
	    table3b=(Table3)sql_dbb.getTableInstance(Table3.class);
	    table4b=(Table4)sql_dbb.getTableInstance(Table4.class);
	    table5b=(Table5)sql_dbb.getTableInstance(Table5.class);
	    table6b=(Table6)sql_dbb.getTableInstance(Table6.class);
	    
	    Assert.assertTrue(sql_db.equals(sql_db));
	    Assert.assertFalse(sql_db.equals(sql_dbb));

	    Assert.assertFalse(table1.getDatabaseWrapper().equals(table1b.getDatabaseWrapper()));
	    Assert.assertFalse(table2.getDatabaseWrapper().equals(table2b.getDatabaseWrapper()));
	    Assert.assertFalse(table3.getDatabaseWrapper().equals(table3b.getDatabaseWrapper()));
	    Assert.assertFalse(table4.getDatabaseWrapper().equals(table4b.getDatabaseWrapper()));
	    Assert.assertFalse(table5.getDatabaseWrapper().equals(table5b.getDatabaseWrapper()));
	    Assert.assertFalse(table6.getDatabaseWrapper().equals(table6b.getDatabaseWrapper()));
	    
	    Assert.assertFalse(table1.equals(table1b));
	    Assert.assertFalse(table2.equals(table2b));
	    Assert.assertFalse(table3.equals(table3b));
	    Assert.assertFalse(table4.equals(table4b));
	    Assert.assertFalse(table5.equals(table5b));
	    Assert.assertFalse(table6.equals(table6b));
	    
	    
	    
    }
    @Test(dependsOnMethods={"firstReload"}) public void secondTestSize() throws DatabaseException
    {
	Assert.assertTrue(table1.getRecordsNumber()==1);
	Assert.assertTrue(table3.getRecordsNumber()==1);
	
    }
    
    @Test(dependsOnMethods={"firstReload"}) public void testFirstAdd() throws DatabaseException
    {
	    byte[] tab=new byte[3];
	    tab[0]=0;
	    tab[1]=1;
	    tab[2]=2;
	    Table1.Record r=table1.getRecords().get(0);
	    Assert.assertTrue(r.pk1==0);
	    Assert.assertTrue(r.int_value==3);
	    Assert.assertTrue(r.byte_value==(byte)3);
	    Assert.assertTrue(r.char_value=='x');
	    Assert.assertTrue(r.boolean_value);
	    Assert.assertTrue(r.short_value==(short)3);
	    Assert.assertTrue(r.long_value==(long)3);
	    Assert.assertTrue(r.float_value==3.3f);
	    Assert.assertTrue(r.double_value==3.3);
	    Assert.assertTrue(r.string_value.equals("test string"));
	    Assert.assertTrue(r.IntegerNumber_value.intValue()==3);
	    Assert.assertTrue(r.ByteNumber_value.byteValue()==(byte)3);
	    Assert.assertTrue(r.CharacterNumber_value.charValue()=='x');
	    Assert.assertTrue(r.BooleanNumber_value.booleanValue());
	    Assert.assertTrue(r.ShortNumber_value.shortValue()==(short)3);
	    Assert.assertTrue(r.LongNumber_value.longValue()==(long)3);
	    Assert.assertTrue(r.FloatNumber_value.floatValue()==3.3f);
	    Assert.assertTrue(r.DoubleNumber_value.doubleValue()==3.3);
	    Assert.assertTrue(r.BigInteger_value.equals(new BigInteger("5")));
	    Assert.assertTrue(r.BigDecimal_value.equals(new BigDecimal("8.8")));
	    Assert.assertTrue(r.DateValue.equals(date));
	    Assert.assertTrue(r.CalendarValue.equals(calendar));
	    Assert.assertTrue(r.secretKey.equals(secretKey));
	    Assert.assertTrue(r.typeSecretKey.equals(typeSecretKey));
	    assertEquals(r.subField, subField);
	    assertEquals(r.subSubField, subSubField);
	    
	    for (int i=0;i<3;i++)
		Assert.assertTrue(r.byte_array_value[i]==tab[i]);

	    Table3.Record r2=table3.getRecords().get(0);
	    Assert.assertTrue(r2.pk1==0);
	    Assert.assertTrue(r2.int_value==3);
	    Assert.assertTrue(r2.byte_value==(byte)3);
	    Assert.assertTrue(r2.char_value=='x');
	    Assert.assertTrue(r2.boolean_value);
	    Assert.assertTrue(r2.short_value==(short)3);
	    Assert.assertTrue(r2.long_value==(long)3);
	    Assert.assertTrue(r2.float_value==3.3f);
	    Assert.assertTrue(r2.double_value==3.3);
	    Assert.assertTrue(r2.string_value.equals("test string"));
	    Assert.assertTrue(r2.IntegerNumber_value.intValue()==3);
	    Assert.assertTrue(r2.ByteNumber_value.byteValue()==(byte)3);
	    Assert.assertTrue(r2.CharacterNumber_value.charValue()=='x');
	    Assert.assertTrue(r2.BooleanNumber_value.booleanValue());
	    Assert.assertTrue(r2.ShortNumber_value.shortValue()==(short)3);
	    Assert.assertTrue(r2.LongNumber_value.longValue()==(long)3);
	    Assert.assertTrue(r2.FloatNumber_value.floatValue()==3.3f);
	    Assert.assertTrue(r2.DoubleNumber_value.doubleValue()==3.3);
	    Assert.assertTrue(r2.BigInteger_value.equals(new BigInteger("5")));
	    Assert.assertTrue(r2.BigDecimal_value.equals(new BigDecimal("8.8")));
	    Assert.assertTrue(r2.DateValue.equals(date));
	    Assert.assertTrue(r2.CalendarValue.equals(calendar));
	    Assert.assertTrue(r2.secretKey.equals(secretKey));
	    Assert.assertTrue(r2.typeSecretKey.equals(typeSecretKey));
	    assertEquals(r2.subField, subField);
	    assertEquals(r2.subSubField, subSubField);
	    
	    
	    for (int i=0;i<3;i++)
		Assert.assertTrue(r2.byte_array_value[i]==tab[i]);
    }
    @Test(dependsOnMethods={"alterRecord"}) public void addSecondRecord() throws DatabaseException
    {
	try
	{
	    Map<String, Object> m=null;
	    table1.addRecord(m);
	    Assert.assertTrue(false);
	}
	catch(NullPointerException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table1.addRecord(new HashMap<String, Object>());
	    Assert.assertTrue(false);
	}
	catch(DatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	
	    HashMap<String, Object> map=new HashMap<String, Object>();
	    map.put("pk1", new Integer(0));
	    map.put("int_value", new Integer(3));
	    map.put("byte_value", new Byte((byte)3));
	    map.put("char_value", new Character('x'));
	    map.put("boolean_value", new Boolean(true));
	    map.put("short_value", new Short((short)3));
	    map.put("long_value", new Long(3));
	    map.put("float_value", new Float(3.3f));
	    map.put("double_value", new Double(3.3));
	    map.put("string_value", new String("test string"));
	    map.put("IntegerNumber_value", new Integer(3));
	    map.put("ByteNumber_value", new Byte((byte)3));
	    map.put("CharacterNumber_value", new Character('x'));
	    map.put("BooleanNumber_value", new Boolean(true));
	    map.put("ShortNumber_value", new Short((short)3));
	    map.put("LongNumber_value", new Long((long)3));
	    map.put("FloatNumber_value", new Float(3.3f));
	    map.put("DoubleNumber_value", new Double(3.3));
	    map.put("BigInteger_value", new BigInteger("5"));
	    map.put("BigDecimal_value", new BigDecimal("8.8"));
	    map.put("DateValue", date);
	    map.put("CalendarValue", calendar);
	    map.put("secretKey", secretKey);
	    map.put("typeSecretKey", typeSecretKey);
	    map.put("subField", subField);
	    map.put("subSubField", subSubField);
	    
	    byte[] tab=new byte[3];
	    tab[0]=0;
	    tab[1]=1;
	    tab[2]=2;
	    map.put("byte_array_value", tab);
	    table1.addRecord(map);
	    table3.addRecord(map);
	    table1.checkDataIntegrity();
	    table3.checkDataIntegrity();
	    table2.checkDataIntegrity();
	    table4.checkDataIntegrity();
	    table5.checkDataIntegrity();
	    table6.checkDataIntegrity();
	    

    }
    @Test(dependsOnMethods={"testFirstAdd"}) public void alterRecord() throws DatabaseException
    {
	    HashMap<String, Object> map=new HashMap<String, Object>();
	    map.put("pk1", new Integer(1));
	    map.put("byte_value", new Byte((byte)6));
	    map.put("char_value", new Character('y'));
	    map.put("DoubleNumber_value", new Double(6.6));
	    map.put("subField", getSubField());
	    map.put("subSubField", getSubSubField());
	    byte[] tab=new byte[3];
	    tab[0]=3;
	    tab[1]=5;
	    tab[2]=6;
	    map.put("byte_array_value", tab);
	    
	    Table1.Record r1=table1.getRecords().get(0);
	    table1.updateRecord(r1, map);
	    Table3.Record r2=table3.getRecords().get(0);
	    table3.updateRecord(r2, map);
	    
	    Assert.assertTrue(r1.pk1==1);
	    Assert.assertTrue(r1.int_value==3);
	    Assert.assertTrue(r1.byte_value==(byte)6);
	    Assert.assertTrue(r1.char_value=='y');
	    Assert.assertTrue(r1.boolean_value);
	    Assert.assertTrue(r1.short_value==(short)3);
	    Assert.assertTrue(r1.long_value==(long)3);
	    Assert.assertTrue(r1.float_value==3.3f);
	    Assert.assertTrue(r1.double_value==3.3);
	    Assert.assertTrue(r1.string_value.equals("test string"));
	    Assert.assertTrue(r1.IntegerNumber_value.intValue()==3);
	    Assert.assertTrue(r1.ByteNumber_value.byteValue()==(byte)3);
	    Assert.assertTrue(r1.CharacterNumber_value.charValue()=='x');
	    Assert.assertTrue(r1.BooleanNumber_value.booleanValue());
	    Assert.assertTrue(r1.ShortNumber_value.shortValue()==(short)3);
	    Assert.assertTrue(r1.LongNumber_value.longValue()==(long)3);
	    Assert.assertTrue(r1.FloatNumber_value.floatValue()==3.3f);
	    Assert.assertTrue(r1.DoubleNumber_value.doubleValue()==6.6);
	    Assert.assertTrue(r1.BigInteger_value.equals(new BigInteger("5")));
	    Assert.assertTrue(r1.BigDecimal_value.equals(new BigDecimal("8.8")));
	    Assert.assertTrue(r1.DateValue.equals(date));
	    Assert.assertTrue(r1.CalendarValue.equals(calendar));
	    Assert.assertTrue(r1.secretKey.equals(secretKey));
	    Assert.assertTrue(r1.typeSecretKey.equals(typeSecretKey));
	    assertEquals(r1.subField, subField);
	    assertEquals(r1.subSubField, subSubField);
	    
	    
	    for (int i=0;i<3;i++)
		Assert.assertTrue(r1.byte_array_value[i]==tab[i]);

	    Assert.assertTrue(r2.pk1==1);
	    Assert.assertTrue(r2.int_value==3);
	    Assert.assertTrue(r2.byte_value==(byte)6);
	    Assert.assertTrue(r2.char_value=='y');
	    Assert.assertTrue(r2.boolean_value);
	    Assert.assertTrue(r2.short_value==(short)3);
	    Assert.assertTrue(r2.long_value==(long)3);
	    Assert.assertTrue(r2.float_value==3.3f);
	    Assert.assertTrue(r2.double_value==3.3);
	    Assert.assertTrue(r2.string_value.equals("test string"));
	    Assert.assertTrue(r2.IntegerNumber_value.intValue()==3);
	    Assert.assertTrue(r2.ByteNumber_value.byteValue()==(byte)3);
	    Assert.assertTrue(r2.CharacterNumber_value.charValue()=='x');
	    Assert.assertTrue(r2.BooleanNumber_value.booleanValue());
	    Assert.assertTrue(r2.ShortNumber_value.shortValue()==(short)3);
	    Assert.assertTrue(r2.LongNumber_value.longValue()==(long)3);
	    Assert.assertTrue(r2.FloatNumber_value.floatValue()==3.3f);
	    Assert.assertTrue(r2.DoubleNumber_value.doubleValue()==6.6);
	    Assert.assertTrue(r2.BigInteger_value.equals(new BigInteger("5")));
	    Assert.assertTrue(r2.BigDecimal_value.equals(new BigDecimal("8.8")));
	    Assert.assertTrue(r2.DateValue.equals(date));
	    Assert.assertTrue(r2.CalendarValue.equals(calendar));
	    Assert.assertTrue(r2.secretKey.equals(secretKey));
	    Assert.assertTrue(r2.typeSecretKey.equals(typeSecretKey));
	    assertEquals(r2.subField, subField);
	    assertEquals(r2.subSubField, subSubField);
	    
	    for (int i=0;i<3;i++)
		Assert.assertTrue(r2.byte_array_value[i]==tab[i]);
	
	    r1=table1.getRecords().get(0);
	    r2=table3.getRecords().get(0);
	    
	    Assert.assertTrue(r1.pk1==1);
	    Assert.assertTrue(r1.int_value==3);
	    Assert.assertTrue(r1.byte_value==(byte)6);
	    Assert.assertTrue(r1.char_value=='y');
	    Assert.assertTrue(r1.boolean_value);
	    Assert.assertTrue(r1.short_value==(short)3);
	    Assert.assertTrue(r1.long_value==(long)3);
	    Assert.assertTrue(r1.float_value==3.3f);
	    Assert.assertTrue(r1.double_value==3.3);
	    Assert.assertTrue(r1.string_value.equals("test string"));
	    Assert.assertTrue(r1.IntegerNumber_value.intValue()==3);
	    Assert.assertTrue(r1.ByteNumber_value.byteValue()==(byte)3);
	    Assert.assertTrue(r1.CharacterNumber_value.charValue()=='x');
	    Assert.assertTrue(r1.BooleanNumber_value.booleanValue());
	    Assert.assertTrue(r1.ShortNumber_value.shortValue()==(short)3);
	    Assert.assertTrue(r1.LongNumber_value.longValue()==(long)3);
	    Assert.assertTrue(r1.FloatNumber_value.floatValue()==3.3f);
	    Assert.assertTrue(r1.DoubleNumber_value.doubleValue()==6.6);
	    Assert.assertTrue(r1.BigInteger_value.equals(new BigInteger("5")));
	    Assert.assertTrue(r1.BigDecimal_value.equals(new BigDecimal("8.8")));
	    Assert.assertTrue(r1.DateValue.equals(date));
	    Assert.assertTrue(r1.CalendarValue.equals(calendar));
	    Assert.assertTrue(r1.secretKey.equals(secretKey));
	    Assert.assertTrue(r1.typeSecretKey.equals(typeSecretKey));
	    assertEquals(r1.subField, subField);
	    assertEquals(r1.subSubField, subSubField);
	    
	    for (int i=0;i<3;i++)
		Assert.assertTrue(r1.byte_array_value[i]==tab[i]);

	    Assert.assertTrue(r2.pk1==1);
	    Assert.assertTrue(r2.int_value==3);
	    Assert.assertTrue(r2.byte_value==(byte)6);
	    Assert.assertTrue(r2.char_value=='y');
	    Assert.assertTrue(r2.boolean_value);
	    Assert.assertTrue(r2.short_value==(short)3);
	    Assert.assertTrue(r2.long_value==(long)3);
	    Assert.assertTrue(r2.float_value==3.3f);
	    Assert.assertTrue(r2.double_value==3.3);
	    Assert.assertTrue(r2.string_value.equals("test string"));
	    Assert.assertTrue(r2.IntegerNumber_value.intValue()==3);
	    Assert.assertTrue(r2.ByteNumber_value.byteValue()==(byte)3);
	    Assert.assertTrue(r2.CharacterNumber_value.charValue()=='x');
	    Assert.assertTrue(r2.BooleanNumber_value.booleanValue());
	    Assert.assertTrue(r2.ShortNumber_value.shortValue()==(short)3);
	    Assert.assertTrue(r2.LongNumber_value.longValue()==(long)3);
	    Assert.assertTrue(r2.FloatNumber_value.floatValue()==3.3f);
	    Assert.assertTrue(r2.DoubleNumber_value.doubleValue()==6.6);
	    Assert.assertTrue(r2.BigInteger_value.equals(new BigInteger("5")));
	    Assert.assertTrue(r2.BigDecimal_value.equals(new BigDecimal("8.8")));
	    Assert.assertTrue(r2.DateValue.equals(date));
	    Assert.assertTrue(r2.CalendarValue.equals(calendar));
	    Assert.assertTrue(r2.secretKey.equals(secretKey));
	    Assert.assertTrue(r2.typeSecretKey.equals(typeSecretKey));
	    assertEquals(r2.subField, subField);
	    assertEquals(r2.subSubField, subSubField);
	    
	    for (int i=0;i<3;i++)
		Assert.assertTrue(r2.byte_array_value[i]==tab[i]);
	    table1.checkDataIntegrity();
	    table3.checkDataIntegrity();
	    table2.checkDataIntegrity();
	    table4.checkDataIntegrity();
	    table5.checkDataIntegrity();
	    table6.checkDataIntegrity();

    }
    
    @Test(dependsOnMethods={"addSecondRecord"}) public void getRecord() throws DatabaseException
    {
	Table1.Record r1a=table1.getRecords().get(0);
	Table3.Record r2a=table3.getRecords().get(0);
	
	HashMap<String, Object> keys1=new HashMap<String, Object>();
	keys1.put("pk1", new Integer(r1a.pk1));
	keys1.put("pk2", new Long(r1a.pk2));
	keys1.put("pk3", r1a.pk3);
	keys1.put("pk4", new Long(r1a.pk4));
	keys1.put("pk5", r1a.pk5);
	keys1.put("pk6", r1a.pk6);
	keys1.put("pk7", r1a.pk7);
	
	HashMap<String, Object> keys2=new HashMap<String, Object>();
	keys2.put("pk1", new Integer(r2a.pk1));
	keys2.put("pk2", new Long(r2a.pk2));
	keys2.put("pk3", r2a.pk3);
	keys2.put("pk4", new Long(r2a.pk4));
	keys2.put("pk5", r2a.pk5);
	keys2.put("pk6", r2a.pk6);
	keys2.put("pk7", r2a.pk7);

	Table1.Record r1b=table1.getRecord(keys1);
	Table3.Record r2b=table3.getRecord(keys2);
	
	Assert.assertTrue(r1a.pk1==r1b.pk1);
	Assert.assertTrue(r1a.pk2==r1b.pk2);
	Assert.assertTrue(r1a.pk3.equals(r1b.pk3));
	Assert.assertTrue(r1a.pk4==r1b.pk4);
	Assert.assertTrue(r1a.pk5.equals(r1b.pk5));
	Assert.assertTrue(r1a.pk6.equals(r1b.pk6));
	Assert.assertTrue(r1a.pk7.equals(r1b.pk7));
	Assert.assertTrue(r1a.int_value==r1b.int_value);
	Assert.assertTrue(r1a.byte_value==r1b.byte_value);
	Assert.assertTrue(r1a.char_value==r1b.char_value);
	Assert.assertTrue(r1a.boolean_value==r1b.boolean_value);
	Assert.assertTrue(r1a.short_value==r1b.short_value);
	Assert.assertTrue(r1a.long_value==r1b.long_value);
	Assert.assertTrue(r1a.float_value==r1b.float_value);
	Assert.assertTrue(r1a.double_value==r1b.double_value);
	Assert.assertTrue(r1a.string_value.equals(r1b.string_value));
	Assert.assertTrue(r1a.IntegerNumber_value.equals(r1b.IntegerNumber_value));
	Assert.assertTrue(r1a.ByteNumber_value.equals(r1b.ByteNumber_value));
	Assert.assertTrue(r1a.CharacterNumber_value.equals(r1b.CharacterNumber_value));
	Assert.assertTrue(r1a.BooleanNumber_value.equals(r1b.BooleanNumber_value));
	Assert.assertTrue(r1a.ShortNumber_value.equals(r1b.ShortNumber_value));
	Assert.assertTrue(r1a.LongNumber_value.equals(r1b.LongNumber_value));
	Assert.assertTrue(r1a.FloatNumber_value.equals(r1b.FloatNumber_value));
	Assert.assertTrue(r1a.DoubleNumber_value.equals(r1b.DoubleNumber_value));
	Assert.assertTrue(r1a.BigInteger_value.equals(new BigInteger("5")));
	Assert.assertTrue(r1a.BigDecimal_value.equals(new BigDecimal("8.8")));
	Assert.assertTrue(r1a.DateValue.equals(date));
	Assert.assertTrue(r1a.CalendarValue.equals(calendar));
	Assert.assertTrue(r1a.secretKey.equals(secretKey));
	Assert.assertTrue(r1a.typeSecretKey.equals(typeSecretKey));
	    assertEquals(r1a.subField, subField);
	    assertEquals(r1a.subSubField, subSubField);
	
	for (int i=0;i<3;i++)
	    Assert.assertTrue(r1a.byte_array_value[i]==r1b.byte_array_value[i]);
	
	Assert.assertTrue(r2a.pk1==r2b.pk1);
	Assert.assertTrue(r2a.pk2==r2b.pk2);
	Assert.assertTrue(r2a.pk3.equals(r2b.pk3));
	Assert.assertTrue(r2a.pk4==r2b.pk4);
	Assert.assertTrue(r2a.pk5.equals(r2b.pk5));
	Assert.assertTrue(r2a.pk6.equals(r2b.pk6));
	Assert.assertTrue(r2a.pk7.equals(r2b.pk7));
	Assert.assertTrue(r2a.int_value==r2b.int_value);
	Assert.assertTrue(r2a.byte_value==r2b.byte_value);
	Assert.assertTrue(r2a.char_value==r2b.char_value);
	Assert.assertTrue(r2a.boolean_value==r2b.boolean_value);
	Assert.assertTrue(r2a.short_value==r2b.short_value);
	Assert.assertTrue(r2a.long_value==r2b.long_value);
	Assert.assertTrue(r2a.float_value==r2b.float_value);
	Assert.assertTrue(r2a.double_value==r2b.double_value);
	Assert.assertTrue(r2a.string_value.equals(r2b.string_value));
	Assert.assertTrue(r2a.IntegerNumber_value.equals(r2b.IntegerNumber_value));
	Assert.assertTrue(r2a.ByteNumber_value.equals(r2b.ByteNumber_value));
	Assert.assertTrue(r2a.CharacterNumber_value.equals(r2b.CharacterNumber_value));
	Assert.assertTrue(r2a.BooleanNumber_value.equals(r2b.BooleanNumber_value));
	Assert.assertTrue(r2a.ShortNumber_value.equals(r2b.ShortNumber_value));
	Assert.assertTrue(r2a.LongNumber_value.equals(r2b.LongNumber_value));
	Assert.assertTrue(r2a.FloatNumber_value.equals(r2b.FloatNumber_value));
	Assert.assertTrue(r2a.DoubleNumber_value.equals(r2b.DoubleNumber_value));
	Assert.assertTrue(r2a.BigInteger_value.equals(new BigInteger("5")));
	Assert.assertTrue(r2a.BigDecimal_value.equals(new BigDecimal("8.8")));
	Assert.assertTrue(r2a.DateValue.equals(date));
	Assert.assertTrue(r2a.CalendarValue.equals(calendar));
	Assert.assertTrue(r2a.secretKey.equals(secretKey));
	Assert.assertTrue(r2a.typeSecretKey.equals(typeSecretKey));
	    assertEquals(r2a.subField, subField);
	    assertEquals(r2a.subSubField, subSubField);
	
	for (int i=0;i<3;i++)
	    Assert.assertTrue(r2a.byte_array_value[i]==r2b.byte_array_value[i]);
	    table1.checkDataIntegrity();
	    table3.checkDataIntegrity();
	    table2.checkDataIntegrity();
	    table4.checkDataIntegrity();
	    table5.checkDataIntegrity();
	    table6.checkDataIntegrity();
    
    }
    
    /*@Test(dependsOnMethods={"getRecord"}) public void getRecordIterator() throws DatabaseException
    {
	try(TableIterator<Table1.Record> it1=table1.getIterator())
	{
	    try(TableIterator<Table3.Record> it2=table3.getIterator())
	    {
		Table1.Record r1a=null;
		Table3.Record r2a=null;
		Assert.assertTrue(it1.hasNext());
		Assert.assertTrue(it2.hasNext());
		r1a=it1.next();
		r2a=it2.next();
	
		HashMap<String, Object> keys1=new HashMap<String, Object>();
		keys1.put("pk1", new Integer(r1a.pk1));
		keys1.put("pk2", new Long(r1a.pk2));
		keys1.put("pk3", r1a.pk3);
		keys1.put("pk4", new Long(r1a.pk4));
		keys1.put("pk5", r1a.pk5);
		keys1.put("pk6", r1a.pk6);
		keys1.put("pk7", r1a.pk7);
	
		HashMap<String, Object> keys2=new HashMap<String, Object>();
		keys2.put("pk1", new Integer(r2a.pk1));
		keys2.put("pk2", new Long(r2a.pk2));
		keys2.put("pk3", r2a.pk3);
		keys2.put("pk4", new Long(r2a.pk4));
		keys2.put("pk5", r2a.pk5);
		keys2.put("pk6", r2a.pk6);
		keys2.put("pk7", r2a.pk7);
		
		Table1.Record r1b=table1.getRecord(keys1);
		Table3.Record r2b=table3.getRecord(keys2);
	
		Assert.assertTrue(r1a.pk1==r1b.pk1);
		Assert.assertTrue(r1a.pk2==r1b.pk2);
		Assert.assertTrue(r1a.pk3.equals(r1b.pk3));
		Assert.assertTrue(r1a.pk4==r1b.pk4);
		Assert.assertTrue(r1a.pk5.equals(r1b.pk5));
		Assert.assertTrue(r1a.pk6.equals(r1b.pk6));
		Assert.assertTrue(r1a.pk7.equals(r1b.pk7));
		Assert.assertTrue(r1a.int_value==r1b.int_value);
		Assert.assertTrue(r1a.byte_value==r1b.byte_value);
		Assert.assertTrue(r1a.char_value==r1b.char_value);
		Assert.assertTrue(r1a.boolean_value==r1b.boolean_value);
		Assert.assertTrue(r1a.short_value==r1b.short_value);
		Assert.assertTrue(r1a.long_value==r1b.long_value);
		Assert.assertTrue(r1a.float_value==r1b.float_value);
		Assert.assertTrue(r1a.double_value==r1b.double_value);
		Assert.assertTrue(r1a.string_value.equals(r1b.string_value));
		Assert.assertTrue(r1a.IntegerNumber_value.equals(r1b.IntegerNumber_value));
		Assert.assertTrue(r1a.ByteNumber_value.equals(r1b.ByteNumber_value));
		Assert.assertTrue(r1a.CharacterNumber_value.equals(r1b.CharacterNumber_value));
		Assert.assertTrue(r1a.BooleanNumber_value.equals(r1b.BooleanNumber_value));
		Assert.assertTrue(r1a.ShortNumber_value.equals(r1b.ShortNumber_value));
		Assert.assertTrue(r1a.LongNumber_value.equals(r1b.LongNumber_value));
		Assert.assertTrue(r1a.FloatNumber_value.equals(r1b.FloatNumber_value));
		Assert.assertTrue(r1a.DoubleNumber_value.equals(r1b.DoubleNumber_value));
		Assert.assertTrue(r1a.BigInteger_value.equals(new BigInteger("5")));
		Assert.assertTrue(r1a.BigDecimal_value.equals(new BigDecimal("8.8")));
		Assert.assertTrue(r1a.DateValue.equals(date));
		Assert.assertTrue(r1a.CalendarValue.equals(calendar));
		Assert.assertTrue(r1a.secretKey.equals(secretKey));
		Assert.assertTrue(r1a.typeSecretKey.equals(typeSecretKey));
		    assertEquals(r1a.subField, subField);
		    assertEquals(r1a.subSubField, subSubField);
		
		for (int i=0;i<3;i++)
		    Assert.assertTrue(r1a.byte_array_value[i]==r1b.byte_array_value[i]);
		
		Assert.assertTrue(r2a.pk1==r2b.pk1);
		Assert.assertTrue(r2a.pk2==r2b.pk2);
		Assert.assertTrue(r2a.pk3.equals(r2b.pk3));
		Assert.assertTrue(r2a.pk4==r2b.pk4);
		Assert.assertTrue(r2a.pk5.equals(r2b.pk5));
		Assert.assertTrue(r2a.pk6.equals(r2b.pk6));
		Assert.assertTrue(r2a.pk7.equals(r2b.pk7));
		Assert.assertTrue(r2a.int_value==r2b.int_value);
		Assert.assertTrue(r2a.byte_value==r2b.byte_value);
		Assert.assertTrue(r2a.char_value==r2b.char_value);
		Assert.assertTrue(r2a.boolean_value==r2b.boolean_value);
		Assert.assertTrue(r2a.short_value==r2b.short_value);
		Assert.assertTrue(r2a.long_value==r2b.long_value);
		Assert.assertTrue(r2a.float_value==r2b.float_value);
		Assert.assertTrue(r2a.double_value==r2b.double_value);
		Assert.assertTrue(r2a.string_value.equals(r2b.string_value));
		Assert.assertTrue(r2a.IntegerNumber_value.equals(r2b.IntegerNumber_value));
		Assert.assertTrue(r2a.ByteNumber_value.equals(r2b.ByteNumber_value));
		Assert.assertTrue(r2a.CharacterNumber_value.equals(r2b.CharacterNumber_value));
		Assert.assertTrue(r2a.BooleanNumber_value.equals(r2b.BooleanNumber_value));
		Assert.assertTrue(r2a.ShortNumber_value.equals(r2b.ShortNumber_value));
		Assert.assertTrue(r2a.LongNumber_value.equals(r2b.LongNumber_value));
		Assert.assertTrue(r2a.FloatNumber_value.equals(r2b.FloatNumber_value));
		Assert.assertTrue(r2a.DoubleNumber_value.equals(r2b.DoubleNumber_value));
		Assert.assertTrue(r2a.BigInteger_value.equals(new BigInteger("5")));
		Assert.assertTrue(r2a.BigDecimal_value.equals(new BigDecimal("8.8")));
		Assert.assertTrue(r2a.DateValue.equals(date));
		Assert.assertTrue(r2a.CalendarValue.equals(calendar));
		Assert.assertTrue(r2a.secretKey.equals(secretKey));
		Assert.assertTrue(r2a.typeSecretKey.equals(typeSecretKey));
		    assertEquals(r2a.subField, subField);
		    assertEquals(r2a.subSubField, subSubField);
		
		for (int i=0;i<3;i++)
		    Assert.assertTrue(r2a.byte_array_value[i]==r2b.byte_array_value[i]);
		
	    }
	}
	table1.checkDataIntegrity();
	table3.checkDataIntegrity();
	table2.checkDataIntegrity();
	table4.checkDataIntegrity();
	table5.checkDataIntegrity();
	table6.checkDataIntegrity();
    }*/
    
    @Test(dependsOnMethods={"addSecondRecord"}) public void getRecordFilter() throws DatabaseException
    {
	final Table1.Record r1a=table1.getRecords().get(0);
	final Table3.Record r2a=table3.getRecords().get(0);
	
	ArrayList<Table1.Record> col1=table1.getRecords(new Filter<Table1.Record>() {
	    
	    @Override
	    public boolean nextRecord(Record _record) 
	    {
		if (_record.pk1==r1a.pk1 && _record.pk2==r1a.pk2 && _record.pk4==r1a.pk4)
		    return true;
		return false;
	    }
	});
	Assert.assertTrue(col1.size()==1);
	Table1.Record r1b=col1.get(0);
	
	ArrayList<Table3.Record> col2=table3.getRecords(new Filter<Table3.Record>() {
	    
	    @Override
	    public boolean nextRecord(Table3.Record _record) 
	    {
		if (_record.pk1==r2a.pk1 && _record.pk2==r2a.pk2 && _record.pk4==r2a.pk4)
		    return true;
		return false;
	    }
	});
	Assert.assertTrue(col2.size()==1);
	Table3.Record r2b=col2.get(0);
	
	Assert.assertTrue(r1a.pk1==r1b.pk1);
	Assert.assertTrue(r1a.pk2==r1b.pk2);
	Assert.assertTrue(r1a.pk3.equals(r1b.pk3));
	Assert.assertTrue(r1a.pk4==r1b.pk4);
	Assert.assertTrue(r1a.pk5.equals(r1b.pk5));
	Assert.assertTrue(r1a.pk6.equals(r1b.pk6));
	Assert.assertTrue(r1a.pk7.equals(r1b.pk7));
	Assert.assertTrue(r1a.int_value==r1b.int_value);
	Assert.assertTrue(r1a.byte_value==r1b.byte_value);
	Assert.assertTrue(r1a.char_value==r1b.char_value);
	Assert.assertTrue(r1a.boolean_value==r1b.boolean_value);
	Assert.assertTrue(r1a.short_value==r1b.short_value);
	Assert.assertTrue(r1a.long_value==r1b.long_value);
	Assert.assertTrue(r1a.float_value==r1b.float_value);
	Assert.assertTrue(r1a.double_value==r1b.double_value);
	Assert.assertTrue(r1a.string_value.equals(r1b.string_value));
	Assert.assertTrue(r1a.IntegerNumber_value.equals(r1b.IntegerNumber_value));
	Assert.assertTrue(r1a.ByteNumber_value.equals(r1b.ByteNumber_value));
	Assert.assertTrue(r1a.CharacterNumber_value.equals(r1b.CharacterNumber_value));
	Assert.assertTrue(r1a.BooleanNumber_value.equals(r1b.BooleanNumber_value));
	Assert.assertTrue(r1a.ShortNumber_value.equals(r1b.ShortNumber_value));
	Assert.assertTrue(r1a.LongNumber_value.equals(r1b.LongNumber_value));
	Assert.assertTrue(r1a.FloatNumber_value.equals(r1b.FloatNumber_value));
	Assert.assertTrue(r1a.DoubleNumber_value.equals(r1b.DoubleNumber_value));
	Assert.assertTrue(r1a.BigInteger_value.equals(r1b.BigInteger_value));
	Assert.assertTrue(r1a.BigDecimal_value.equals(r1b.BigDecimal_value));
	Assert.assertTrue(r1a.DateValue.equals(r1b.DateValue));
	Assert.assertTrue(r1a.CalendarValue.equals(r1b.CalendarValue));
	Assert.assertTrue(r1a.secretKey.equals(secretKey));
	Assert.assertTrue(r1a.typeSecretKey.equals(typeSecretKey));
	    assertEquals(r1a.subField, subField);
	    assertEquals(r1a.subSubField, subSubField);
	
	for (int i=0;i<3;i++)
	    Assert.assertTrue(r1a.byte_array_value[i]==r1b.byte_array_value[i]);
	
	Assert.assertTrue(r2a.pk1==r2b.pk1);
	Assert.assertTrue(r2a.pk2==r2b.pk2);
	Assert.assertTrue(r2a.pk3.equals(r2b.pk3));
	Assert.assertTrue(r2a.pk4==r2b.pk4);
	Assert.assertTrue(r2a.pk5.equals(r2b.pk5));
	Assert.assertTrue(r2a.pk6.equals(r2b.pk6));
	Assert.assertTrue(r2a.pk7.equals(r2b.pk7));
	Assert.assertTrue(r2a.int_value==r2b.int_value);
	Assert.assertTrue(r2a.byte_value==r2b.byte_value);
	Assert.assertTrue(r2a.char_value==r2b.char_value);
	Assert.assertTrue(r2a.boolean_value==r2b.boolean_value);
	Assert.assertTrue(r2a.short_value==r2b.short_value);
	Assert.assertTrue(r2a.long_value==r2b.long_value);
	Assert.assertTrue(r2a.float_value==r2b.float_value);
	Assert.assertTrue(r2a.double_value==r2b.double_value);
	Assert.assertTrue(r2a.string_value.equals(r2b.string_value));
	Assert.assertTrue(r2a.IntegerNumber_value.equals(r2b.IntegerNumber_value));
	Assert.assertTrue(r2a.ByteNumber_value.equals(r2b.ByteNumber_value));
	Assert.assertTrue(r2a.CharacterNumber_value.equals(r2b.CharacterNumber_value));
	Assert.assertTrue(r2a.BooleanNumber_value.equals(r2b.BooleanNumber_value));
	Assert.assertTrue(r2a.ShortNumber_value.equals(r2b.ShortNumber_value));
	Assert.assertTrue(r2a.LongNumber_value.equals(r2b.LongNumber_value));
	Assert.assertTrue(r2a.FloatNumber_value.equals(r2b.FloatNumber_value));
	Assert.assertTrue(r2a.DoubleNumber_value.equals(r2b.DoubleNumber_value));
	Assert.assertTrue(r2a.BigInteger_value.equals(r2b.BigInteger_value));
	Assert.assertTrue(r2a.BigDecimal_value.equals(r2b.BigDecimal_value));
	Assert.assertTrue(r2a.DateValue.equals(r2b.DateValue));
	Assert.assertTrue(r2a.CalendarValue.equals(r2b.CalendarValue));
	Assert.assertTrue(r2a.secretKey.equals(secretKey));
	Assert.assertTrue(r2a.typeSecretKey.equals(typeSecretKey));
	    assertEquals(r2a.subField, subField);
	    assertEquals(r2a.subSubField, subSubField);
	
	for (int i=0;i<3;i++)
	    Assert.assertTrue(r2a.byte_array_value[i]==r2b.byte_array_value[i]);
	
	HashMap<String, Object> map=new HashMap<>();
	map.put("fr1_pk1", table1.getRecords().get(0));
	map.put("int_value", new Integer(0));
	table2.addRecord(map);
	
	table1.getRecords(new Filter<Table1.Record>() {

	    @Override
	    public boolean nextRecord(Record _record) throws DatabaseException
	    {
		table1.getRecords();
		return true;
	    }
	});
	
	table1.getRecords(new Filter<Table1.Record>() {

	    @Override
	    public boolean nextRecord(Record _record) throws DatabaseException
	    {
		table2.getRecords();
		return true;
	    }
	});
	table2.getRecords(new Filter<Table2.Record>() {

	    @Override
	    public boolean nextRecord(Table2.Record _record) throws DatabaseException
	    {
		table1.getRecords();
		return true;
	    }
	});

	try
	{
	    table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

		@Override
		public boolean nextRecord(Record _record) throws DatabaseException
		{
		    table1.getRecords();
		    return false;
		}
	    });
	    Assert.assertTrue(false);
	}
	catch(ConcurentTransactionDatabaseException e)
	{
	    Assert.assertTrue(true);
	}

	try
	{
	    table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

		@Override
		public boolean nextRecord(Record _record) throws DatabaseException
		{
		    table2.getRecords();
		    return false;
		}
	    });
	    Assert.assertTrue(false);
	}
	catch(ConcurentTransactionDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table2.removeRecordsWithCascade(new Filter<Table2.Record>() {

		@Override
		public boolean nextRecord(Table2.Record _record) throws DatabaseException
		{
		    table1.getRecords();
		    return false;
		}
	    });
	}
	catch(ConcurentTransactionDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	

	try
	{
	    table1.getRecords(new Filter<Table1.Record>() {
		@Override
		public boolean nextRecord(Record _record) throws DatabaseException
		{
		    table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

			@Override
			public boolean nextRecord(Record _record) 
			{
			    return false;
			}
		    });
		    return false;
		}
	    });
	    Assert.assertTrue(false);
	}
	catch(ConcurentTransactionDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	
	table1.getRecords(new Filter<Table1.Record>() {
	    @Override
	    public boolean nextRecord(Record _record) throws DatabaseException
	    {
		table2.removeRecordsWithCascade(new Filter<Table2.Record>() {
		    @Override
		    public boolean nextRecord(Table2.Record _record) 
		    {
			return false;
		    }
		});
		return false;
	    }
	});
	
	try
	{
	    table2.getRecords(new Filter<Table2.Record>() {

		@Override
		public boolean nextRecord(Table2.Record _record) throws DatabaseException
		{
		    table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

			@Override
			public boolean nextRecord(Record _record) 
			{
			    return false;
			}
		    });
		    return false;
		}
	    });
	    Assert.assertTrue(false);
	}
	catch(ConcurentTransactionDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	table2.removeRecords(table2.getRecords());
	table1.checkDataIntegrity();
	table3.checkDataIntegrity();
	table2.checkDataIntegrity();
	table4.checkDataIntegrity();
	table5.checkDataIntegrity();
	table6.checkDataIntegrity();
	
	
    }
    
    @Test(dependsOnMethods={"getRecordFilter", "getRecord", "alterRecord"}) public void removeRecord() throws DatabaseException
    {
	Table1.Record r1=table1.getRecords().get(0);
	Table3.Record r2=table3.getRecords().get(0);
	table1.removeRecord(r1);
	table3.removeRecord(r2);
	Assert.assertTrue(table1.getRecordsNumber()==1);
	Assert.assertTrue(table3.getRecordsNumber()==1);
	Assert.assertTrue(table1.getRecords().size()==1);
	Assert.assertTrue(table3.getRecords().size()==1);
	    table1.checkDataIntegrity();
	    table3.checkDataIntegrity();
	    table2.checkDataIntegrity();
	    table4.checkDataIntegrity();
	    table5.checkDataIntegrity();
	    table6.checkDataIntegrity();
	
	
    }
    
    @Test(dependsOnMethods={"removeRecord"}) public void testArrayRecordParameters() throws DatabaseException
    {
	    byte[] tab=new byte[3];
	    tab[0]=0;
	    tab[1]=1;
	    tab[2]=2;
	    Object []parameters={"pk1", new Integer(4356), 
		    "int_value", new Integer(3), 
		    "byte_value", new Byte((byte)3),
		    "char_value", new Character('x'),
		    "boolean_value", new Boolean(true),
		    "short_value", new Short((short)3),
		    "long_value", new Long(3),
		    "float_value", new Float(3.3f),
		    "double_value", new Double(3.3),
		    "string_value", new String("test string"),
		    "IntegerNumber_value", new Integer(3),
		    "ByteNumber_value", new Byte((byte)3),
		    "CharacterNumber_value", new Character('x'),
		    "BooleanNumber_value", new Boolean(true),
		    "ShortNumber_value", new Short((short)3),
		    "LongNumber_value", new Long((long)3),
		    "FloatNumber_value", new Float(3.3f),
		    "DoubleNumber_value", new Double(3.3),
		    "BigInteger_value", new BigInteger("5"),
		    "BigDecimal_value", new BigDecimal("8.8"),
		    "DateValue", date,
		    "CalendarValue", calendar,
		    "secretKey", secretKey,
		    "typeSecretKey", typeSecretKey,
		    "byte_array_value", tab,
		    "subField", subField,
		    "subSubField", subSubField
		    };
	    Table1.Record r1=table1.addRecord(parameters);
	    Table3.Record r2=table3.addRecord(parameters);
	    Assert.assertTrue(r1.pk1==4356);
	    Assert.assertTrue(r2.pk1==4356);
	    table1.removeRecord(r1);
	    table3.removeRecord(r2);

	    Object []p2={"rert"};
	    Object []p3={new Integer(125), "rert"};
	    try
	    {
		table1.addRecord(p2);
		Assert.assertTrue(false);
	    }
	    catch(Exception e)
	    {
		Assert.assertTrue(true);
	    }
	    try
	    {
		table1.addRecord(p3);
		Assert.assertTrue(false);
	    }
	    catch(Exception e)
	    {
		Assert.assertTrue(true);
	    }
	    try
	    {
		table3.addRecord(p2);
		Assert.assertTrue(false);
	    }
	    catch(Exception e)
	    {
		Assert.assertTrue(true);
	    }
	    try
	    {
		table3.addRecord(p3);
		Assert.assertTrue(false);
	    }
	    catch(Exception e)
	    {
		Assert.assertTrue(true);
	    }
    }
    
    @Test(dependsOnMethods={"testArrayRecordParameters"}) public void removeRecords() throws DatabaseException
    {
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
	ArrayList<Table1.Record> r1=table1.getRecords();
	ArrayList<Table3.Record> r2=table3.getRecords();
	table1.removeRecords(r1);
	table3.removeRecords(r2);
	Assert.assertTrue(table1.getRecords().size()==0);
	Assert.assertTrue(table1.getRecordsNumber()==0);
	Assert.assertTrue(table3.getRecords().size()==0);
	Assert.assertTrue(table3.getRecordsNumber()==0);
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
	long size1=table1.getRecordsNumber();
	long size2=table3.getRecordsNumber();
	table1.removeRecords(new Filter<Table1.Record>() {

	    @Override
	    public boolean nextRecord(Record _record)
	    {
		return false;
	    }
	});
	table3.removeRecords(new Filter<Table3.Record>() {

	    @Override
	    public boolean nextRecord(com.distrimind.ood.tests.database.Table3.Record _record) 
	    {
		return false;
	    }
	});
	Assert.assertTrue(size1==table1.getRecordsNumber());
	Assert.assertTrue(size1==table1.getRecords().size());
	Assert.assertTrue(size2==table3.getRecordsNumber());
	Assert.assertTrue(size2==table3.getRecords().size());
	table1.removeRecords(new Filter<Table1.Record>() {

	    @Override
	    public boolean nextRecord(Record _record)
	    {
		return true;
	    }
	});
	table3.removeRecords(new Filter<Table3.Record>() {

	    @Override
	    public boolean nextRecord(com.distrimind.ood.tests.database.Table3.Record _record) 
	    {
		return true;
	    }
	});
	Assert.assertTrue(0==table1.getRecordsNumber());
	Assert.assertTrue(0==table1.getRecords().size());
	
	Assert.assertTrue(0==table3.getRecordsNumber());
	Assert.assertTrue(0==table3.getRecords().size());
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
	    public void nextRecord(Record _record)
	    {
		this.remove();
		
	    }
	});
	table3.updateRecords(new AlterRecordFilter<Table3.Record>() {

	    @Override
	    public void nextRecord(Table3.Record _record)
	    {
		this.remove();
		
	    }
	});
	Assert.assertTrue(0==table1.getRecordsNumber());
	Assert.assertTrue(0==table1.getRecords().size());
	
	Assert.assertTrue(0==table3.getRecordsNumber());
	Assert.assertTrue(0==table3.getRecords().size());
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
	    public void nextRecord(Record _record)
	    {
		this.removeWithCascade();
	    }
	});
	table3.updateRecords(new AlterRecordFilter<Table3.Record>() {

	    @Override
	    public void nextRecord(Table3.Record _record)
	    {
		this.removeWithCascade();
	    }
	});
	Assert.assertTrue(0==table1.getRecordsNumber());
	Assert.assertTrue(0==table1.getRecords().size());
	
	Assert.assertTrue(0==table3.getRecordsNumber());
	Assert.assertTrue(0==table3.getRecords().size());
	
	
	    table1.checkDataIntegrity();
	    table3.checkDataIntegrity();
	    table2.checkDataIntegrity();
	    table4.checkDataIntegrity();
	    table5.checkDataIntegrity();
	    table6.checkDataIntegrity();
	
    }
    
    @Test(dependsOnMethods={"removeRecords"}) public void testFilters() throws DatabaseException
    {
	HashMap<String, Object> map=new HashMap<String, Object>();
	map.put("val1", new Integer(1));
	map.put("val2", new Integer(2));
	map.put("val3", new Integer(3));
	table7.addRecord(map);
	HashMap<String, Object> map2=new HashMap<String, Object>();
	map2.put("val1", new Integer(0));
	map2.put("val2", new Integer(2));
	map2.put("val3", new Integer(3));
	table7.addRecord(map2);
	HashMap<String, Object> map3=new HashMap<String, Object>();
	map3.put("val1", new Integer(0));
	map3.put("val2", new Integer(2));
	map3.put("val3", new Integer(4));
	table7.addRecord(map3);
	
	HashMap<String, Object> mg0=new HashMap<String, Object>();
	mg0.put("val2", new Integer(2));
	mg0.put("val3", new Integer(4));
	HashMap<String, Object> mg1=new HashMap<String, Object>();
	mg1.put("val2", new Integer(2));
	mg1.put("val3", new Integer(3));
	Assert.assertTrue(table7.hasRecordsWithAllFields(mg1));
	Assert.assertTrue(table7.hasRecordsWithOneOfFields(mg1));
	ArrayList<Table7.Record> res=table7.getRecordsWithAllFields(mg1);
	Assert.assertTrue(res.size()==2);
	Assert.assertTrue(res.get(0).val1==1);
	Assert.assertTrue(res.get(0).val2==2);
	Assert.assertTrue(res.get(0).val3==3);
	Assert.assertTrue(res.get(1).val1==0);
	Assert.assertTrue(res.get(1).val2==2);
	Assert.assertTrue(res.get(1).val3==3);
	res=table7.getRecordsWithOneOfFields(mg1);
	Assert.assertTrue(res.size()==3);
	HashMap<String, Object> mg2=new HashMap<String, Object>();
	mg2.put("val1", new Integer(1));
	mg2.put("val3", new Integer(4));
	res=table7.getRecordsWithOneOfFields(mg2);
	Assert.assertTrue(table7.hasRecordsWithOneOfFields(mg2));
	Assert.assertTrue(res.size()==2);
	Assert.assertTrue(res.get(0).val1==1);
	Assert.assertTrue(res.get(0).val2==2);
	Assert.assertTrue(res.get(0).val3==3);
	Assert.assertTrue(res.get(1).val1==0);
	Assert.assertTrue(res.get(1).val2==2);
	Assert.assertTrue(res.get(1).val3==4);
	
	
	Assert.assertTrue(table7.hasRecordsWithAllFields(mg1, mg0));
	Assert.assertTrue(table7.hasRecordsWithOneOfFields(mg1, mg0));
	Assert.assertTrue(table7.getRecordsWithAllFields(mg1, mg0).size()==3);
	Assert.assertTrue(table7.getRecordsWithOneOfFields(mg1, mg0).size()==3);
	    table1.checkDataIntegrity();
	    table3.checkDataIntegrity();
	    table2.checkDataIntegrity();
	    table4.checkDataIntegrity();
	    table5.checkDataIntegrity();
	    table6.checkDataIntegrity();
	
    }
    @Test(dependsOnMethods={"testFilters"}) public void testRemoveFilters() throws DatabaseException
    {
	HashMap<String, Object> mg0=new HashMap<String, Object>();
	mg0.put("val2", new Integer(9));
	mg0.put("val3", new Integer(9));
	HashMap<String, Object> mg1=new HashMap<String, Object>();
	mg1.put("val2", new Integer(2));
	mg1.put("val3", new Integer(3));
	HashMap<String, Object> mg2=new HashMap<String, Object>();
	mg2.put("val1", new Integer(2));
	mg2.put("val3", new Integer(4));
	
	Assert.assertTrue(table7.removeRecordsWithAllFields(mg0)==0);
	Assert.assertTrue(table7.getRecordsNumber()==3);
	Assert.assertTrue(table7.removeRecordsWithOneOfFields(mg0)==0);
	Assert.assertTrue(table7.getRecordsNumber()==3);
	Assert.assertTrue(table7.removeRecordsWithAllFields(mg2)==0);
	Assert.assertTrue(table7.getRecordsNumber()==3);
	Assert.assertTrue(table7.removeRecordsWithOneOfFields(mg2)==1);
	Assert.assertTrue(table7.getRecordsNumber()==2);
	Assert.assertTrue(table7.removeRecordsWithAllFields(mg1)==2);
	Assert.assertTrue(table7.getRecordsNumber()==0);
	    table1.checkDataIntegrity();
	    table3.checkDataIntegrity();
	    table2.checkDataIntegrity();
	    table4.checkDataIntegrity();
	    table5.checkDataIntegrity();
	    table6.checkDataIntegrity();
	
	
    }
    
    @Test(dependsOnMethods={"testRemoveFilters"}) public void addForeignKeyAndTestUniqueKeys() throws DatabaseException
    {
	addSecondRecord();
	addSecondRecord();
	Table1.Record r1=table1.getRecords().get(0);
	Table3.Record r2=table3.getRecords().get(0);
	Table1.Record r1b=table1.getRecords().get(1);
	Table3.Record r2b=table3.getRecords().get(1);
	Assert.assertTrue(table1.getRecordsNumber()==2);
	Assert.assertTrue(table3.getRecordsNumber()==2);
	Assert.assertTrue(table1.getRecords().size()==2);
	Assert.assertTrue(table3.getRecords().size()==2);
	
	HashMap<String, Object> map1=new HashMap<String, Object>();
	map1.put("fr1_pk1", r1);
	map1.put("int_value", new Integer(0));
	HashMap<String, Object> map2=new HashMap<String, Object>();
	map2.put("fr1_pk1", r2);
	map2.put("int_value", new Integer(0));
	try
	{
	    table2.addRecord(map2);
	    Assert.assertTrue(false);
	}
	catch(DatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	table2.addRecord(map1);
	table4.addRecord(map2);
	table5.addRecord(map2);
	
	Table1.Record r1fr2=table2.getRecords().get(0).fr1_pk1;
	Table3.Record r2fr4=table4.getRecords().get(0).fr1_pk1;
	Table3.Record r2fr5=table5.getRecords().get(0).fr1_pk1;
	
	Assert.assertTrue(table2.getRecords().get(0).int_value==0);
	Assert.assertTrue(table4.getRecords().get(0).int_value==0);
	Assert.assertTrue(table5.getRecords().get(0).int_value==0);
	
	Assert.assertTrue(r1.pk1==r1fr2.pk1);
	Assert.assertTrue(r1.pk2==r1fr2.pk2);
	Assert.assertTrue(r1.pk3.equals(r1fr2.pk3));
	Assert.assertTrue(r1.pk4==r1fr2.pk4);
	Assert.assertTrue(r1.pk5.equals(r1fr2.pk5));
	Assert.assertTrue(r1.pk6.equals(r1fr2.pk6));
	Assert.assertTrue(r1.pk7.equals(r1fr2.pk7));
	Assert.assertTrue(r1.int_value==r1fr2.int_value);
	Assert.assertTrue(r1.byte_value==r1fr2.byte_value);
	Assert.assertTrue(r1.char_value==r1fr2.char_value);
	Assert.assertTrue(r1.boolean_value==r1fr2.boolean_value);
	Assert.assertTrue(r1.short_value==r1fr2.short_value);
	Assert.assertTrue(r1.long_value==r1fr2.long_value);
	Assert.assertTrue(r1.float_value==r1fr2.float_value);
	Assert.assertTrue(r1.double_value==r1fr2.double_value);
	Assert.assertTrue(r1.string_value.equals(r1fr2.string_value));
	Assert.assertTrue(r1.IntegerNumber_value.equals(r1fr2.IntegerNumber_value));
	Assert.assertTrue(r1.ByteNumber_value.equals(r1fr2.ByteNumber_value));
	Assert.assertTrue(r1.CharacterNumber_value.equals(r1fr2.CharacterNumber_value));
	Assert.assertTrue(r1.BooleanNumber_value.equals(r1fr2.BooleanNumber_value));
	Assert.assertTrue(r1.ShortNumber_value.equals(r1fr2.ShortNumber_value));
	Assert.assertTrue(r1.LongNumber_value.equals(r1fr2.LongNumber_value));
	Assert.assertTrue(r1.FloatNumber_value.equals(r1fr2.FloatNumber_value));
	Assert.assertTrue(r1.DoubleNumber_value.equals(r1fr2.DoubleNumber_value));
	Assert.assertTrue(r1.BigInteger_value.equals(r1fr2.BigInteger_value));
	Assert.assertTrue(r1.BigDecimal_value.equals(r1fr2.BigDecimal_value));
	Assert.assertTrue(r1.DateValue.equals(r1fr2.DateValue));
	Assert.assertTrue(r1.CalendarValue.equals(r1fr2.CalendarValue));
	Assert.assertTrue(r1.secretKey.equals(r1fr2.secretKey));
	Assert.assertTrue(r1.typeSecretKey.equals(r1fr2.typeSecretKey));
	assertEquals(r1.subField, subField);
	assertEquals(r1.subSubField, subSubField);
	
	
	Assert.assertTrue(r2.pk1==r2fr4.pk1);
	Assert.assertTrue(r2.pk2==r2fr4.pk2);
	Assert.assertTrue(r2.pk3.equals(r2fr4.pk3));
	Assert.assertTrue(r2.pk4==r2fr4.pk4);
	Assert.assertTrue(r2.pk5.equals(r2fr4.pk5));
	Assert.assertTrue(r2.pk6.equals(r2fr4.pk6));
	Assert.assertTrue(r2.pk7.equals(r2fr4.pk7));
	Assert.assertTrue(r2.int_value==r2fr4.int_value);
	Assert.assertTrue(r2.byte_value==r2fr4.byte_value);
	Assert.assertTrue(r2.char_value==r2fr4.char_value);
	Assert.assertTrue(r2.boolean_value==r2fr4.boolean_value);
	Assert.assertTrue(r2.short_value==r2fr4.short_value);
	Assert.assertTrue(r2.long_value==r2fr4.long_value);
	Assert.assertTrue(r2.float_value==r2fr4.float_value);
	Assert.assertTrue(r2.double_value==r2fr4.double_value);
	Assert.assertTrue(r2.string_value.equals(r2fr4.string_value));
	Assert.assertTrue(r2.IntegerNumber_value.equals(r2fr4.IntegerNumber_value));
	Assert.assertTrue(r2.ByteNumber_value.equals(r2fr4.ByteNumber_value));
	Assert.assertTrue(r2.CharacterNumber_value.equals(r2fr4.CharacterNumber_value));
	Assert.assertTrue(r2.BooleanNumber_value.equals(r2fr4.BooleanNumber_value));
	Assert.assertTrue(r2.ShortNumber_value.equals(r2fr4.ShortNumber_value));
	Assert.assertTrue(r2.LongNumber_value.equals(r2fr4.LongNumber_value));
	Assert.assertTrue(r2.FloatNumber_value.equals(r2fr4.FloatNumber_value));
	Assert.assertTrue(r2.DoubleNumber_value.equals(r2fr4.DoubleNumber_value));
	Assert.assertTrue(r2.BigInteger_value.equals(r2fr4.BigInteger_value));
	Assert.assertTrue(r2.BigDecimal_value.equals(r2fr4.BigDecimal_value));
	Assert.assertTrue(r2.DateValue.equals(r2fr4.DateValue));
	Assert.assertTrue(r2.CalendarValue.equals(r2fr4.CalendarValue));
	Assert.assertTrue(r2.secretKey.equals(r2fr4.secretKey));
	Assert.assertTrue(r2.typeSecretKey.equals(r2fr4.typeSecretKey));
	assertEquals(r2.subField, subField);
	assertEquals(r2.subSubField, subSubField);

	
	Assert.assertTrue(r2.pk1==r2fr5.pk1);
	Assert.assertTrue(r2.pk2==r2fr5.pk2);
	Assert.assertTrue(r2.pk3.equals(r2fr5.pk3));
	Assert.assertTrue(r2.pk4==r2fr5.pk4);
	Assert.assertTrue(r2.pk5.equals(r2fr5.pk5));
	Assert.assertTrue(r2.pk6.equals(r2fr5.pk6));
	Assert.assertTrue(r2.pk7.equals(r2fr5.pk7));
	Assert.assertTrue(r2.int_value==r2fr5.int_value);
	Assert.assertTrue(r2.byte_value==r2fr5.byte_value);
	Assert.assertTrue(r2.char_value==r2fr5.char_value);
	Assert.assertTrue(r2.boolean_value==r2fr5.boolean_value);
	Assert.assertTrue(r2.short_value==r2fr5.short_value);
	Assert.assertTrue(r2.long_value==r2fr5.long_value);
	Assert.assertTrue(r2.float_value==r2fr5.float_value);
	Assert.assertTrue(r2.double_value==r2fr5.double_value);
	Assert.assertTrue(r2.string_value.equals(r2fr5.string_value));
	Assert.assertTrue(r2.IntegerNumber_value.equals(r2fr5.IntegerNumber_value));
	Assert.assertTrue(r2.ByteNumber_value.equals(r2fr5.ByteNumber_value));
	Assert.assertTrue(r2.CharacterNumber_value.equals(r2fr5.CharacterNumber_value));
	Assert.assertTrue(r2.BooleanNumber_value.equals(r2fr5.BooleanNumber_value));
	Assert.assertTrue(r2.ShortNumber_value.equals(r2fr5.ShortNumber_value));
	Assert.assertTrue(r2.LongNumber_value.equals(r2fr5.LongNumber_value));
	Assert.assertTrue(r2.FloatNumber_value.equals(r2fr5.FloatNumber_value));
	Assert.assertTrue(r2.DoubleNumber_value.equals(r2fr5.DoubleNumber_value));
	Assert.assertTrue(r2.BigInteger_value.equals(r2fr5.BigInteger_value));
	Assert.assertTrue(r2.BigDecimal_value.equals(r2fr5.BigDecimal_value));
	Assert.assertTrue(r2.DateValue.equals(r2fr5.DateValue));
	Assert.assertTrue(r2.CalendarValue.equals(r2fr5.CalendarValue));
	Assert.assertTrue(r2.secretKey.equals(r2fr5.secretKey));
	Assert.assertTrue(r2.typeSecretKey.equals(r2fr5.typeSecretKey));
	assertEquals(r2.subField, subField);
	assertEquals(r2.subSubField, subSubField);

	
	HashMap<String, Object> map1b=new HashMap<String, Object>();
	map1b.put("fr1_pk1", r1);
	map1b.put("int_value", new Integer(1));
	HashMap<String, Object> map2b=new HashMap<String, Object>();
	map2b.put("fr1_pk1", r2);
	map2b.put("int_value", new Integer(1));
	try
	{
	    table2.addRecord(map1b);
	    Assert.assertTrue(false);
	}
	catch(DatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table4.addRecord(map2b);
	    Assert.assertTrue(false);
	}
	catch(DatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table5.addRecord(map2b);
	    Assert.assertTrue(false);
	}
	catch(DatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	HashMap<String, Object> map1c=new HashMap<String, Object>();
	map1c.put("fr1_pk1", r1b);
	map1c.put("int_value", new Integer(0));
	HashMap<String, Object> map2c=new HashMap<String, Object>();
	map2c.put("fr1_pk1", r2b);
	map2c.put("int_value", new Integer(0));
	try
	{
	    table2.addRecord(map1c);
	    Assert.assertTrue(false);
	}
	catch(DatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table4.addRecord(map2c);
	    Assert.assertTrue(false);
	}
	catch(DatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table5.addRecord(map2c);
	    Assert.assertTrue(false);
	}
	catch(DatabaseException e)
	{
	    Assert.assertTrue(true);
	}

	
	
	
	Assert.assertTrue(table2.getRecordsNumber()==1);
	Assert.assertTrue(table4.getRecordsNumber()==1);
	Assert.assertTrue(table5.getRecordsNumber()==1);
	HashMap<String, Object> map6=new HashMap<String, Object>();
	map6.put("fk1_pk1", table2.getRecords().get(0));
	map6.put("fk2", table5.getRecords().get(0));
	Table6.Record r6=table6.addRecord(map6);
	Assert.assertTrue(equals(r6.fk1_pk1, table2.getRecords().get(0)));
	Assert.assertTrue(equals(r6.fk2, table5.getRecords().get(0)));
	Assert.assertTrue(table6.getRecordsNumber()==1);
	r6=table6.getRecords().get(0);
	Assert.assertTrue(equals(r6.fk1_pk1, table2.getRecords().get(0)));
	Assert.assertTrue(equals(r6.fk2, table5.getRecords().get(0)));
	
	try
	{
	    table6.addRecord(map6);
	    Assert.assertTrue(false);
	}
	catch(DatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	    table1.checkDataIntegrity();
	    table3.checkDataIntegrity();
	    table2.checkDataIntegrity();
	    table4.checkDataIntegrity();
	    table5.checkDataIntegrity();
	    table6.checkDataIntegrity();
	
    }
    @Test(dependsOnMethods={"addForeignKeyAndTestUniqueKeys"}) public void alterRecordWithCascade() throws DatabaseException, NoSuchAlgorithmException, NoSuchProviderException
    {
	    HashMap<String, Object> map=new HashMap<String, Object>();
	    map.put("pk1", new Integer(10));
	    map.put("pk2", new Long(1526345));
	    BigInteger val=null;
	    do
	    {
		val=BigInteger.valueOf(random.nextLong());
		if (val.longValue()<0)
		    val=null;
		else if (table1.getRecords().get(0).pk3.equals(val))
		    val=null;
		else if (table1.getRecords().get(1).pk3.equals(val))
		    val=null;
		else if (table3.getRecords().get(0).pk3.equals(val))
		    val=null;
		else if (table3.getRecords().get(1).pk3.equals(val))
		    val=null;
	    } while (val==null);
	    map.put("pk3", val);
	    map.put("pk5", new SecuredDecentralizedID(new RenforcedDecentralizedIDGenerator(), SecureRandomType.DEFAULT.getInstance()));
	    map.put("pk6", new DecentralizedIDGenerator());
	    map.put("pk7", new RenforcedDecentralizedIDGenerator());
	    map.put("byte_value", new Byte((byte)9));
	    map.put("char_value", new Character('s'));
	    map.put("DoubleNumber_value", new Double(7.7));
	    map.put("subField", subField);
	    map.put("subSubField", subSubField);
	    byte[] tab=new byte[3];
	    tab[0]=7;
	    tab[1]=8;
	    tab[2]=9;
	    map.put("byte_array_value", tab);
	    
	    Table1.Record r1=table1.getRecords().get(0);
	    table1.updateRecord(r1, map);
	    table1.updateRecord(r1, map);
	    Table3.Record r2=table3.getRecords().get(0);
	    table3.updateRecord(r2, map);
	    table3.updateRecord(r2, map);
	    
	    Table1.Record r1a=table1.getRecords().get(0);
	    Table3.Record r2a=table3.getRecords().get(0);
	    
	    map.remove("pk2");
	    map.remove("pk3");
	    map.remove("pk5");
	    map.remove("pk6");
	    map.remove("pk7");
	    map.put("pk3", table1.getRecords().get(1).pk3);
	    try
	    {
		table1.updateRecord(r1a, map);
		Assert.assertTrue(false);
	    }
	    catch(ConstraintsNotRespectedDatabaseException e)
	    {
		Assert.assertTrue(true);
	    }
	    map.remove("pk3");
	    map.put("pk3", table3.getRecords().get(1).pk3);
	    try
	    {
		table3.updateRecord(r2a, map);
		Assert.assertTrue(false);
	    }
	    catch(ConstraintsNotRespectedDatabaseException e)
	    {
		Assert.assertTrue(true);
	    }

	    r1a=table1.getRecords().get(0);
	    r2a=table3.getRecords().get(0);
	    
	    map.remove("pk3");
	    map.put("pk2", new Long(table1.getRecords().get(1).pk2));
	    try
	    {
		table1.updateRecord(r1a, map);
		Assert.assertTrue(false);
	    }
	    catch(ConstraintsNotRespectedDatabaseException e)
	    {
		Assert.assertTrue(true);
	    }
	    map.put("pk2", new Long(table3.getRecords().get(1).pk2));
	    try
	    {
		table3.updateRecord(r2a, map);
		Assert.assertTrue(false);
	    }
	    catch(ConstraintsNotRespectedDatabaseException e)
	    {
		Assert.assertTrue(true);
	    }
	    
	    
	    Assert.assertTrue(r1.pk1==10);
	    Assert.assertTrue(r1.int_value==3);
	    Assert.assertTrue(r1.byte_value==(byte)9);
	    Assert.assertTrue(r1.char_value=='s');
	    Assert.assertTrue(r1.boolean_value);
	    Assert.assertTrue(r1.short_value==(short)3);
	    Assert.assertTrue(r1.long_value==(long)3);
	    Assert.assertTrue(r1.float_value==3.3f);
	    Assert.assertTrue(r1.double_value==3.3);
	    Assert.assertTrue(r1.string_value.equals("test string"));
	    Assert.assertTrue(r1.IntegerNumber_value.intValue()==3);
	    Assert.assertTrue(r1.ByteNumber_value.byteValue()==(byte)3);
	    Assert.assertTrue(r1.CharacterNumber_value.charValue()=='x');
	    Assert.assertTrue(r1.BooleanNumber_value.booleanValue());
	    Assert.assertTrue(r1.ShortNumber_value.shortValue()==(short)3);
	    Assert.assertTrue(r1.LongNumber_value.longValue()==(long)3);
	    Assert.assertTrue(r1.FloatNumber_value.floatValue()==3.3f);
	    Assert.assertTrue(r1.DoubleNumber_value.doubleValue()==7.7);
	    Assert.assertTrue(r1.BigInteger_value.equals(new BigInteger("5")));
	    Assert.assertTrue(r1.BigDecimal_value.equals(new BigDecimal("8.8")));
	    Assert.assertTrue(r1.DateValue.equals(date));
	    Assert.assertTrue(r1.CalendarValue.equals(calendar));
	    Assert.assertTrue(r1.secretKey.equals(secretKey));
	    Assert.assertTrue(r1.typeSecretKey.equals(typeSecretKey));
	    assertEquals(r1.subField, subField);
	    assertEquals(r1.subSubField, subSubField);
	    
	    for (int i=0;i<3;i++)
		Assert.assertTrue(r1.byte_array_value[i]==tab[i]);

	    Assert.assertTrue(r2.pk1==10);
	    Assert.assertTrue(r2.int_value==3);
	    Assert.assertTrue(r2.byte_value==(byte)9);
	    Assert.assertTrue(r2.char_value=='s');
	    Assert.assertTrue(r2.boolean_value);
	    Assert.assertTrue(r2.short_value==(short)3);
	    Assert.assertTrue(r2.long_value==(long)3);
	    Assert.assertTrue(r2.float_value==3.3f);
	    Assert.assertTrue(r2.double_value==3.3);
	    Assert.assertTrue(r2.string_value.equals("test string"));
	    Assert.assertTrue(r2.IntegerNumber_value.intValue()==3);
	    Assert.assertTrue(r2.ByteNumber_value.byteValue()==(byte)3);
	    Assert.assertTrue(r2.CharacterNumber_value.charValue()=='x');
	    Assert.assertTrue(r2.BooleanNumber_value.booleanValue());
	    Assert.assertTrue(r2.ShortNumber_value.shortValue()==(short)3);
	    Assert.assertTrue(r2.LongNumber_value.longValue()==(long)3);
	    Assert.assertTrue(r2.FloatNumber_value.floatValue()==3.3f);
	    Assert.assertTrue(r2.DoubleNumber_value.doubleValue()==7.7);
	    Assert.assertTrue(r2.BigInteger_value.equals(new BigInteger("5")));
	    Assert.assertTrue(r2.BigDecimal_value.equals(new BigDecimal("8.8")));
	    Assert.assertTrue(r2.DateValue.equals(date));
	    Assert.assertTrue(r2.CalendarValue.equals(calendar));
	    Assert.assertTrue(r2.secretKey.equals(secretKey));
	    Assert.assertTrue(r2.typeSecretKey.equals(typeSecretKey));
	    assertEquals(r2.subField, subField);
	    assertEquals(r2.subSubField, subSubField);
	    
	    for (int i=0;i<3;i++)
		Assert.assertTrue(r2.byte_array_value[i]==tab[i]);
	    
	    Table1.Record ra=table2.getRecords().get(0).fr1_pk1;
	    Table3.Record rb=table4.getRecords().get(0).fr1_pk1;
	    Table3.Record rc=table5.getRecords().get(0).fr1_pk1;
	    Table1.Record rd=table6.getRecords().get(0).fk1_pk1.fr1_pk1;
	    Table3.Record re=table6.getRecords().get(0).fk2.fr1_pk1;
	    
	    
	    Assert.assertTrue(r1.pk1==ra.pk1);
	    Assert.assertTrue(r1.pk2==ra.pk2);
	    Assert.assertTrue(r1.pk3.equals(ra.pk3));
	    Assert.assertTrue(r1.pk4==ra.pk4);
	    Assert.assertTrue(r1.pk5.equals(ra.pk5));
	    Assert.assertTrue(r1.pk6.equals(ra.pk6));
	    Assert.assertTrue(r1.pk7.equals(ra.pk7));
	    Assert.assertTrue(r1.int_value==ra.int_value);
	    Assert.assertTrue(r1.byte_value==ra.byte_value);
	    Assert.assertTrue(r1.char_value==ra.char_value);
	    Assert.assertTrue(r1.boolean_value==ra.boolean_value);
	    Assert.assertTrue(r1.short_value==ra.short_value);
	    Assert.assertTrue(r1.long_value==ra.long_value);
	    Assert.assertTrue(r1.float_value==ra.float_value);
	    Assert.assertTrue(r1.double_value==ra.double_value);
	    Assert.assertTrue(r1.string_value.equals(ra.string_value));
	    Assert.assertTrue(r1.IntegerNumber_value.equals(ra.IntegerNumber_value));
	    Assert.assertTrue(r1.ByteNumber_value.equals(ra.ByteNumber_value));
	    Assert.assertTrue(r1.CharacterNumber_value.equals(ra.CharacterNumber_value));
	    Assert.assertTrue(r1.BooleanNumber_value.equals(ra.BooleanNumber_value));
	    Assert.assertTrue(r1.ShortNumber_value.equals(ra.ShortNumber_value));
	    Assert.assertTrue(r1.LongNumber_value.equals(ra.LongNumber_value));
	    Assert.assertTrue(r1.FloatNumber_value.equals(ra.FloatNumber_value));
	    Assert.assertTrue(r1.DoubleNumber_value.equals(ra.DoubleNumber_value));
	    Assert.assertTrue(r1.BigInteger_value.equals(ra.BigInteger_value));
	    Assert.assertTrue(r1.BigDecimal_value.equals(ra.BigDecimal_value));
	    Assert.assertTrue(r1.DateValue.equals(ra.DateValue));
	    Assert.assertTrue(r1.CalendarValue.equals(ra.CalendarValue));
	    Assert.assertTrue(r1.secretKey.equals(ra.secretKey));
	    Assert.assertTrue(r1.typeSecretKey.equals(ra.typeSecretKey));
	    assertEquals(r1.subField, subField);
	    assertEquals(r1.subSubField, subSubField);
	    
	    for (int i=0;i<3;i++)
		Assert.assertTrue(ra.byte_array_value[i]==tab[i]);

	    Assert.assertTrue(r1.pk1==rd.pk1);
	    Assert.assertTrue(r1.pk2==rd.pk2);
	    Assert.assertTrue(r1.pk3.equals(rd.pk3));
	    Assert.assertTrue(r1.pk4==rd.pk4);
	    Assert.assertTrue(r1.pk5.equals(rd.pk5));
	    Assert.assertTrue(r1.pk6.equals(rd.pk6));
	    Assert.assertTrue(r1.pk7.equals(rd.pk7));
	    Assert.assertTrue(r1.int_value==rd.int_value);
	    Assert.assertTrue(r1.byte_value==rd.byte_value);
	    Assert.assertTrue(r1.char_value==rd.char_value);
	    Assert.assertTrue(r1.boolean_value==rd.boolean_value);
	    Assert.assertTrue(r1.short_value==rd.short_value);
	    Assert.assertTrue(r1.long_value==rd.long_value);
	    Assert.assertTrue(r1.float_value==rd.float_value);
	    Assert.assertTrue(r1.double_value==rd.double_value);
	    Assert.assertTrue(r1.string_value.equals(rd.string_value));
	    Assert.assertTrue(r1.IntegerNumber_value.equals(rd.IntegerNumber_value));
	    Assert.assertTrue(r1.ByteNumber_value.equals(rd.ByteNumber_value));
	    Assert.assertTrue(r1.CharacterNumber_value.equals(rd.CharacterNumber_value));
	    Assert.assertTrue(r1.BooleanNumber_value.equals(rd.BooleanNumber_value));
	    Assert.assertTrue(r1.ShortNumber_value.equals(rd.ShortNumber_value));
	    Assert.assertTrue(r1.LongNumber_value.equals(rd.LongNumber_value));
	    Assert.assertTrue(r1.FloatNumber_value.equals(rd.FloatNumber_value));
	    Assert.assertTrue(r1.DoubleNumber_value.equals(rd.DoubleNumber_value));
	    Assert.assertTrue(r1.BigInteger_value.equals(rd.BigInteger_value));
	    Assert.assertTrue(r1.BigDecimal_value.equals(rd.BigDecimal_value));
	    Assert.assertTrue(r1.DateValue.equals(rd.DateValue));
	    Assert.assertTrue(r1.CalendarValue.equals(rd.CalendarValue));
	    Assert.assertTrue(r1.secretKey.equals(rd.secretKey));
	    Assert.assertTrue(r1.typeSecretKey.equals(rd.typeSecretKey));
	    assertEquals(r1.subField, subField);
	    assertEquals(r1.subSubField, subSubField);
	    
	    for (int i=0;i<3;i++)
		Assert.assertTrue(rd.byte_array_value[i]==tab[i]);
	    
	    Assert.assertTrue(r2.pk1==rb.pk1);
	    Assert.assertTrue(r2.pk2==rb.pk2);
	    Assert.assertTrue(r2.pk3.equals(rb.pk3));
	    Assert.assertTrue(r2.pk4==rb.pk4);
	    Assert.assertTrue(r2.pk5.equals(rb.pk5));
	    Assert.assertTrue(r2.pk6.equals(rb.pk6));
	    Assert.assertTrue(r2.pk7.equals(rb.pk7));
	    Assert.assertTrue(r2.int_value==rb.int_value);
	    Assert.assertTrue(r2.byte_value==rb.byte_value);
	    Assert.assertTrue(r2.char_value==rb.char_value);
	    Assert.assertTrue(r2.boolean_value==rb.boolean_value);
	    Assert.assertTrue(r2.short_value==rb.short_value);
	    Assert.assertTrue(r2.long_value==rb.long_value);
	    Assert.assertTrue(r2.float_value==rb.float_value);
	    Assert.assertTrue(r2.double_value==rb.double_value);
	    Assert.assertTrue(r2.string_value.equals(rb.string_value));
	    Assert.assertTrue(r2.IntegerNumber_value.equals(rb.IntegerNumber_value));
	    Assert.assertTrue(r2.ByteNumber_value.equals(rb.ByteNumber_value));
	    Assert.assertTrue(r2.CharacterNumber_value.equals(rb.CharacterNumber_value));
	    Assert.assertTrue(r2.BooleanNumber_value.equals(rb.BooleanNumber_value));
	    Assert.assertTrue(r2.ShortNumber_value.equals(rb.ShortNumber_value));
	    Assert.assertTrue(r2.LongNumber_value.equals(rb.LongNumber_value));
	    Assert.assertTrue(r2.FloatNumber_value.equals(rb.FloatNumber_value));
	    Assert.assertTrue(r2.DoubleNumber_value.equals(rb.DoubleNumber_value));
	    Assert.assertTrue(r2.BigInteger_value.equals(rb.BigInteger_value));
	    Assert.assertTrue(r2.BigDecimal_value.equals(rb.BigDecimal_value));
	    Assert.assertTrue(r2.DateValue.equals(rb.DateValue));
	    Assert.assertTrue(r2.CalendarValue.equals(rb.CalendarValue));
	    Assert.assertTrue(r2.secretKey.equals(rb.secretKey));
	    Assert.assertTrue(r2.typeSecretKey.equals(rb.typeSecretKey));
	    
	    for (int i=0;i<3;i++)
		Assert.assertTrue(rb.byte_array_value[i]==tab[i]);
	    
	    Assert.assertTrue(r2.pk1==rc.pk1);
	    Assert.assertTrue(r2.pk2==rc.pk2);
	    Assert.assertTrue(r2.pk3.equals(rc.pk3));
	    Assert.assertTrue(r2.pk4==rc.pk4);
	    Assert.assertTrue(r2.pk5.equals(rc.pk5));
	    Assert.assertTrue(r2.pk6.equals(rc.pk6));
	    Assert.assertTrue(r2.pk7.equals(rc.pk7));
	    Assert.assertTrue(r2.int_value==rc.int_value);
	    Assert.assertTrue(r2.byte_value==rc.byte_value);
	    Assert.assertTrue(r2.char_value==rc.char_value);
	    Assert.assertTrue(r2.boolean_value==rc.boolean_value);
	    Assert.assertTrue(r2.short_value==rc.short_value);
	    Assert.assertTrue(r2.long_value==rc.long_value);
	    Assert.assertTrue(r2.float_value==rc.float_value);
	    Assert.assertTrue(r2.double_value==rc.double_value);
	    Assert.assertTrue(r2.string_value.equals(rc.string_value));
	    Assert.assertTrue(r2.IntegerNumber_value.equals(rc.IntegerNumber_value));
	    Assert.assertTrue(r2.ByteNumber_value.equals(rc.ByteNumber_value));
	    Assert.assertTrue(r2.CharacterNumber_value.equals(rc.CharacterNumber_value));
	    Assert.assertTrue(r2.BooleanNumber_value.equals(rc.BooleanNumber_value));
	    Assert.assertTrue(r2.ShortNumber_value.equals(rc.ShortNumber_value));
	    Assert.assertTrue(r2.LongNumber_value.equals(rc.LongNumber_value));
	    Assert.assertTrue(r2.FloatNumber_value.equals(rc.FloatNumber_value));
	    Assert.assertTrue(r2.DoubleNumber_value.equals(rc.DoubleNumber_value));
	    Assert.assertTrue(r2.BigInteger_value.equals(rc.BigInteger_value));
	    Assert.assertTrue(r2.BigDecimal_value.equals(rc.BigDecimal_value));
	    Assert.assertTrue(r2.DateValue.equals(rc.DateValue));
	    Assert.assertTrue(r2.CalendarValue.equals(rc.CalendarValue));
	    Assert.assertTrue(r1.secretKey.equals(rc.secretKey));
	    Assert.assertTrue(r1.typeSecretKey.equals(rc.typeSecretKey));
	    for (int i=0;i<3;i++)
		Assert.assertTrue(rc.byte_array_value[i]==tab[i]);
	    
	    Assert.assertTrue(r2.pk1==re.pk1);
	    Assert.assertTrue(r2.pk2==re.pk2);
	    Assert.assertTrue(r2.pk3.equals(re.pk3));
	    Assert.assertTrue(r2.pk4==re.pk4);
	    Assert.assertTrue(r2.pk5.equals(re.pk5));
	    Assert.assertTrue(r2.pk6.equals(re.pk6));
	    Assert.assertTrue(r2.pk7.equals(re.pk7));
	    Assert.assertTrue(r2.int_value==re.int_value);
	    Assert.assertTrue(r2.byte_value==re.byte_value);
	    Assert.assertTrue(r2.char_value==re.char_value);
	    Assert.assertTrue(r2.boolean_value==re.boolean_value);
	    Assert.assertTrue(r2.short_value==re.short_value);
	    Assert.assertTrue(r2.long_value==re.long_value);
	    Assert.assertTrue(r2.float_value==re.float_value);
	    Assert.assertTrue(r2.double_value==re.double_value);
	    Assert.assertTrue(r2.string_value.equals(re.string_value));
	    Assert.assertTrue(r2.IntegerNumber_value.equals(re.IntegerNumber_value));
	    Assert.assertTrue(r2.ByteNumber_value.equals(re.ByteNumber_value));
	    Assert.assertTrue(r2.CharacterNumber_value.equals(re.CharacterNumber_value));
	    Assert.assertTrue(r2.BooleanNumber_value.equals(re.BooleanNumber_value));
	    Assert.assertTrue(r2.ShortNumber_value.equals(re.ShortNumber_value));
	    Assert.assertTrue(r2.LongNumber_value.equals(re.LongNumber_value));
	    Assert.assertTrue(r2.FloatNumber_value.equals(re.FloatNumber_value));
	    Assert.assertTrue(r2.DoubleNumber_value.equals(re.DoubleNumber_value));
	    Assert.assertTrue(r2.BigInteger_value.equals(re.BigInteger_value));
	    Assert.assertTrue(r2.BigDecimal_value.equals(re.BigDecimal_value));
	    Assert.assertTrue(r2.DateValue.equals(re.DateValue));
	    Assert.assertTrue(r2.CalendarValue.equals(re.CalendarValue));
	    Assert.assertTrue(r2.secretKey.equals(re.secretKey));
	    Assert.assertTrue(r2.typeSecretKey.equals(re.typeSecretKey));
	    for (int i=0;i<3;i++)
		Assert.assertTrue(re.byte_array_value[i]==tab[i]);

    
	    
	    Table2.Record t2=table2.getRecords().get(0);
	    HashMap<String, Object> t2map=new HashMap<String, Object>();
	    t2map.put("fr1_pk1", table1.getRecords().get(1));
	    table2.updateRecord(t2, t2map);
	    
	    Table2.Record t2bis=table6.getRecords().get(0).fk1_pk1;
	    Table1.Record t1=table1.getRecords().get(1);
	    
	    Assert.assertTrue(t2.fr1_pk1.pk1==t2bis.fr1_pk1.pk1);
	    Assert.assertTrue(t2.fr1_pk1.pk2==t2bis.fr1_pk1.pk2);
	    Assert.assertTrue(t2.fr1_pk1.pk4==t2bis.fr1_pk1.pk4);
	    
	    Assert.assertTrue(t1.pk1==t2bis.fr1_pk1.pk1);
	    Assert.assertTrue(t1.pk2==t2bis.fr1_pk1.pk2);
	    Assert.assertTrue(t1.pk4==t2bis.fr1_pk1.pk4);
	    
	    HashMap<String, Object> t2map2=new HashMap<String, Object>();
	    t2map2.put("int_value", new Integer(t2.int_value));
	    
	    table2.updateRecord(t2, t2map2);
	    t2map2.put("fr1_pk1", table1.getRecords().get(1));
	    

	    try
	    {
		table2.addRecord(t2map2);
		Assert.assertTrue(false);
	    }
	    catch(ConstraintsNotRespectedDatabaseException e)
	    {
		Assert.assertTrue(true);
	    }
	    t2map.put("int_value", new Integer(t2.int_value+1));
	    try
	    {
		table2.addRecord(t2map);
		Assert.assertTrue(false);
	    }
	    catch(ConstraintsNotRespectedDatabaseException e)
	    {
		Assert.assertTrue(true);
	    }
	    t2map.remove("fr1_pk1");
	    t2map.put("fr1_pk1", table1.getRecords().get(0));
	    table2.addRecord(t2map);
	    t2map.remove("fr1_pk1");
	    try
	    {
		table2.updateRecord(t2, t2map);
		Assert.assertTrue(false);
	    }
	    catch(ConstraintsNotRespectedDatabaseException e)
	    {
		Assert.assertTrue(true);
	    }
	    
	    
	    table1.updateRecords(new AlterRecordFilter<Table1.Record>() {
	        
	        @Override
	        public void nextRecord(Record _record)
	        {
	            HashMap<String, Object> m=new HashMap<String, Object>();
	            m.put("int_value", new Integer(15));
	            this.update(m);
	        }
	    });
	    ArrayList<Table1.Record> records1=table1.getRecords();
	    for (Table1.Record r : records1)
		Assert.assertTrue(r.int_value==15);
	    
	    ArrayList<Table3.Record> records3=table3.getRecords();
	    table3.updateRecords(new AlterRecordFilter<Table3.Record>() {
	        
	        @Override
	        public void nextRecord(Table3.Record _record)
	        {
	            HashMap<String, Object> m=new HashMap<String, Object>();
	            m.put("int_value", new Integer(15));
	            
	            this.update(m);
	        }
	    });
	    
	    for (Table3.Record r : records3)
		Assert.assertTrue(r.int_value==15);
	    
	    try
	    {
		table1.updateRecords(new AlterRecordFilter<Table1.Record>() {
		    
		    @Override
		    public void nextRecord(Record _record)
		    {
			HashMap<String, Object> m=new HashMap<String, Object>();
			m.put("pk1", new Integer(15));
			this.update(m);
		    }
		});
		Assert.assertTrue(false);
	    }
	    catch(FieldDatabaseException e)
	    {
		Assert.assertTrue(true);
	    }
	    try
	    {
		table3.updateRecords(new AlterRecordFilter<Table3.Record>() {
		    
		    @Override
		    public void nextRecord(Table3.Record _record)
		    {
			HashMap<String, Object> m=new HashMap<String, Object>();
			m.put("pk1", new Integer(15));
			this.update(m);
		    }
		});
		Assert.assertTrue(false);
	    }
	    catch(FieldDatabaseException e)
	    {
		Assert.assertTrue(true);
	    }
	    try
	    {
		table2.updateRecords(new AlterRecordFilter<Table2.Record>() {
		    
		    @Override
		    public void nextRecord(Table2.Record _record)
		    {
			HashMap<String, Object> m=new HashMap<String, Object>();
			m.put("int_value", new Integer(15));
			this.update(m);
		    }
		});
		Assert.assertTrue(false);
	    }
	    catch(FieldDatabaseException e)
	    {
		Assert.assertTrue(true);
	    }
	    
	    
	    
	    table1.checkDataIntegrity();
	    table3.checkDataIntegrity();
	    table2.checkDataIntegrity();
	    table4.checkDataIntegrity();
	    table5.checkDataIntegrity();
	    table6.checkDataIntegrity();
	
    }
    private final boolean equals(DatabaseRecord _instance1, DatabaseRecord _instance2) throws DatabaseException
    {
	    if (_instance1==null || _instance2==null)
		return _instance1==_instance2;
	    if (_instance1==_instance2)
		return true;
	    Table<?> t=sql_db.getTableInstance(Table.getTableClass(_instance1.getClass()));
	    
	    for (FieldAccessor fa : t.getPrimaryKeysFieldAccessors())
	    {
		if (!fa.equals(_instance1, fa.getValue(_instance2)))
		    return false;
	    }
	    return true;

    }
    
    @Test(dependsOnMethods={"alterRecordWithCascade"}) public void removePointedRecords() throws DatabaseException
    {
	Assert.assertTrue(table1.getRecordsNumber()==2);
	Assert.assertTrue(table3.getRecordsNumber()==2);
	Assert.assertTrue(table2.getRecordsNumber()==2);
	Assert.assertTrue(table4.getRecordsNumber()==1);
	Assert.assertTrue(table5.getRecordsNumber()==1);
	Assert.assertTrue(table1.getRecords().size()==2);
	Assert.assertTrue(table2.getRecords().size()==2);
	Assert.assertTrue(table3.getRecords().size()==2);
	Assert.assertTrue(table4.getRecords().size()==1);
	Assert.assertTrue(table5.getRecords().size()==1);
	Assert.assertTrue(table6.getRecords().size()==1);
	
	table1.removeRecords(new Filter<Table1.Record>() {

	    @Override
	    public boolean nextRecord(Record _record)
	    {
		return true;
	    }
	});
	table3.removeRecords(new Filter<Table3.Record>() {

	    @Override
	    public boolean nextRecord(Table3.Record _record)
	    {
		return true;
	    }
	});
	
	Assert.assertTrue(table1.getRecordsNumber()==2);
	Assert.assertTrue(table3.getRecordsNumber()==1);
	Assert.assertTrue(table2.getRecordsNumber()==2);
	Assert.assertTrue(table4.getRecordsNumber()==1);
	Assert.assertTrue(table5.getRecordsNumber()==1);
	Assert.assertTrue(table1.getRecords().size()==2);
	Assert.assertTrue(table2.getRecords().size()==2);
	Assert.assertTrue(table3.getRecords().size()==1);
	Assert.assertTrue(table4.getRecords().size()==1);
	Assert.assertTrue(table5.getRecords().size()==1);
	
	try
	{
	    table1.removeRecords(table1.getRecords());
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table3.removeRecords(table3.getRecords());
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	Assert.assertTrue(table1.getRecordsNumber()==2);
	Assert.assertTrue(table3.getRecordsNumber()==1);
	Assert.assertTrue(table1.getRecords().size()==2);
	Assert.assertTrue(table3.getRecords().size()==1);
	try
	{
	    table1.removeRecord(table1.getRecords().get(0));
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table3.removeRecord(table3.getRecords().get(0));
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	    table1.checkDataIntegrity();
	    table3.checkDataIntegrity();
	    table2.checkDataIntegrity();
	    table4.checkDataIntegrity();
	    table5.checkDataIntegrity();
	    table6.checkDataIntegrity();
	
	
    }
    @Test(dependsOnMethods={"removePointedRecords"}) public void removeForeignKeyRecords() throws DatabaseException
    {
	table1.removeRecords(new Filter<Table1.Record>() {

	    @Override
	    public boolean nextRecord(com.distrimind.ood.tests.database.Table1.Record _record)
	    {
		return true;
	    }
	});
	table3.removeRecords(new Filter<Table3.Record>() {

	    @Override
	    public boolean nextRecord(com.distrimind.ood.tests.database.Table3.Record _record)
	    {
		return true;
	    }
	});
	table2.removeRecords(new Filter<Table2.Record>() {

	    @Override
	    public boolean nextRecord(com.distrimind.ood.tests.database.Table2.Record _record)
	    {
		return true;
	    }
	});
	table4.removeRecords(new Filter<Table4.Record>() {

	    @Override
	    public boolean nextRecord(com.distrimind.ood.tests.database.Table4.Record _record)
	    {
		return true;
	    }
	});
	table5.removeRecords(new Filter<Table5.Record>() {

	    @Override
	    public boolean nextRecord(com.distrimind.ood.tests.database.Table5.Record _record)
	    {
		return true;
	    }
	});
	Assert.assertTrue(table1.getRecordsNumber()==2);
	Assert.assertTrue(table3.getRecordsNumber()==1);
	Assert.assertTrue(table2.getRecordsNumber()==1);
	Assert.assertTrue(table4.getRecordsNumber()==0);
	Assert.assertTrue(table5.getRecordsNumber()==1);
	Assert.assertTrue(table1.getRecords().size()==2);
	Assert.assertTrue(table3.getRecords().size()==1);
	Assert.assertTrue(table2.getRecords().size()==1);
	Assert.assertTrue(table4.getRecords().size()==0);
	Assert.assertTrue(table5.getRecords().size()==1);
	
	table6.removeRecords(new Filter<Table6.Record>() {

	    @Override
	    public boolean nextRecord(com.distrimind.ood.tests.database.Table6.Record _record)
	    {
		return true;
	    }
	});
	Assert.assertTrue(table6.getRecordsNumber()==0);
	HashMap<String, Object> map6=new HashMap<String, Object>();
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
    @Test(invocationCount=0) private void addRecords() throws DatabaseException
    {
	addSecondRecord();
	addSecondRecord();
	Table1.Record r1=table1.getRecords().get(0);
	Table3.Record r2=table3.getRecords().get(0);
	HashMap<String, Object> map1=new HashMap<String, Object>();
	map1.put("fr1_pk1", r1);
	map1.put("int_value", new Integer(0));
	HashMap<String, Object> map2=new HashMap<String, Object>();
	map2.put("fr1_pk1", r2);
	map2.put("int_value", new Integer(0));
	Table2.Record r22=table2.addRecord(map1);
	table4.addRecord(map2);
	Table5.Record r55=table5.addRecord(map2);
	
	HashMap<String, Object> map6=new HashMap<String, Object>();
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
    @Test(dependsOnMethods={"removeForeignKeyRecords"}) public void testIsPointed() throws DatabaseException
    {
	boolean b1=false;
	Table1.Record r1=table1.getRecords().get(0);
	for (Table2.Record r2 : table2.getRecords())
	{
	    if (table1.equals(r2.fr1_pk1, r1))
	    {
		b1=true;
		break;
	    }
	}
	boolean b3=false;
	Table3.Record r3=table3.getRecords().get(0);
	for (Table4.Record r4 : table4.getRecords())
	{
	    if (table3.equals(r4.fr1_pk1, r3))
	    {
		b3=true;
		break;
	    }
	}
	for (Table5.Record r5 : table5.getRecords())
	{
	    if (table3.equals(r5.fr1_pk1, r3))
	    {
		b3=true;
		break;
	    }
	}
	Assert.assertTrue(table1.isRecordPointedByForeignKeys(r1)==b1);
	Assert.assertTrue(table3.isRecordPointedByForeignKeys(r3)==b3);
	    table1.checkDataIntegrity();
	    table3.checkDataIntegrity();
	    table2.checkDataIntegrity();
	    table4.checkDataIntegrity();
	    table5.checkDataIntegrity();
	    table6.checkDataIntegrity();
	
    }
    @Test(dependsOnMethods={"testIsPointed"}) public void removeWithCascade() throws DatabaseException
    {
	table2.removeRecordsWithCascade(new Filter<Table2.Record>() {

	    @Override
	    public boolean nextRecord(com.distrimind.ood.tests.database.Table2.Record _record)
	    {
		return true;
	    }
	});
	table5.removeRecordsWithCascade(new Filter<Table5.Record>() {

	    @Override
	    public boolean nextRecord(com.distrimind.ood.tests.database.Table5.Record _record)
	    {
		return true;
	    }
	});
	Assert.assertTrue(table2.getRecordsNumber()==0);
	Assert.assertTrue(table5.getRecordsNumber()==0);
	Assert.assertTrue(table6.getRecordsNumber()==0);
	Assert.assertTrue(table2.getRecords().size()==0);
	Assert.assertTrue(table5.getRecords().size()==0);
	Assert.assertTrue(table6.getRecords().size()==0);
	
	addRecords();
	table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

	    @Override
	    public boolean nextRecord(Record _record)
	    {
		return true;
	    }
	});
	table3.removeRecordsWithCascade(new Filter<Table3.Record>() {

	    @Override
	    public boolean nextRecord(Table3.Record _record)
	    {
		return true;
	    }
	});
	Assert.assertTrue(table1.getRecordsNumber()==0);
	Assert.assertTrue(table3.getRecordsNumber()==0);
	Assert.assertTrue(table2.getRecordsNumber()==0);
	Assert.assertTrue(table4.getRecordsNumber()==0);
	Assert.assertTrue(table5.getRecordsNumber()==0);
	Assert.assertTrue(table6.getRecordsNumber()==0);
	Assert.assertTrue(table1.getRecords().size()==0);
	Assert.assertTrue(table2.getRecords().size()==0);
	Assert.assertTrue(table3.getRecords().size()==0);
	Assert.assertTrue(table4.getRecords().size()==0);
	Assert.assertTrue(table5.getRecords().size()==0);
	Assert.assertTrue(table6.getRecords().size()==0);

    
	addRecords();
	table1.removeRecordWithCascade(table1.getRecords().get(0));
	table3.removeRecordWithCascade(table3.getRecords().get(0));
	Assert.assertTrue(table1.getRecordsNumber()==1);
	Assert.assertTrue(table3.getRecordsNumber()==1);
	Assert.assertTrue(table2.getRecordsNumber()==0);
	Assert.assertTrue(table4.getRecordsNumber()==0);
	Assert.assertTrue(table5.getRecordsNumber()==0);
	Assert.assertTrue(table6.getRecordsNumber()==0);
	Assert.assertTrue(table1.getRecords().size()==1);
	Assert.assertTrue(table2.getRecords().size()==0);
	Assert.assertTrue(table3.getRecords().size()==1);
	Assert.assertTrue(table4.getRecords().size()==0);
	Assert.assertTrue(table5.getRecords().size()==0);
	Assert.assertTrue(table6.getRecords().size()==0);
	table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

	    @Override
	    public boolean nextRecord(Record _record)
	    {
		return true;
	    }
	});
	table3.removeRecordsWithCascade(new Filter<Table3.Record>() {

	    @Override
	    public boolean nextRecord(Table3.Record _record)
	    {
		return true;
	    }
	});
	Assert.assertTrue(table1.getRecordsNumber()==0);
	Assert.assertTrue(table3.getRecordsNumber()==0);
	Assert.assertTrue(table2.getRecordsNumber()==0);
	Assert.assertTrue(table4.getRecordsNumber()==0);
	Assert.assertTrue(table5.getRecordsNumber()==0);
	Assert.assertTrue(table6.getRecordsNumber()==0);
	Assert.assertTrue(table1.getRecords().size()==0);
	Assert.assertTrue(table2.getRecords().size()==0);
	Assert.assertTrue(table3.getRecords().size()==0);
	Assert.assertTrue(table4.getRecords().size()==0);
	Assert.assertTrue(table5.getRecords().size()==0);
	Assert.assertTrue(table6.getRecords().size()==0);
	addRecords();
	table1.removeRecordsWithCascade(table1.getRecords());
	table3.removeRecordsWithCascade(table3.getRecords());
	Assert.assertTrue(table1.getRecordsNumber()==0);
	Assert.assertTrue(table3.getRecordsNumber()==0);
	Assert.assertTrue(table2.getRecordsNumber()==0);
	Assert.assertTrue(table4.getRecordsNumber()==0);
	Assert.assertTrue(table5.getRecordsNumber()==0);
	Assert.assertTrue(table6.getRecordsNumber()==0);
	Assert.assertTrue(table1.getRecords().size()==0);
	Assert.assertTrue(table2.getRecords().size()==0);
	Assert.assertTrue(table3.getRecords().size()==0);
	Assert.assertTrue(table4.getRecords().size()==0);
	Assert.assertTrue(table5.getRecords().size()==0);
	Assert.assertTrue(table6.getRecords().size()==0);
	
	prepareMultipleTest();

	table1.updateRecords(new AlterRecordFilter<Table1.Record>() {

	    @Override
	    public void nextRecord(Record _record)
	    {
		this.removeWithCascade();
	    }
	});
	table3.updateRecords(new AlterRecordFilter<Table3.Record>() {

	    @Override
	    public void nextRecord(Table3.Record _record)
	    {
		this.removeWithCascade();
	    }
	});
	Assert.assertTrue(0==table1.getRecordsNumber());
	Assert.assertTrue(0==table1.getRecords().size());
	
	Assert.assertTrue(0==table3.getRecordsNumber());
	Assert.assertTrue(0==table3.getRecords().size());
	
	
	table1.checkDataIntegrity();
	table3.checkDataIntegrity();
	table2.checkDataIntegrity();
	table4.checkDataIntegrity();
	table5.checkDataIntegrity();
	table6.checkDataIntegrity();
    }
    
    @SuppressWarnings("unchecked")
    @Test(dependsOnMethods={"removeWithCascade"}) public void setAutoRandomFields() throws DatabaseException
    {
	    HashMap<String, Object> map=new HashMap<String, Object>();
	    map.put("pk1", new Integer(0));
	    map.put("pk2", new Long(1));
	    map.put("pk3", new BigInteger("0"));
	    map.put("pk4", new Long(0));
	    map.put("pk5", new DecentralizedIDGenerator());
	    map.put("pk6", new DecentralizedIDGenerator());
	    map.put("pk7", new RenforcedDecentralizedIDGenerator());
	    
	    map.put("int_value", new Integer(3));
	    map.put("byte_value", new Byte((byte)3));
	    map.put("char_value", new Character('x'));
	    map.put("boolean_value", new Boolean(false));
	    map.put("short_value", new Short((short)3));
	    map.put("long_value", new Long(3));
	    map.put("float_value", new Float(3.3f));
	    map.put("double_value", new Double(3.3));
	    map.put("string_value", new String("test string"));
	    map.put("IntegerNumber_value", new Integer(3));
	    map.put("ByteNumber_value", new Byte((byte)3));
	    map.put("CharacterNumber_value", new Character('x'));
	    map.put("BooleanNumber_value", new Boolean(false));
	    map.put("ShortNumber_value", new Short((short)3));
	    map.put("LongNumber_value", new Long((long)3));
	    map.put("FloatNumber_value", new Float(3.3f));
	    map.put("DoubleNumber_value", new Double(3.3));
	    map.put("BigInteger_value", new BigInteger("5"));
	    map.put("BigDecimal_value", new BigDecimal("8.8"));
	    map.put("DateValue", date);
	    map.put("CalendarValue", calendar);
	    map.put("secretKey", secretKey);
	    map.put("typeSecretKey", typeSecretKey);
	    map.put("subField", subField);
	    map.put("subSubField", subSubField);
	table1.addRecord(map);
	table3.addRecord(map);
	Table1.Record r1=table1.getRecords().get(0);
	
	Assert.assertTrue(map.get("pk1").equals(new Integer(r1.pk1)));
	Assert.assertTrue(map.get("pk2").equals(new Long(r1.pk2)));
	Assert.assertTrue(map.get("pk3").equals(r1.pk3));
	Assert.assertTrue(map.get("pk4").equals(new Long(r1.pk4)));
	Assert.assertTrue(map.get("pk5").equals(r1.pk5));
	Assert.assertTrue(map.get("pk6").equals(r1.pk6));
	Assert.assertTrue(map.get("pk7").equals(r1.pk7));
	Assert.assertTrue(map.get("int_value").equals(new Integer(r1.int_value)));
	Assert.assertTrue(map.get("byte_value").equals(new Byte(r1.byte_value)));
	Assert.assertTrue(map.get("char_value").equals(new Character(r1.char_value)));
	Assert.assertTrue(map.get("boolean_value").equals(new Boolean(r1.boolean_value)));
	Assert.assertTrue(map.get("short_value").equals(new Short(r1.short_value)));
	Assert.assertTrue(map.get("long_value").equals(new Long(r1.long_value)));
	Assert.assertTrue(map.get("float_value").equals(new Float(r1.float_value)));
	Assert.assertTrue(map.get("double_value").equals(new Double(r1.double_value)));
	Assert.assertTrue(map.get("string_value").equals(r1.string_value));
	Assert.assertTrue(map.get("IntegerNumber_value").equals(r1.IntegerNumber_value));
	Assert.assertTrue(map.get("ByteNumber_value").equals(r1.ByteNumber_value));
	Assert.assertTrue(map.get("CharacterNumber_value").equals(r1.CharacterNumber_value));
	Assert.assertTrue(map.get("BooleanNumber_value").equals(r1.BooleanNumber_value));
	Assert.assertTrue(map.get("ShortNumber_value").equals(r1.ShortNumber_value));
	Assert.assertTrue(map.get("LongNumber_value").equals(r1.LongNumber_value));
	Assert.assertTrue(map.get("FloatNumber_value").equals(r1.FloatNumber_value));
	Assert.assertTrue(map.get("DoubleNumber_value").equals(r1.DoubleNumber_value));
	Assert.assertTrue(map.get("BigInteger_value").equals(r1.BigInteger_value));
	Assert.assertTrue(map.get("BigDecimal_value").equals(r1.BigDecimal_value));
	Assert.assertTrue(map.get("DateValue").equals(r1.DateValue));
	Assert.assertTrue(map.get("CalendarValue").equals(r1.CalendarValue));
	Assert.assertTrue(map.get("secretKey").equals(r1.secretKey));
	Assert.assertTrue(map.get("typeSecretKey").equals(r1.typeSecretKey));
	assertEquals((SubField)map.get("subField"), r1.subField);
	assertEquals((SubSubField)map.get("subSubField"), r1.subSubField);
	
	Table3.Record r3=table3.getRecords().get(0);
	
	Assert.assertTrue(map.get("pk1").equals(new Integer(r3.pk1)));
	Assert.assertTrue(map.get("pk2").equals(new Long(r3.pk2)));
	Assert.assertTrue(map.get("pk3").equals(r3.pk3));
	Assert.assertTrue(map.get("pk4").equals(new Long(r3.pk4)));
	Assert.assertTrue(map.get("pk5").equals(r3.pk5));
	Assert.assertTrue(map.get("pk6").equals(r3.pk6));
	Assert.assertTrue(map.get("pk7").equals(r3.pk7));
	Assert.assertTrue(map.get("int_value").equals(new Integer(r3.int_value)));
	Assert.assertTrue(map.get("byte_value").equals(new Byte(r3.byte_value)));
	Assert.assertTrue(map.get("char_value").equals(new Character(r3.char_value)));
	Assert.assertTrue(map.get("boolean_value").equals(new Boolean(r3.boolean_value)));
	Assert.assertTrue(map.get("short_value").equals(new Short(r3.short_value)));
	Assert.assertTrue(map.get("long_value").equals(new Long(r3.long_value)));
	Assert.assertTrue(map.get("float_value").equals(new Float(r3.float_value)));
	Assert.assertTrue(map.get("double_value").equals(new Double(r3.double_value)));
	Assert.assertTrue(map.get("string_value").equals(r3.string_value));
	Assert.assertTrue(map.get("IntegerNumber_value").equals(r3.IntegerNumber_value));
	Assert.assertTrue(map.get("ByteNumber_value").equals(r3.ByteNumber_value));
	Assert.assertTrue(map.get("CharacterNumber_value").equals(r3.CharacterNumber_value));
	Assert.assertTrue(map.get("BooleanNumber_value").equals(r3.BooleanNumber_value));
	Assert.assertTrue(map.get("ShortNumber_value").equals(r3.ShortNumber_value));
	Assert.assertTrue(map.get("LongNumber_value").equals(r3.LongNumber_value));
	Assert.assertTrue(map.get("FloatNumber_value").equals(r3.FloatNumber_value));
	Assert.assertTrue(map.get("DoubleNumber_value").equals(r3.DoubleNumber_value));
	Assert.assertTrue(map.get("BigInteger_value").equals(r3.BigInteger_value));
	Assert.assertTrue(map.get("BigDecimal_value").equals(r3.BigDecimal_value));
	Assert.assertTrue(map.get("DateValue").equals(r3.DateValue));
	Assert.assertTrue(map.get("CalendarValue").equals(r3.CalendarValue));
	Assert.assertTrue(map.get("secretKey").equals(r3.secretKey));
	Assert.assertTrue(map.get("typeSecretKey").equals(r3.typeSecretKey));
	assertEquals((SubField)map.get("subField"), r3.subField);
	assertEquals((SubSubField)map.get("subSubField"), r3.subSubField);
	
	try
	{
	    table1.addRecord(map);
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table3.addRecord(map);
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	map.put("pk4", new Long(1));
	map.put("pk5", new DecentralizedIDGenerator());
	map.put("pk6", new DecentralizedIDGenerator());
	map.put("pk7", new RenforcedDecentralizedIDGenerator());
	try
	{
	    table1.addRecord(map);
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table3.addRecord(map);
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	
	map.put("pk3", new BigInteger("1"));
	table1.addRecord(map);
	table3.addRecord(map);
	map.put("pk2", new Long(2));
	try
	{
	    table1.addRecord(map);
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table3.addRecord(map);
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	map.put("pk2", new Long("2"));
	map.put("pk3", new BigInteger("2"));
	Map<String, Object> maps[]=new Map[2];
	maps[0]=map;
	maps[1]=map;
	try
	{
	    table1.addRecords(maps);
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table3.addRecords(maps);
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	maps[1]=(HashMap<String, Object>)map.clone();
	maps[1].remove("pk2");
	try
	{
	    table1.addRecords(maps);
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table3.addRecords(maps);
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}

	maps[1].remove("pk3");
	
	try
	{
	    table1.addRecords(maps);
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table3.addRecords(maps);
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	
	
	Assert.assertTrue(table1.getRecordsNumber()==2);
	Assert.assertTrue(table3.getRecordsNumber()==2);
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
	Assert.assertTrue(table1.getRecordsNumber()==2);
	Assert.assertTrue(table3.getRecordsNumber()==2);
	r1=table1.getRecords().get(0);
	
	Assert.assertTrue(map.get("pk1").equals(new Integer(r1.pk1)));
	Assert.assertTrue(map.get("pk2").equals(new Long(r1.pk2)));
	Assert.assertTrue(map.get("pk3").equals(r1.pk3));
	Assert.assertTrue(map.get("pk4").equals(new Long(r1.pk4)));
	Assert.assertTrue(map.get("pk5").equals(r1.pk5));
	Assert.assertTrue(map.get("pk6").equals(r1.pk6));
	Assert.assertTrue(map.get("pk7").equals(r1.pk7));
	Assert.assertTrue(map.get("int_value").equals(new Integer(r1.int_value)));
	Assert.assertTrue(map.get("byte_value").equals(new Byte(r1.byte_value)));
	Assert.assertTrue(map.get("char_value").equals(new Character(r1.char_value)));
	Assert.assertTrue(map.get("boolean_value").equals(new Boolean(r1.boolean_value)));
	Assert.assertTrue(map.get("short_value").equals(new Short(r1.short_value)));
	Assert.assertTrue(map.get("long_value").equals(new Long(r1.long_value)));
	Assert.assertTrue(map.get("float_value").equals(new Float(r1.float_value)));
	Assert.assertTrue(map.get("double_value").equals(new Double(r1.double_value)));
	Assert.assertTrue(map.get("string_value").equals(r1.string_value));
	Assert.assertTrue(map.get("IntegerNumber_value").equals(r1.IntegerNumber_value));
	Assert.assertTrue(map.get("ByteNumber_value").equals(r1.ByteNumber_value));
	Assert.assertTrue(map.get("CharacterNumber_value").equals(r1.CharacterNumber_value));
	Assert.assertTrue(map.get("BooleanNumber_value").equals(r1.BooleanNumber_value));
	Assert.assertTrue(map.get("ShortNumber_value").equals(r1.ShortNumber_value));
	Assert.assertTrue(map.get("LongNumber_value").equals(r1.LongNumber_value));
	Assert.assertTrue(map.get("FloatNumber_value").equals(r1.FloatNumber_value));
	Assert.assertTrue(map.get("DoubleNumber_value").equals(r1.DoubleNumber_value));
	Assert.assertTrue(map.get("BigInteger_value").equals(r1.BigInteger_value));
	Assert.assertTrue(map.get("BigDecimal_value").equals(r1.BigDecimal_value));
	assertEquals((SubField)map.get("subField"), r1.subField);
	assertEquals((SubSubField)map.get("subSubField"), r1.subSubField);
	
	r3=table3.getRecords().get(0);
	
	Assert.assertTrue(map.get("pk1").equals(new Integer(r3.pk1)));
	Assert.assertTrue(map.get("pk2").equals(new Long(r3.pk2)));
	Assert.assertTrue(map.get("pk3").equals(r3.pk3));
	Assert.assertTrue(map.get("pk4").equals(new Long(r3.pk4)));
	Assert.assertTrue(map.get("pk5").equals(r3.pk5));
	Assert.assertTrue(map.get("pk6").equals(r3.pk6));
	Assert.assertTrue(map.get("pk7").equals(r3.pk7));
	Assert.assertTrue(map.get("int_value").equals(new Integer(r3.int_value)));
	Assert.assertTrue(map.get("byte_value").equals(new Byte(r3.byte_value)));
	Assert.assertTrue(map.get("char_value").equals(new Character(r3.char_value)));
	Assert.assertTrue(map.get("boolean_value").equals(new Boolean(r3.boolean_value)));
	Assert.assertTrue(map.get("short_value").equals(new Short(r3.short_value)));
	Assert.assertTrue(map.get("long_value").equals(new Long(r3.long_value)));
	Assert.assertTrue(map.get("float_value").equals(new Float(r3.float_value)));
	Assert.assertTrue(map.get("double_value").equals(new Double(r3.double_value)));
	Assert.assertTrue(map.get("string_value").equals(r3.string_value));
	Assert.assertTrue(map.get("IntegerNumber_value").equals(r3.IntegerNumber_value));
	Assert.assertTrue(map.get("ByteNumber_value").equals(r3.ByteNumber_value));
	Assert.assertTrue(map.get("CharacterNumber_value").equals(r3.CharacterNumber_value));
	Assert.assertTrue(map.get("BooleanNumber_value").equals(r3.BooleanNumber_value));
	Assert.assertTrue(map.get("ShortNumber_value").equals(r3.ShortNumber_value));
	Assert.assertTrue(map.get("LongNumber_value").equals(r3.LongNumber_value));
	Assert.assertTrue(map.get("FloatNumber_value").equals(r3.FloatNumber_value));
	Assert.assertTrue(map.get("DoubleNumber_value").equals(r3.DoubleNumber_value));
	Assert.assertTrue(map.get("BigInteger_value").equals(r3.BigInteger_value));
	Assert.assertTrue(map.get("BigDecimal_value").equals(r3.BigDecimal_value));
	assertEquals((SubField)map.get("subField"), r3.subField);
	assertEquals((SubSubField)map.get("subSubField"), r3.subSubField);
	
	Map<String, Object> maps2[]=new Map[1];
	maps2[0]=maps[0];

	try
	{
	    table1.addRecords(maps2);
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	try
	{
	    table3.addRecords(maps2);
	    Assert.assertTrue(false);
	}
	catch(ConstraintsNotRespectedDatabaseException e)
	{
	    Assert.assertTrue(true);
	}
	    table1.checkDataIntegrity();
	    table3.checkDataIntegrity();
	    table2.checkDataIntegrity();
	    table4.checkDataIntegrity();
	    table5.checkDataIntegrity();
	    table6.checkDataIntegrity();
	
    }
    
    private ArrayList<Object> getExpectedParameter(Class<?> type, Object o) throws IOException
    {
	ArrayList<Object> res=new ArrayList<>();
	if (o==null)
	{
	    res.add(null);
	    return res;
	}
	if (o.getClass()==DecentralizedIDGenerator.class)
	{
	    DecentralizedIDGenerator id=(DecentralizedIDGenerator)o;
	    res.add(new Long(id.getTimeStamp()));
	    res.add(new Long(id.getWorkerIDAndSequence()));
	}
	else if (o.getClass()==RenforcedDecentralizedIDGenerator.class)
	{
	    RenforcedDecentralizedIDGenerator id=(RenforcedDecentralizedIDGenerator)o;
	    res.add(new Long(id.getTimeStamp()));
	    res.add(new Long(id.getWorkerIDAndSequence()));
	}
	else if (DecentralizedIDGenerator.class.isAssignableFrom(type))
	{
	    DecentralizedIDGenerator d=(DecentralizedIDGenerator)o;
	    res.add(new Long(d.getTimeStamp()));
	    res.add(new Long(d.getWorkerIDAndSequence()));
	}
	else if (AbstractDecentralizedID.class.isAssignableFrom(type))
	{
	    AbstractDecentralizedID id=(AbstractDecentralizedID)o;
	    if (DatabaseWrapperAccessor.isVarBinarySupported(sql_db))
		res.add(id.getBytes());
	    else
	    {
		byte[] bytes=id.getBytes();
		BigInteger r=BigInteger.valueOf(1);
		for (int i=0;i<bytes.length;i++)
		{
		    r=r.shiftLeft(8).or(BigInteger.valueOf(bytes[i] & 0xFF));
		}
		res.add(new BigDecimal(r));		
	    }
	}
	else if (o.getClass()==int.class 
		|| o.getClass()==byte.class
		|| o.getClass()==char.class
		|| o.getClass()==boolean.class
		|| o.getClass()==short.class
		|| o.getClass()==long.class
		|| o.getClass()==float.class
		|| o.getClass()==double.class
		|| o.getClass()==Integer.class
		|| o.getClass()==Byte.class
		|| o.getClass()==Character.class
		|| o.getClass()==Boolean.class
		|| o.getClass()==Short.class
		|| o.getClass()==Long.class
		|| o.getClass()==Float.class
		|| o.getClass()==Double.class
		|| o.getClass()==byte[].class
		|| o.getClass()==String.class
		)
	{
	    res.add(o);
	    
	}
	else if (o.getClass()==BigInteger.class
		|| o.getClass()==BigDecimal.class)
	{
	    res.add(o.toString());
	}
	else if (o instanceof Date)
	{
	    res.add(new Timestamp(((Date)o).getTime()));
	}
	else if (o instanceof Serializable)
	{
	    if (DatabaseWrapperAccessor.getSerializableType(sql_db).equals("BLOB"))
	    {
		try(ByteArrayOutputStream baos=new ByteArrayOutputStream())
		{
		    try(ObjectOutputStream os=new ObjectOutputStream(baos))
		    {
			os.writeObject(o);
			res.add(baos.toByteArray());
		    }
		}    
	    }
	    else
		res.add(o);
	}
	return res;
    }
    private ArrayList<String> getExpectedParametersName(String variableName, Class<?> objectClass) 
    {
	ArrayList<String> res=new ArrayList<>();

	if (objectClass==DecentralizedIDGenerator.class)
	{
	    res.add(variableName+"_ts");
	    res.add(variableName+"_widseq");
	}
	else if (objectClass==RenforcedDecentralizedIDGenerator.class)
	{
	    res.add(variableName+"_ts");
	    res.add(variableName+"_widseq");
	}
	else 
	{
	    res.add(variableName);
	    
	}
	
	return res;
    }
    
    
    
    @DataProvider(name = "interpreterCommandsProvider")
    public Object[][] interpreterCommandsProvider() throws IOException, NoSuchAlgorithmException, NoSuchProviderException, DatabaseException
    {
	HashMap<String, Object> parametersTable1Equallable=new HashMap<>();
	
	AbstractSecureRandom rand=SecureRandomType.DEFAULT.getInstance();
	ArrayList<Object[]> res=new ArrayList<>();
	parametersTable1Equallable.put("pk5", new SecuredDecentralizedID(new DecentralizedIDGenerator(), rand));
	parametersTable1Equallable.put("pk6", new DecentralizedIDGenerator());
	parametersTable1Equallable.put("pk7", new RenforcedDecentralizedIDGenerator());
	parametersTable1Equallable.put("int_value", new Integer(1));
	parametersTable1Equallable.put("byte_value", new Byte((byte)1));
	parametersTable1Equallable.put("char_value", new Character('a'));
	parametersTable1Equallable.put("boolean_value", new Boolean(true));
	parametersTable1Equallable.put("short_value", new Short((short)1));
	parametersTable1Equallable.put("long_value", new Long(1));
	parametersTable1Equallable.put("float_value", new Float(1.0f));
	parametersTable1Equallable.put("double_value", new Double(1.0));
	parametersTable1Equallable.put("string_value", "string");
	
	parametersTable1Equallable.put("IntegerNumber_value", new Integer(1));
	parametersTable1Equallable.put("ByteNumber_value", new Byte((byte)1));
	parametersTable1Equallable.put("CharacterNumber_value", new Character('a'));
	parametersTable1Equallable.put("BooleanNumber_value", new Boolean(true));
	parametersTable1Equallable.put("ShortNumber_value", new Short((short)1));
	parametersTable1Equallable.put("LongNumber_value", new Long(1));
	parametersTable1Equallable.put("FloatNumber_value", new Float(1.0f));
	parametersTable1Equallable.put("DoubleNumber_value", new Double(1.0));
	
	Calendar calendar=Calendar.getInstance(Locale.CANADA);
	calendar.set(2045, 7, 29, 18, 32, 43);
	
	
	
	
	parametersTable1Equallable.put("byte_array_value", new byte[]{(byte)0, (byte)1});
	parametersTable1Equallable.put("BigInteger_value", BigInteger.valueOf(3));
	parametersTable1Equallable.put("BigDecimal_value", BigDecimal.valueOf(4));
	parametersTable1Equallable.put("DateValue", date);
	parametersTable1Equallable.put("CalendarValue", calendar);
	
	SymbolType []ops_cond=new SymbolType[]{SymbolType.ANDCONDITION, SymbolType.ORCONDITION};
	SymbolType []ops_comp=new SymbolType[]{SymbolType.EQUALOPERATOR, SymbolType.NOTEQUALOPERATOR};
	for (SymbolType op_cond : ops_cond)
	{
	    for (SymbolType op_comp : ops_comp)
	    {
		StringBuffer command=new StringBuffer();
		StringBuffer expectedCommand=new StringBuffer();
	    
		HashMap<Integer, Object> expectedParameters=new HashMap<>();
		int expectedParamterIndex=1;
		for (Map.Entry<String, Object> m : parametersTable1Equallable.entrySet())
		{
		    if (command.length()>0)
		    {
			command.append(" ");
			command.append(op_cond.getContent());
			command.append(" ");
			
			expectedCommand.append(" ");
			expectedCommand.append(op_cond.getContent());
			expectedCommand.append(" ");
		    }
		    command.append(m.getKey());
		    command.append(op_comp.getContent());
		    command.append("%");
		    command.append(m.getKey());
		    
		    
		    ArrayList<Object> sqlInstance=getExpectedParameter(table1.getFieldAccessor(m.getKey()).getFieldClassType(), m.getValue());
		    ArrayList<String> sqlVariablesName=getExpectedParametersName(m.getKey(), m.getValue().getClass());
		    expectedCommand.append("(");
		    for (int i=0;i<sqlInstance.size();i++)
		    {
			if (i>0)
			    expectedCommand.append(" AND ");
			expectedCommand.append(Table.getName(Table1.class));
			expectedCommand.append(".");
			expectedCommand.append(sqlVariablesName.get(i));
			expectedCommand.append(op_comp.getContent());
			expectedCommand.append("?");
			expectedParameters.put(new Integer(expectedParamterIndex++), sqlInstance.get(i));
		    }
		    expectedCommand.append(")");
		}
		res.add(new Object[]{table1, command.toString(), parametersTable1Equallable, expectedCommand.toString(), expectedParameters});
	    }
	}
	Object [][]resO=new Object[res.size()][];
	for (int i=0;i<res.size();i++)
	{
	    resO[i]=res.get(i);
	}
	return resO;
    }
    
    
    
    //@DataProvider(name = "interpreterCommandsVerifProvider", parallel = true)
    public Object[][] interpreterCommandsVerifProvider()
    {
	//TODO complete
	return null;
    }

    //@Test(dependsOnMethods={"setAutoRandomFields"}, dataProvider = "interpreterCommandsProvider") 
    public <T extends DatabaseRecord> void testIsConcernedInterpreterFunction(Table<T> table, T record, String command, Map<String, Object> parameters, boolean expectedBoolean) throws DatabaseException
    {
	boolean bool=Interpreter.getRuleInstance(command).isConcernedBy(table, parameters, record);
	Assert.assertEquals(bool, expectedBoolean);
    }
    
    
    @Test(dependsOnMethods={"firstLoad"}, dataProvider = "interpreterCommandsProvider") 
    public void testCommandTranslatorInterpreter(Table<?> table, String command, Map<String, Object> parameters, String expectedSqlCommand, Map<Integer, Object> expectedSqlParameters) throws DatabaseException
    {

	HashMap<Integer, Object> sqlParameters=new HashMap<>();
	String sqlCommand=Interpreter.getRuleInstance(command).translateToSqlQuery(table, parameters, sqlParameters).toString();
	Assert.assertEquals(sqlCommand, expectedSqlCommand.toUpperCase());
	for (Map.Entry<Integer, Object> e : sqlParameters.entrySet())
	{
	    Assert.assertEquals(e.getValue(), expectedSqlParameters.get(e.getKey()));
	}
    }
    
    
    private static AtomicInteger next_unique=new AtomicInteger(0);
    private static AtomicInteger number_thread_test=new AtomicInteger(0);
    @Test(dependsOnMethods={"setAutoRandomFields"}) public void prepareMultipleTest() throws DatabaseException
    {
	
	HashMap<String, Object> map=new HashMap<String, Object>();
	
	map.put("pk1", new Integer(random.nextInt()));
	map.put("int_value", new Integer(3));
	map.put("byte_value", new Byte((byte)3));
	map.put("char_value", new Character('x'));
	map.put("boolean_value", new Boolean(true));
	map.put("short_value", new Short((short)random.nextInt()));
	map.put("long_value", new Long(3));
	map.put("float_value", new Float(3.3f));
	map.put("double_value", new Double(3.3));
	map.put("string_value", new String("test string"));
	map.put("IntegerNumber_value", new Integer(3));
	map.put("ByteNumber_value", new Byte((byte)3));
	map.put("CharacterNumber_value", new Character('x'));
	map.put("BooleanNumber_value", new Boolean(true));
	map.put("ShortNumber_value", new Short((short)3));
	map.put("LongNumber_value", new Long(random.nextLong()));
	map.put("FloatNumber_value", new Float(3.3f));
	map.put("DoubleNumber_value", new Double(3.3));
	map.put("BigInteger_value", new BigInteger("6"));
	map.put("BigDecimal_value", new BigDecimal("9.9"));
	map.put("DateValue", Calendar.getInstance().getTime());
	map.put("CalendarValue", Calendar.getInstance());
	map.put("secretKey", secretKey);
	map.put("typeSecretKey", typeSecretKey);
	map.put("subField", subField);
	map.put("subSubField", subSubField);
	byte[] tab=new byte[3];
	tab[0]=0;
	tab[1]=1;
	tab[2]=2;
	map.put("byte_array_value", tab);
	table1.addRecord(map);
	table3.addRecord(map);
	@SuppressWarnings("unchecked")
	HashMap<String, Object> list[]=new HashMap[10];
	for (int i=0;i<list.length;i++)
	    list[i]=map;
	table1.addRecords(list);
	table3.addRecords(list);
	
	for (int i=0;i<8;i++)
	{
	    try
	    {
		ArrayList<Table1.Record> records=table1.getRecords();
		HashMap<String, Object> map2=new HashMap<String, Object>();
		map2.put("fr1_pk1", records.get(random.nextInt(records.size())));
		map2.put("int_value", new Integer(random.nextInt()));
		table2.addRecord(map2);
	    }
	    catch(ConstraintsNotRespectedDatabaseException e)
	    {
	    }
	    catch(RecordNotFoundDatabaseException e)
	    {
		if (no_thread)
		    throw e;
	    }
	}
	for (int i=0;i<8;i++)
	{
	    try
	    {
		ArrayList<Table3.Record> records=table3.getRecords();
		HashMap<String, Object> map2=new HashMap<String, Object>();
		map2.put("fr1_pk1", records.get(random.nextInt(records.size())));
		map2.put("int_value", new Integer(random.nextInt()));
		
		table4.addRecord(map2);
		table5.addRecord(map2);
	    }
	    catch(ConstraintsNotRespectedDatabaseException e)
	    {
		
	    }
	    catch(RecordNotFoundDatabaseException e)
	    {
		if (no_thread)
		    throw e;
	    }
	}
	for (int i=0;i<6;i++)
	{
	    try
	    {
		ArrayList<Table2.Record> records2=table2.getRecords();
		ArrayList<Table5.Record> records5=table5.getRecords();
		HashMap<String, Object> map2=new HashMap<String, Object>();
		map2.put("fk1_pk1", records2.get(random.nextInt(records2.size())));
		map2.put("fk2", records5.get(random.nextInt(records5.size())));
		table6.addRecord(map2);
	    }
	    catch(ConstraintsNotRespectedDatabaseException e)
	    {
	    }
	    catch(RecordNotFoundDatabaseException e)
	    {
		if (no_thread)
		    throw e;
	    }
	}
    }
    @Test(dependsOnMethods={"prepareMultipleTest"}) public void multipleTests() throws DatabaseException
    {
	System.out.println("No thread="+no_thread);
	for (int i=0;i<getMultiTestsNumber();i++)
	    subMultipleTests();
    }
    
    public abstract boolean isTestEnabled(int testNumber);
    
    public static volatile boolean no_thread=true;
    public final ThreadLocalRandom random=ThreadLocalRandom.current();
    @Test(invocationCount=0) public void subMultipleTests() throws DatabaseException
    {
	Table1 table1;
	Table2 table2;
	Table3 table3;
	Table4 table4;
	Table5 table5;
	Table6 table6;
	if (!isMultiConcurrentDatabase() || random.nextInt(2)==0)
	{
	    table1=TestDatabase.table1;
	    table2=TestDatabase.table2;
	    table3=TestDatabase.table3;
	    table4=TestDatabase.table4;
	    table5=TestDatabase.table5;
	    table6=TestDatabase.table6;
	}
	else
	{
	    table1=TestDatabase.table1b;
	    table2=TestDatabase.table2b;
	    table3=TestDatabase.table3b;
	    table4=TestDatabase.table4b;
	    table5=TestDatabase.table5b;
	    table6=TestDatabase.table6b;
	}
	int r=random.nextInt(33);
	
	int number=number_thread_test.incrementAndGet();

	if (isTestEnabled(r))
	{
	System.out.println("Test "+number+" number "+r+" in progress.");
	switch(r)
	{
	    case 0:
	    {
		table1.getRecords();
		table2.getRecords();
		table3.getRecords();
		table4.getRecords();
		table5.getRecords();
		table6.getRecords();
	    }
	    break;
	    case 1:
	    {
		HashMap<String, Object> map=new HashMap<String, Object>();
		map.put("pk1", new Integer(random.nextInt()));
		map.put("int_value", new Integer(3));
		map.put("byte_value", new Byte((byte)3));
		map.put("char_value", new Character('x'));
		map.put("boolean_value", new Boolean(true));
		map.put("short_value", new Short((short)random.nextInt()));
		map.put("long_value", new Long(3));
		map.put("float_value", new Float(3.3f));
		map.put("double_value", new Double(3.3));
		map.put("string_value", new String("test string"));
		map.put("IntegerNumber_value", new Integer(3));
		map.put("ByteNumber_value", new Byte((byte)3));
		map.put("CharacterNumber_value", new Character('x'));
		map.put("BooleanNumber_value", new Boolean(true));
		map.put("ShortNumber_value", new Short((short)3));
		map.put("LongNumber_value", new Long(random.nextLong()));
		map.put("FloatNumber_value", new Float(3.3f));
		map.put("DoubleNumber_value", new Double(3.3));
		map.put("BigInteger_value", new BigInteger("6"));
		map.put("BigDecimal_value", new BigDecimal("1.10"));
		map.put("DateValue", Calendar.getInstance().getTime());
		map.put("CalendarValue", Calendar.getInstance());
		map.put("secretKey", secretKey);
		map.put("typeSecretKey", typeSecretKey);
		map.put("subField", subField);
		map.put("subSubField", subSubField);
		byte[] tab=new byte[3];
		tab[0]=0;
		tab[1]=1;
		tab[2]=2;
		map.put("byte_array_value", tab);
		try
		{
		    Table1.Record r1=table1.addRecord(map);
		    Table3.Record r3=table3.addRecord(map);
		    if (no_thread)
		    {
			Assert.assertTrue(table1.contains(r1));
			Assert.assertTrue(table3.contains(r3));
		    }
		
		    Assert.assertTrue(r1.pk1==((Integer)map.get("pk1")).intValue());
		    Assert.assertTrue(r1.int_value==((Integer)map.get("int_value")).intValue());
		    Assert.assertTrue(r1.byte_value==((Byte)map.get("byte_value")).byteValue());
		    Assert.assertTrue(r1.char_value==((Character)map.get("char_value")).charValue());
		    Assert.assertTrue(r1.boolean_value==((Boolean)map.get("boolean_value")).booleanValue());
		    Assert.assertTrue(r1.short_value==((Short)map.get("short_value")).shortValue());
		    Assert.assertTrue(r1.long_value==((Long)map.get("long_value")).longValue());
		    Assert.assertTrue(r1.float_value==((Float)map.get("float_value")).floatValue());
		    Assert.assertTrue(r1.double_value==((Double)map.get("double_value")).doubleValue());
		    Assert.assertTrue(r1.string_value.equals(map.get("string_value")));
		    Assert.assertTrue(r1.IntegerNumber_value.equals(map.get("IntegerNumber_value")));
		    Assert.assertTrue(r1.ByteNumber_value.equals(map.get("ByteNumber_value")));
		    Assert.assertTrue(r1.CharacterNumber_value.equals(map.get("CharacterNumber_value")));
		    Assert.assertTrue(r1.BooleanNumber_value.equals(map.get("BooleanNumber_value")));
		    Assert.assertTrue(r1.ShortNumber_value.equals(map.get("ShortNumber_value")));
		    Assert.assertTrue(r1.LongNumber_value.equals(map.get("LongNumber_value")));
		    Assert.assertTrue(r1.FloatNumber_value.equals(map.get("FloatNumber_value")));
		    Assert.assertTrue(r1.DoubleNumber_value.equals(map.get("DoubleNumber_value")));
		    Assert.assertTrue(r1.BigInteger_value.equals(map.get("BigInteger_value")));
		    Assert.assertTrue(r1.BigDecimal_value.equals(map.get("BigDecimal_value")));
		    Assert.assertTrue(r1.secretKey.equals(map.get("secretKey")));
		    Assert.assertTrue(r1.typeSecretKey.equals(map.get("typeSecretKey")));
		    assertEquals((SubField)map.get("subField"), r1.subField);
		    assertEquals((SubSubField)map.get("subSubField"), r1.subSubField);

		    for (int i=0;i<3;i++)
			Assert.assertTrue(r1.byte_array_value[i]==((byte[])map.get("byte_array_value"))[i]);

		    Assert.assertTrue(r3.pk1==((Integer)map.get("pk1")).intValue());
		    Assert.assertTrue(r3.int_value==((Integer)map.get("int_value")).intValue());
		    Assert.assertTrue(r3.byte_value==((Byte)map.get("byte_value")).byteValue());
		    Assert.assertTrue(r3.char_value==((Character)map.get("char_value")).charValue());
		    Assert.assertTrue(r3.boolean_value==((Boolean)map.get("boolean_value")).booleanValue());
		    Assert.assertTrue(r3.short_value==((Short)map.get("short_value")).shortValue());
		    Assert.assertTrue(r3.long_value==((Long)map.get("long_value")).longValue());
		    Assert.assertTrue(r3.float_value==((Float)map.get("float_value")).floatValue());
		    Assert.assertTrue(r3.double_value==((Double)map.get("double_value")).doubleValue());
		    Assert.assertTrue(r3.string_value.equals(map.get("string_value")));
		    Assert.assertTrue(r3.IntegerNumber_value.equals(map.get("IntegerNumber_value")));
		    Assert.assertTrue(r3.ByteNumber_value.equals(map.get("ByteNumber_value")));
		    Assert.assertTrue(r3.CharacterNumber_value.equals(map.get("CharacterNumber_value")));
		    Assert.assertTrue(r3.BooleanNumber_value.equals(map.get("BooleanNumber_value")));
		    Assert.assertTrue(r3.ShortNumber_value.equals(map.get("ShortNumber_value")));
		    Assert.assertTrue(r3.LongNumber_value.equals(map.get("LongNumber_value")));
		    Assert.assertTrue(r3.FloatNumber_value.equals(map.get("FloatNumber_value")));
		    Assert.assertTrue(r3.DoubleNumber_value.equals(map.get("DoubleNumber_value")));
		    Assert.assertTrue(r3.BigInteger_value.equals(map.get("BigInteger_value")));
		    Assert.assertTrue(r3.BigDecimal_value.equals(map.get("BigDecimal_value")));
		    Assert.assertTrue(r3.secretKey.equals(map.get("secretKey")));
		    Assert.assertTrue(r3.typeSecretKey.equals(map.get("typeSecretKey")));
		    assertEquals((SubField)map.get("subField"), r3.subField);
		    assertEquals((SubSubField)map.get("subSubField"), r3.subSubField);
		    
		    for (int i=0;i<3;i++)
			Assert.assertTrue(r3.byte_array_value[i]==((byte[])map.get("byte_array_value"))[i]);
		}
		catch(ConstraintsNotRespectedDatabaseException e)
		{
		    if (no_thread)
		    {
			throw e;
		    }
		}
	    }
	    break;
	    case 2:
	    {
		int next;
		synchronized(next_unique)
		{
		    next=next_unique.intValue();
		    next_unique.set(next+1);
		}
		ArrayList<Table1.Record> records=table1.getRecords();
		if (records.size()>0)
		{
		    HashMap<String, Object> map=new HashMap<String, Object>();
		    map.put("fr1_pk1", records.get(random.nextInt(records.size())));
		    map.put("int_value", new Integer(next));
		    try
		    {
			ArrayList<Table2.Record> records2=table2.getRecords();
			boolean found=false;
			for (Table2.Record r2 : records2)
			{
			    if (table1.equals(r2.fr1_pk1, (Table1.Record)map.get("fr1_pk1")))
			    {
				found=true;
				break;
			    }
			}
			if (!found)
			{
			    Table2.Record r2=table2.addRecord(map);
			    if (no_thread)
			    {
				Assert.assertTrue(table1.equals(r2.fr1_pk1, (Table1.Record)map.get("fr1_pk1")));
				Assert.assertTrue(r2.int_value==((Integer)map.get("int_value")).intValue());
			    }
			}
		    }
		    catch(RecordNotFoundDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		    catch(ConstraintsNotRespectedDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		   
		}
	    }
	    break;
	    case 3:
	    {
		int next;
		synchronized(next_unique)
		{
		    next=next_unique.intValue();
		    next_unique.set(next+1);
		}
		ArrayList<Table3.Record> records=table3.getRecords();
		if (records.size()>0)
		{
		    Table3.Record rec1=records.get(random.nextInt(records.size()));
		    HashMap<String, Object> map=new HashMap<String, Object>();
		    map.put("fr1_pk1", rec1);
		    map.put("int_value", new Integer(next));
		    try
		    {
			ArrayList<Table4.Record> records4=table4.getRecords();
			boolean found=false;
			for (Table4.Record r4 : records4)
			{
			    if (table3.equals(r4.fr1_pk1, (Table3.Record)map.get("fr1_pk1")))
			    {
				found=true;
				break;
			    }
			}
			if (!found)
			{
			    Table4.Record r4=table4.addRecord(map);
			    if (no_thread)
			    {
				Assert.assertTrue(table3.equals(r4.fr1_pk1, (Table3.Record)map.get("fr1_pk1")));
				Assert.assertTrue(r4.int_value==((Integer)map.get("int_value")).intValue());
			    }
			}			    
		    }
		    catch(RecordNotFoundDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		    catch(ConstraintsNotRespectedDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		}
	    }
	    break;
	    case 4:
	    {
		int next;
		synchronized(next_unique)
		{
		    next=next_unique.intValue();
		    next_unique.set(next+1);
		}
		ArrayList<Table3.Record> records=table3.getRecords();
		if (records.size()>0)
		{
		    Table3.Record rec1=records.get(random.nextInt(records.size()));
		    HashMap<String, Object> map=new HashMap<String, Object>();
		    map.put("fr1_pk1", rec1);
		    map.put("int_value", new Integer(next));
		    try
		    {
			ArrayList<Table5.Record> records5=table5.getRecords();
			boolean found=false;
			for (Table5.Record r5 : records5)
			{
			    if (table3.equals(r5.fr1_pk1, (Table3.Record)map.get("fr1_pk1")))
			    {
				found=true;
				break;
			    }
			}
			if (!found)
			{
			    Table5.Record r5=table5.addRecord(map);
			    if (no_thread)
			    {
				Assert.assertTrue(table3.equals(r5.fr1_pk1, (Table3.Record)map.get("fr1_pk1")));
				Assert.assertTrue(r5.int_value==((Integer)map.get("int_value")).intValue());
			    }
			}
		    }
		    catch(RecordNotFoundDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		    catch(ConstraintsNotRespectedDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		    
		}
	    }
	    break;
	    case 5:
	    {
		final ArrayList<Table1.Record> removed_records=new ArrayList<Table1.Record>();
		table1.removeRecords(new Filter<Table1.Record>() {

		    @Override
		    public boolean nextRecord(Record _record)
		    {
			boolean toremove=random.nextInt(2)==0;
			if (toremove)
			    removed_records.add(_record);
			return toremove;
		    }
		});
		if (no_thread)
		{
		    for (Table1.Record r1 : removed_records)
		    {
			Assert.assertFalse(table1.contains(r1));
			for (Table2.Record r2 : table2.getRecords())
			{
			    Assert.assertFalse(table1.equals(r2.fr1_pk1, r1));
			}
		    }
		}
		else
		    table2.getRecords();
		table6.getRecords();
	    }
	    break;
	    case 6:
	    {
		final ArrayList<Table3.Record> removed_records=new ArrayList<Table3.Record>();
		table3.removeRecords(new Filter<Table3.Record>() {

		    @Override
		    public boolean nextRecord(Table3.Record _record)
		    {
			boolean toremove=random.nextInt(2)==0;
			if (toremove)
			    removed_records.add(_record);
			return toremove;
		    }
		});
		if (no_thread)
		{
		    for (Table3.Record r3 : removed_records)
		    {
			Assert.assertFalse(table3.contains(r3));
			for (Table4.Record r4 : table4.getRecords())
			{
			    Assert.assertFalse(table3.equals(r4.fr1_pk1, r3));
			}
			for (Table5.Record r5 : table5.getRecords())
			{
			    Assert.assertFalse(table3.equals(r5.fr1_pk1, r3));
			}
		    }
		}
		else
		{
		    table4.getRecords();
		    table5.getRecords();
		}
		table6.getRecords();
	    }
	    break;
	    case 7:
	    {
		
		final ArrayList<Table4.Record> removed_records=new ArrayList<Table4.Record>();
		table4.removeRecords(new Filter<Table4.Record>() {

		    @Override
		    public boolean nextRecord(Table4.Record _record)
		    {
			boolean toremove=random.nextInt(2)==0;

			if (toremove)
			    removed_records.add(_record);
			return toremove;
		    }
		});
		if (no_thread)
		{
		    for (Table4.Record r4 : removed_records)
		    {
			Assert.assertFalse(table4.contains(r4));
		    }
		}
	    }
	    break;
	    case 8:
	    {
		final ArrayList<Table2.Record> removed_records=new ArrayList<Table2.Record>();
		table2.removeRecords(new Filter<Table2.Record>() {

		    @Override
		    public boolean nextRecord(Table2.Record _record)
		    {
			boolean toremove=random.nextInt(2)==0;

			if (toremove)
			    removed_records.add(_record);
			return toremove;
		    }
		});
		if (no_thread)
		{
		    for (Table2.Record r2 : removed_records)
		    {
			Assert.assertFalse(table2.contains(r2));
			for (Table6.Record r6 : table6.getRecords())
			{
			    Assert.assertFalse(table2.equals(r6.fk1_pk1, r2));
			}
		    }
		}
		table6.getRecords();
	    }
	    break;
	    case 9:
	    {
		final ArrayList<Table5.Record> removed_records=new ArrayList<Table5.Record>();
		table5.removeRecords(new Filter<Table5.Record>() {

		    @Override
		    public boolean nextRecord(Table5.Record _record)
		    {
			boolean toremove=random.nextInt(2)==0;

			if (toremove)
			    removed_records.add(_record);
			return toremove;
		    }
		});
		if (no_thread)
		{
		    for (Table5.Record r5 : removed_records)
		    {
			Assert.assertFalse(table5.contains(r5));
			for (Table6.Record r6 : table6.getRecords())
			{
			    Assert.assertFalse(table5.equals(r6.fk2, r5));
			}
		    }
		}
		table6.getRecords();
	    }
	    break;
	    case 10:
	    {
		final ArrayList<Table1.Record> removed_records=new ArrayList<Table1.Record>();
		table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

		    @Override
		    public boolean nextRecord(Record _record)
		    {
			boolean toremove=random.nextInt(2)==0;

			if (toremove)
			    removed_records.add(_record);
			return toremove;
		    }
		});
		if(no_thread)
		{
		    for (Table1.Record r1 : removed_records)
		    {
			Assert.assertFalse(table1.contains(r1));
			for (Table2.Record r2 : table2.getRecords())
			{
			    Assert.assertFalse(table1.equals(r2.fr1_pk1, r1));
			}
		    }
		}
		else
		{
		    table2.getRecords();
		}
		table6.getRecords();
	    }
	    break;
	    case 11:
	    {
		table6.getRecords();
		final ArrayList<Table3.Record> removed_records=new ArrayList<Table3.Record>();
		table3.removeRecordsWithCascade(new Filter<Table3.Record>() {

		    @Override
		    public boolean nextRecord(Table3.Record _record)
		    {
			boolean toremove=random.nextInt(2)==0;

			if (toremove)
			    removed_records.add(_record);
			return toremove;
		    }
		});
		if (no_thread)
		{
		    for (Table3.Record r3 : removed_records)
		    {
			Assert.assertFalse(table3.contains(r3));
			for (Table4.Record r4 : table4.getRecords())
			{
			    Assert.assertFalse(table3.equals(r4.fr1_pk1, r3));
			}
			for (Table5.Record r5 : table5.getRecords())
			{
			    Assert.assertFalse(table3.equals(r5.fr1_pk1, r3));
			
			}
		    }
		}
		else
		{
		    table4.getRecords();
		    table5.getRecords();
		}
		table6.getRecords();
	    }
	    break;
	    case 12:
	    {
		ArrayList<Table1.Record> records=table1.getRecords();
		if (records.size()>0)
		{
		    Table1.Record rec1=records.get(random.nextInt(records.size()));
		    HashMap<String, Object> map=new HashMap<String, Object>();
		    map.put("pk1", new Integer(random.nextInt()));
		    map.put("byte_value", new Byte((byte)9));
		    map.put("char_value", new Character('s'));
		    map.put("DoubleNumber_value", new Double(7.7));
		    try
		    {
			table1.updateRecord(rec1, map);
			Assert.assertTrue(rec1.pk1==((Integer)map.get("pk1")).intValue());
			Assert.assertTrue(rec1.byte_value==((Byte)map.get("byte_value")).byteValue());
			Assert.assertTrue(rec1.char_value==((Character)map.get("char_value")).charValue());
			Assert.assertTrue(rec1.DoubleNumber_value.equals(map.get("DoubleNumber_value")));
		    }
		    catch(RecordNotFoundDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		    catch(ConstraintsNotRespectedDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		    table2.getRecords();
		    table6.getRecords();
		}
	    }
	    break;
	    case 13:
	    {
		ArrayList<Table3.Record> records=table3.getRecords();
		if (records.size()>0)
		{
		    Table3.Record rec1=records.get(random.nextInt(records.size()));
		    HashMap<String, Object> map=new HashMap<String, Object>();
		    map.put("pk1", new Integer(random.nextInt()));
		    map.put("byte_value", new Byte((byte)9));
		    map.put("char_value", new Character('s'));
		    map.put("DoubleNumber_value", new Double(7.7));
		    try
		    {
			table3.updateRecord(rec1, map);
			Assert.assertTrue(rec1.pk1==((Integer)map.get("pk1")).intValue());
			Assert.assertTrue(rec1.byte_value==((Byte)map.get("byte_value")).byteValue());
			Assert.assertTrue(rec1.char_value==((Character)map.get("char_value")).charValue());
			Assert.assertTrue(rec1.DoubleNumber_value.equals(map.get("DoubleNumber_value")));
		    }
		    catch(RecordNotFoundDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		    catch(ConstraintsNotRespectedDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		    table4.getRecords();
		    table5.getRecords();
		    table6.getRecords();
		}
	    }
	    break;
	    case 14:
	    {
		long l=table1.getRecordsNumber();
		Assert.assertTrue(table1.removeRecordsWithCascade(new Filter<Table1.Record>() {

		    @Override
		    public boolean nextRecord(Record _record)
		    {
			return false;
		    }
		})==0);
		if (no_thread)
		    Assert.assertTrue(l==table1.getRecordsNumber());
		table2.getRecords();
		table6.getRecords();
	    }
	    break;
	    case 15:
	    {
		long l=table3.getRecordsNumber();
		Assert.assertTrue(table3.removeRecordsWithCascade(new Filter<Table3.Record>() {

		    @Override
		    public boolean nextRecord(Table3.Record _record)
		    {
			return false;
		    }
		})==0);
		if (no_thread)
		    Assert.assertTrue(l==table3.getRecordsNumber());
		table4.getRecords();
		table5.getRecords();
		table6.getRecords();
	    }
	    break;
	    case 16:
	    {
		ArrayList<Table1.Record> records=table1.getRecords();
		if (records.size()>0)
		{
		    Table1.Record rec=records.get(random.nextInt(records.size()));
		    try
		    {
			table1.removeRecordWithCascade(rec);
			if (no_thread)
			{
			    Assert.assertFalse(table1.contains(rec));
			    for (Table2.Record r2 : table2.getRecords())
			    {
				Assert.assertFalse(table1.equals(r2.fr1_pk1, rec));
			    }
			}
			else
			    table2.getRecords();
			table6.getRecords();
		    }
		    catch(RecordNotFoundDatabaseException e)
		    {
			if (no_thread)
			{
			    throw e;
			}
		    }
		}
	    }
	    break;
	    case 17:
	    {
		ArrayList<Table3.Record> records=table3.getRecords();
		if (records.size()>0)
		{
		    Table3.Record rec=records.get(random.nextInt(records.size()));
		    try
		    {
			table3.removeRecordWithCascade(rec);
			if (no_thread)
			{
			    Assert.assertFalse(table3.contains(rec));
			    for (Table4.Record r4 : table4.getRecords())
			    {
				Assert.assertFalse(table3.equals(r4.fr1_pk1, rec));
			    }
			    for (Table5.Record r5 : table5.getRecords())
			    {
				Assert.assertFalse(table3.equals(r5.fr1_pk1, rec));
			    }
			}
			else
			{
			    table4.getRecords();
			    table5.getRecords();
			}
			table6.getRecords();
		    }
		    catch(RecordNotFoundDatabaseException e)
		    {
			if (no_thread)
			{
			    throw e;
			}
		    }
		}
	    }
	    break;
	    case 18:
	    {
		ArrayList<Table4.Record> records=table4.getRecords();
		if (records.size()>0)
		{
		    Table4.Record rec=records.get(random.nextInt(records.size()));
		    try
		    {
			table4.removeRecordWithCascade(rec);
			if (no_thread)
			    Assert.assertFalse(table4.contains(rec));
		    }
		    catch(RecordNotFoundDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		}
	    }
	    break;
	    case 19:
	    {
		ArrayList<Table4.Record> records=table4.getRecords();
		if (records.size()>0)
		{
		    Table4.Record rec=records.get(random.nextInt(records.size()));
		    try
		    {
			table4.removeRecord(rec);
			if (no_thread)
			    Assert.assertFalse(table4.contains(rec));
		    }
		    catch(RecordNotFoundDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		}
	    }
	    break;
	    case 20:
	    {
		ArrayList<Table2.Record> records=table2.getRecords();
		if (records.size()>0)
		{
		    Table2.Record rec=records.get(random.nextInt(records.size()));
		    try
		    {
			table2.removeRecordWithCascade(rec);
			if (no_thread)
			{
			    Assert.assertFalse(table2.contains(rec));
			    for (Table6.Record r6 : table6.getRecords())
			    {
				Assert.assertFalse(table2.equals(r6.fk1_pk1, rec));
			    }
			}
			else
			    table6.getRecords();
		    }
		    catch(RecordNotFoundDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		}
	    }
	    break;
	    case 21:
	    {
		    table1.updateRecords(new AlterRecordFilter<Table1.Record>() {
		        
		        @Override
		        public void nextRecord(Record _record)
		        {
		            HashMap<String, Object> m=new HashMap<String, Object>();
			    m.put("int_value", new Integer(18));
			    this.update(m);
		        }
		    });
		    if (no_thread)
		    {
			ArrayList<Table1.Record> records1=table1.getRecords();
			for (Table1.Record r1 : records1)
			    Assert.assertTrue(r1.int_value==18);
		    }
		    
		    
		    table3.updateRecords(new AlterRecordFilter<Table3.Record>() {
		        
		        @Override
		        public void nextRecord(Table3.Record _record)
		        {
		            HashMap<String, Object> m=new HashMap<String, Object>();
			    m.put("int_value", new Integer(18));
			    this.update(m);
		        }
		    });
		    if (no_thread)
		    {
			ArrayList<Table3.Record> records3=table3.getRecords();
			for (Table3.Record r3 : records3)
			    Assert.assertTrue(r3.int_value==18);
		    }
		
	    }
	    break;
	    case 22:
	    {
		{
		    ArrayList<Table1.Record> recs=table1.getRecords(new Filter<Table1.Record>() {
			private int val=0;
			@Override
			public boolean nextRecord(Table1.Record _record) 
			{
			    if ((val++)%3==0)
				return true;
			    return false;
			}
		    });
		    try
		    {
			if (recs.size()>0)
			{
			    table1.removeRecordsWithCascade(recs);
			    if (no_thread)
			    {
				for (Table1.Record rec : recs)
				{
				    Assert.assertFalse(table1.contains(rec));
				    for (Table2.Record r2 : table2.getRecords())
				    {
					Assert.assertFalse(table1.equals(r2.fr1_pk1, rec));
				    }
				}
			    }
			    else
				table2.getRecords();
			    table6.getRecords();
			}
		    }
		    catch(RecordNotFoundDatabaseException e)
		    {
			if (no_thread)
			{
			    throw e;
			}
		    }
		}
	    }
	    break;
	    case 23:
	    {
		{
		    ArrayList<Table3.Record> recs=table3.getRecords(new Filter<Table3.Record>() {
			private int val=0;
			@Override
			public boolean nextRecord(Table3.Record _record) 
			{
			    if ((val++)%3==0)
				return true;
			    return false;
			}
		    });
		    try
		    {
			if (recs.size()>0)
			{
			    table3.removeRecordsWithCascade(recs);
			    if (no_thread)
			    {
				for (Table3.Record rec : recs)
				{
				    Assert.assertFalse(table3.contains(rec));
				    for (Table4.Record r4 : table4.getRecords())
				    {	
					Assert.assertFalse(table3.equals(r4.fr1_pk1, rec));
				    }
				    for (Table5.Record r5 : table5.getRecords())
				    {
					Assert.assertFalse(table3.equals(r5.fr1_pk1, rec));
				    }
				}
			    }
			    else
			    {
				table4.getRecords();
				table5.getRecords();
			    }
			    table6.getRecords();
			}
		    }
		    catch(RecordNotFoundDatabaseException e)
		    {
			if (no_thread)
			{
			    throw e;
			}
		    }
		}
	    }
	    break;
	    case 24:
	    {
		{
		    ArrayList<Table1.Record> recs=table1.getRecords(new Filter<Table1.Record>() {
			private int val=0;
			@Override
			public boolean nextRecord(Table1.Record _record) 
			{
			    if ((val++)%3==0)
				return true;
			    return false;
			}
		    });
		    for (Iterator<Table1.Record> it=recs.iterator();it.hasNext();)
		    {
			if (table1.isRecordPointedByForeignKeys(it.next()))
			    it.remove();
		    }
		    try
		    {
			if (recs.size()>0)
			{
			    table1.removeRecords(recs);
			    if (no_thread)
			    {
				for (Table1.Record rec : recs)
				{
				    Assert.assertFalse(table1.contains(rec));
				    for (Table2.Record r2 : table2.getRecords())
				    {
					Assert.assertFalse(table1.equals(r2.fr1_pk1, rec));
				    }
				}
			    }
			    else
				table2.getRecords();
			    table6.getRecords();
			}
		    }
		    catch(RecordNotFoundDatabaseException e)
		    {
			if (no_thread)
			{
			    throw e;
			}
		    }
		    catch(ConstraintsNotRespectedDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		}
	    }
	    break;
	    case 25:
	    {
		{
		    ArrayList<Table3.Record> recs=table3.getRecords(new Filter<Table3.Record>() {
			private int val=0;
			@Override
			public boolean nextRecord(Table3.Record _record) 
			{
			    if ((val++)%3==0)
				return true;
			    return false;
			}
		    });
		    for (Iterator<Table3.Record> it=recs.iterator();it.hasNext();)
		    {
			if (table3.isRecordPointedByForeignKeys(it.next()))
			    it.remove();
		    }
		    
		    try
		    {
			if (recs.size()>0)
			{
			    table3.removeRecords(recs);
			    if (no_thread)
			    {
				for (Table3.Record rec : recs)
				{
				    Assert.assertFalse(table3.contains(rec));
				    for (Table4.Record r4 : table4.getRecords())
				    {	
					Assert.assertFalse(table3.equals(r4.fr1_pk1, rec));
				    }
				    for (Table5.Record r5 : table5.getRecords())
				    {
					Assert.assertFalse(table3.equals(r5.fr1_pk1, rec));
				    }
				}
			    }
			    else
			    {
				table4.getRecords();
				table5.getRecords();
			    }
			    table6.getRecords();
			}
		    }
		    catch(RecordNotFoundDatabaseException e)
		    {
			if (no_thread)
			{
			    throw e;
			}
		    }
		    catch(ConstraintsNotRespectedDatabaseException e)
		    {
			if (no_thread)
			    throw e;
		    }
		}
	    }
	    break;
	    case 26:
	    {
		Assert.assertFalse(table1.hasRecords(new Filter<Table1.Record>() {

		    @Override
		    public boolean nextRecord(Table1.Record _record) 
		    {
			return false;
		    }
		}));
		if (no_thread)
		{
		    if (table1.getRecordsNumber()>0)
		    {
			Assert.assertTrue(table1.hasRecords(new Filter<Table1.Record>() {

			    @Override
			    public boolean nextRecord(Table1.Record _record) 
			    {
				return true;
			    }
			}));
		    }
		}
		Assert.assertFalse(table2.hasRecords(new Filter<Table2.Record>() {

		    @Override
		    public boolean nextRecord(Table2.Record _record) 
		    {
			return false;
		    }
		}));
		if (no_thread)
		{
		    if (table2.getRecordsNumber()>0)
		    {
		
			Assert.assertTrue(table2.hasRecords(new Filter<Table2.Record>() {

			    @Override
			    public boolean nextRecord(Table2.Record _record) 
			    {
				return true;
			    }
			}));
		    }
		}
		Assert.assertFalse(table3.hasRecords(new Filter<Table3.Record>() {

		    @Override
		    public boolean nextRecord(Table3.Record _record) 
		    {
			return false;
		    }
		}));
		if (no_thread)
		{
		    if (table3.getRecordsNumber()>0)
		    {

			Assert.assertTrue(table3.hasRecords(new Filter<Table3.Record>() {

			    @Override
			    public boolean nextRecord(Table3.Record _record) 
			    {
				return true;
			    }
			}));
		    }
		}
		Assert.assertFalse(table4.hasRecords(new Filter<Table4.Record>() {

		    @Override
		    public boolean nextRecord(Table4.Record _record) 
		    {
			return false;
		    }
		}));
		if (no_thread)
		{
		    if (table4.getRecordsNumber()>0)
		    {

			Assert.assertTrue(table4.hasRecords(new Filter<Table4.Record>() {

			    @Override
			    public boolean nextRecord(Table4.Record _record) 
			    {
				return true;
			    }
			}));
		    }
		}
		Assert.assertFalse(table5.hasRecords(new Filter<Table5.Record>() {

		    @Override
		    public boolean nextRecord(Table5.Record _record) 
		    {
			return false;
		    }
		}));
		if (no_thread)
		{
		    if (table6.getRecordsNumber()>0)
		    {

			Assert.assertTrue(table5.hasRecords(new Filter<Table5.Record>() {

			    @Override
			    public boolean nextRecord(Table5.Record _record) 
			    {
				return true;
			    }
			}));
		    }
		}
		Assert.assertFalse(table6.hasRecords(new Filter<Table6.Record>() {

		    @Override
		    public boolean nextRecord(Table6.Record _record) 
		    {
			return false;
		    }
		}));
		if (no_thread)
		{
		    if (table6.getRecordsNumber()>0)
		    {

			Assert.assertTrue(table6.hasRecords(new Filter<Table6.Record>() {

			    @Override
			    public boolean nextRecord(Table6.Record _record) 
			    {
				return true;
			    }
			}));
		    }
		}
		
	    }
		break;
	    case 27:
	    {
		ArrayList<Table1.Record> r1=table1.getOrderedRecords(new Filter<Table1.Record>() {

		    @Override
		    public boolean nextRecord(Table1.Record _record)
		    {
			return random.nextInt(2)==0;
		    }
		}, true, "short_value", "LongNumber_value");
		testOrderedTable1(r1);
		r1=table1.getOrderedRecords(new Filter<Table1.Record>() {

		    @Override
		    public boolean nextRecord(Table1.Record _record)
		    {
			return random.nextInt(2)==0;
		    }
		}, false, "short_value", "LongNumber_value");
		testInverseOrderedTable1(r1);
		
		r1=table1.getOrderedRecords(true, "short_value", "LongNumber_value");
		testOrderedTable1(r1);
		if (no_thread)
		{
		    ArrayList<Table1.Record> r1b=table1.getOrderedRecords(table1.getRecords(), true, "short_value", "LongNumber_value");
		    Assert.assertTrue(r1.size()==r1b.size());
		    
		    for (int i=0;i<r1.size();i++)
		    {
			Assert.assertEquals(r1.get(i).short_value, r1b.get(i).short_value);
			Assert.assertEquals(r1.get(i).LongNumber_value, r1b.get(i).LongNumber_value);
			//Assert.assertTrue(table1.equalsAllFields(r1.get(i), r1b.get(i)), "r1_short_value="+r1.get(i).short_value+"; r1LongNumber_value="+r1.get(i).LongNumber_value+"-----------"+"r1b_short_value="+r1b.get(i).short_value+"; r1bLongNumber_value="+r1b.get(i).LongNumber_value);
		    }
		}
		r1=table1.getOrderedRecords(false, "short_value", "LongNumber_value");
		testInverseOrderedTable1(r1);
		if (no_thread)
		{
		    ArrayList<Table1.Record> r1b=table1.getOrderedRecords(table1.getRecords(), false, "short_value", "LongNumber_value");
		    Assert.assertTrue(r1.size()==r1b.size());
		    
		    for (int i=0;i<r1.size();i++)
		    {
			Assert.assertEquals(r1.get(i).short_value, r1b.get(i).short_value);
			Assert.assertEquals(r1.get(i).LongNumber_value, r1b.get(i).LongNumber_value);
			//Assert.assertTrue(table1.equalsAllFields(r1.get(i), r1b.get(i)));
		    }
		}
	    }    
	    {
		ArrayList<Table3.Record> r3=table3.getOrderedRecords(new Filter<Table3.Record>() {

		    @Override
		    public boolean nextRecord(Table3.Record _record)
		    {
			return random.nextInt(2)==0;
		    }
		}, true, "short_value", "LongNumber_value");
		testOrderedTable3(r3);
		GroupedResults<Table3.Record> grouped_results=table3.getGroupedResults(r3, "FloatNumber_value");
		for (GroupedResults<Table3.Record>.Group l : grouped_results.getGroupedResults())
		{
		    testOrderedTable3(l.getResults());		    
		}
		    
		r3=table3.getOrderedRecords(new Filter<Table3.Record>() {

		    @Override
		    public boolean nextRecord(Table3.Record _record)
		    {
			return random.nextInt(2)==0;
		    }
		}, false, "short_value", "LongNumber_value");
		testInverseOrderedTable3(r3);
		grouped_results=table3.getGroupedResults(r3, "FloatNumber_value", "BigInteger_value");
		for (GroupedResults<Table3.Record>.Group l : grouped_results.getGroupedResults())
		{
		    testInverseOrderedTable3(l.getResults());		    
		}
		table3.getOrderedRecords(true, "DateValue", "LongNumber_value");
		table3.getOrderedRecords(true, "CalendarValue", "LongNumber_value");
		
		r3=table3.getOrderedRecords(true, "short_value", "LongNumber_value");
		testOrderedTable3(r3);
		if (no_thread)
		{
		    ArrayList<Table3.Record> r3b=table3.getOrderedRecords(table3.getRecords(), true, "short_value", "LongNumber_value");
		    Assert.assertTrue(r3.size()==r3b.size());
		    
		    for (int i=0;i<r3.size();i++)
		    {
			Assert.assertTrue(table3.equalsAllFields(r3.get(i), r3b.get(i)));
		    }
		}
		r3=table3.getOrderedRecords(false, "short_value", "LongNumber_value");
		testInverseOrderedTable3(r3);
		if (no_thread)
		{
		    ArrayList<Table3.Record> r3b=table3.getOrderedRecords(table3.getRecords(), false, "short_value", "LongNumber_value");
		    Assert.assertTrue(r3.size()==r3b.size());
		    
		    for (int i=0;i<r3.size();i++)
		    {
			Assert.assertTrue(table3.equalsAllFields(r3.get(i), r3b.get(i)));
		    }
		}
	    }    
		break;
	    case 28:
	    {
		    table1.updateRecords(new AlterRecordFilter<Table1.Record>() {
		        
		        @Override
		        public void nextRecord(Record _record)
		        {
		            switch(random.nextInt(4))
		            {
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
		        public void nextRecord(Table3.Record _record)
		        {
		            switch(random.nextInt(4))
		            {
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
	    case 29:
	    {
		/*try(TableIterator<Table1.Record> it=table1.getIterator())
		{
		    while (it.hasNext())
			it.next();
		    try(TableIterator<Table3.Record> it2=table3.getIterator())
		    {
			while (it2.hasNext())
			    it2.next();
		    }
		}
		try(TableIterator<Table2.Record> it=table2.getIterator())
		{
		    while (it.hasNext())
			it.next();
		}
		try(TableIterator<Table4.Record> it=table4.getIterator())
		{
		    while (it.hasNext())
			it.next();
		}
		try(TableIterator<Table5.Record> it=table5.getIterator())
		{
		    while (it.hasNext())
			it.next();
		}
		try(TableIterator<Table6.Record> it=table6.getIterator())
		{
		    while (it.hasNext())
			it.next();
		}*/
	    }
	    break;
	    case 30:
	    {
		    byte[] tab=new byte[3];
		    tab[0]=0;
		    tab[1]=1;
		    tab[2]=2;
		    Object []parameters={"pk1", new Integer(random.nextInt()), 
			    "int_value", new Integer(3), 
			    "byte_value", new Byte((byte)3),
			    "char_value", new Character('x'),
			    "boolean_value", new Boolean(true),
			    "short_value", new Short((short)3),
			    "long_value", new Long(3),
			    "float_value", new Float(3.3f),
			    "double_value", new Double(3.3),
			    "string_value", new String("test string"),
			    "IntegerNumber_value", new Integer(3),
			    "ByteNumber_value", new Byte((byte)3),
			    "CharacterNumber_value", new Character('x'),
			    "BooleanNumber_value", new Boolean(true),
			    "ShortNumber_value", new Short((short)3),
			    "LongNumber_value", new Long((long)3),
			    "FloatNumber_value", new Float(3.3f),
			    "DoubleNumber_value", new Double(3.3),
			    "BigInteger_value", new BigInteger("5"),
			    "BigDecimal_value", new BigDecimal("8.8"),
			    "DateValue", date.clone(),
			    "CalendarValue", calendar.clone(),
			    "secretKey", secretKey,
			    "typeSecretKey", typeSecretKey,
			    "byte_array_value", tab.clone(),
			    "subField", subField,
			    "subSubField", subSubField
			    };
		    
			try
			{
			    Table1.Record r1=table1.addRecord(parameters);
			    Table3.Record r3=table3.addRecord(parameters);
			    if (no_thread)
			    {
				Assert.assertTrue(table1.contains(r1));
				Assert.assertTrue(table3.contains(r3));
			    }
			}
			catch(ConstraintsNotRespectedDatabaseException e)
			{
			    if (no_thread)
			    {
				throw e;
			    }
			}
		    
			    try
			    {
				Table1.Record r1=table1.addRecord(parameters);
				Table3.Record r2=table3.addRecord(parameters);
				table1.removeRecord(r1);
				table3.removeRecord(r2);
			    }
			    catch(ConstraintsNotRespectedDatabaseException e)
			    {
				
			    }
			    catch(RecordNotFoundDatabaseException e)
			    {
				if (no_thread)
				    throw e;
			    }

		    Object []p2={"rert"};
		    Object []p3={new Integer(125), "rert"};
		    try
		    {
			table1.addRecord(p2);
			Assert.assertTrue(false);
		    }
		    catch(Exception e)
		    {
			Assert.assertTrue(true);
		    }
		    try
		    {
			table1.addRecord(p3);
			Assert.assertTrue(false);
		    }
		    catch(Exception e)
		    {
			Assert.assertTrue(true);
		    }
		    try
		    {
			table3.addRecord(p2);
			Assert.assertTrue(false);
		    }
		    catch(Exception e)
		    {
			Assert.assertTrue(true);
		    }
		    try
		    {
			table3.addRecord(p3);
			Assert.assertTrue(false);
		    }
		    catch(Exception e)
		    {
			Assert.assertTrue(true);
		    }
	    }
	    break;
	    case 31:
	    {
		Table1.Record r1=new Table1.Record();
		
		r1.pk1=random.nextInt();
		r1.int_value=3;
		r1.byte_value=(byte)3;
		r1.char_value='x';
		r1.boolean_value=true;
		r1.short_value=(short)random.nextInt();
		r1.long_value=3;
		r1.float_value=3.3f;
		r1.double_value=3.3;
		r1.string_value="test string";
		r1.IntegerNumber_value=new Integer(3);
		r1.ByteNumber_value=new Byte((byte)3);
		r1.CharacterNumber_value=new Character('x');
		r1.BooleanNumber_value=new Boolean(true);
		r1.ShortNumber_value=new Short((short)3);
		r1.LongNumber_value=new Long(random.nextLong());
		r1.FloatNumber_value=new Float(3.3f);
		r1.DoubleNumber_value=new Double(3.3);
		r1.BigInteger_value=new BigInteger("6");
		r1.BigDecimal_value=new BigDecimal("1.10");
		r1.DateValue=Calendar.getInstance().getTime();
		r1.CalendarValue=Calendar.getInstance();
		r1.secretKey=secretKey;
		r1.typeSecretKey=typeSecretKey;
		r1.subField=subField;
		r1.subSubField=subSubField;
			
		byte[] tab=new byte[3];
		tab[0]=0;
		tab[1]=1;
		tab[2]=2;
		r1.byte_array_value=tab;

		Table3.Record r3=new Table3.Record();
		
		r3.pk1=random.nextInt();
		r3.int_value=3;
		r3.byte_value=(byte)3;
		r3.char_value='x';
		r3.boolean_value=true;
		r3.short_value=(short)random.nextInt();
		r3.long_value=3;
		r3.float_value=3.3f;
		r3.double_value=3.3;
		r3.string_value="test string";
		r3.IntegerNumber_value=new Integer(3);
		r3.ByteNumber_value=new Byte((byte)3);
		r3.CharacterNumber_value=new Character('x');
		r3.BooleanNumber_value=new Boolean(true);
		r3.ShortNumber_value=new Short((short)3);
		r3.LongNumber_value=new Long(random.nextLong());
		r3.FloatNumber_value=new Float(3.3f);
		r3.DoubleNumber_value=new Double(3.3);
		r3.BigInteger_value=new BigInteger("6");
		r3.BigDecimal_value=new BigDecimal("1.10");
		r3.DateValue=Calendar.getInstance().getTime();
		r3.CalendarValue=Calendar.getInstance();
		r3.secretKey=secretKey;
		r3.typeSecretKey=typeSecretKey;
		r3.byte_array_value=tab;
		r3.subField=subField;
		r3.subSubField=subSubField;
		try
		{
		    Table1.Record r1b=table1.addRecord(r1);
		    Table3.Record r3b=table3.addRecord(r3);
		    if (no_thread)
		    {
			Assert.assertTrue(table1.contains(r1));
			Assert.assertTrue(table3.contains(r3));
		    }
		    Assert.assertTrue(r1==r1b);
		    Assert.assertTrue(r3==r3b);
		}
		catch(ConstraintsNotRespectedDatabaseException e)
		{
		    if (no_thread)
		    {
			throw e;
		    }
		}
	    }
	    break;
	    case 32:
	    {
		try
		{
		    ArrayList<Table1.Record> records1=table1.getRecords();
		    if (records1.size()>0)
		    {
		    
			Table1.Record r1=records1.get(0);
			r1.long_value=123456789l;
			table1.updateRecord(r1);
			if (no_thread)
			{
			    Table1.Record r1b=table1.getRecords().get(0);
			    Assert.assertTrue(r1b.long_value==r1.long_value);
			}
		    }
		    ArrayList<Table3.Record> records3=table3.getRecords();
		    if (records3.size()>0)
		    {
			
			Table3.Record r3=records3.get(0);
			r3.long_value=123456789l;
			table3.updateRecord(r3);
			if (no_thread)
			{
			    Table3.Record r3b=table3.getRecords().get(0);
			    Assert.assertTrue(r3b.long_value==r3.long_value);
			}
		    }
		}
		catch(RecordNotFoundDatabaseException e)
		{
		    if (no_thread)
			throw e;
		}
	    }
	    break;
		
	    	
	}
	}
	else
	{
	    System.out.println("Test "+number+" number "+r+" skipped.");
	}
	try
	{
	    table1.checkDataIntegrity();
	    table3.checkDataIntegrity();
	    table2.checkDataIntegrity();
	    table4.checkDataIntegrity();
	    table5.checkDataIntegrity();
	    table6.checkDataIntegrity();
	}
	catch(DatabaseException e)
	{
	    System.out.println("Exception in test number "+r);
	    e.printStackTrace();
	    throw e;
	}
	if (table1.getRecordsNumber()==0 || table2.getRecordsNumber()==0 || table3.getRecordsNumber()==0 || table4.getRecordsNumber()==0 || table5.getRecordsNumber()==0 || table6.getRecordsNumber()==0)
	{
	    System.out.println("\tAdding new records.");
	    prepareMultipleTest();
	}
	System.out.println("\tTest "+number+" number "+r+" OK!!!");
    }
    @Test(threadPoolSize = 5, invocationCount = 5, dependsOnMethods={"multipleTests"}) public void testThreadSafe() 
    {
	try
	{
	    no_thread=false;
	    System.out.println("No thread="+no_thread);
	    for (int i=0;i<getThreadTestsNumber();i++)
		subMultipleTests();
	}
	catch(Exception e)
	{
	    e.printStackTrace();
	    Assert.fail("testThreadSafe", e);
	}
    }
    @Test(threadPoolSize = 1, invocationCount = 1,  dependsOnMethods={"testThreadSafe"}) public void testCheckPoint() throws DatabaseException
    {
	((EmbeddedHSQLDBWrapper)table1.getDatabaseWrapper()).checkPoint(false);
	((EmbeddedHSQLDBWrapper)table1.getDatabaseWrapper()).checkPoint(true);
    }
    @Test(threadPoolSize = 1, invocationCount = 1,  dependsOnMethods={"testCheckPoint"}) public void testBackup() throws DatabaseException
    {
	table1.getDatabaseWrapper().backup(getDatabaseBackupFileName());
	table1.checkDataIntegrity();
	table2.checkDataIntegrity();
	table3.checkDataIntegrity();
	table4.checkDataIntegrity();
	table5.checkDataIntegrity();
	table6.checkDataIntegrity();
	table7.checkDataIntegrity();
    }
    @Test(threadPoolSize = 1, invocationCount = 1,  dependsOnMethods={"testBackup"}) public void testDatabaseRemove() throws DatabaseException
    {
	sql_db.deleteDatabase(dbConfig1);
	try
	{
	    sql_db.loadDatabase(dbConfig1, false);
	    Assert.fail();
	}
	catch(Exception e)
	{
	    
	}
    }
    @Test(threadPoolSize = 0, invocationCount = 0,  timeOut = 0) private void testOrderedTable1(ArrayList<Table1.Record> res)
    {
	if (res.size()>1)
	{
	    for (int i=1;i<res.size();i++)
	    {
		Assert.assertTrue(res.get(i-1).short_value<=res.get(i).short_value);
		Assert.assertTrue(res.get(i-1).short_value!=res.get(i).short_value || (res.get(i-1).short_value==res.get(i).short_value) && res.get(i-1).LongNumber_value.longValue()<=res.get(i).LongNumber_value.longValue());
	    }
	}
    }
    @Test(threadPoolSize = 0, invocationCount = 0,  timeOut = 0) private void testInverseOrderedTable1(ArrayList<Table1.Record> res)
    {
	if (res.size()>1)
	{
	    for (int i=1;i<res.size();i++)
	    {
		Assert.assertTrue(res.get(i-1).short_value>=res.get(i).short_value);
		Assert.assertTrue(res.get(i-1).short_value!=res.get(i).short_value || (res.get(i-1).short_value==res.get(i).short_value) && res.get(i-1).LongNumber_value.longValue()>=res.get(i).LongNumber_value.longValue());
	    }
	}
    }
    @Test(threadPoolSize = 0, invocationCount = 0,  timeOut = 0) private void testOrderedTable3(ArrayList<Table3.Record> res)
    {
	if (res.size()>1)
	{
	    for (int i=1;i<res.size();i++)
	    {
		Assert.assertTrue(res.get(i-1).short_value<=res.get(i).short_value);
		Assert.assertTrue(res.get(i-1).short_value!=res.get(i).short_value || (res.get(i-1).short_value==res.get(i).short_value) && res.get(i-1).LongNumber_value.longValue()<=res.get(i).LongNumber_value.longValue());
	    }
	}
    }
    @Test(threadPoolSize = 0, invocationCount = 0,  timeOut = 0) private void testInverseOrderedTable3(ArrayList<Table3.Record> res)
    {
	if (res.size()>1)
	{
	    for (int i=1;i<res.size();i++)
	    {
		Assert.assertTrue(res.get(i-1).short_value>=res.get(i).short_value);
		Assert.assertTrue(res.get(i-1).short_value!=res.get(i).short_value || (res.get(i-1).short_value==res.get(i).short_value) && res.get(i-1).LongNumber_value.longValue()>=res.get(i).LongNumber_value.longValue());
	    }
	}
    }
    
    
    
    
    
}


