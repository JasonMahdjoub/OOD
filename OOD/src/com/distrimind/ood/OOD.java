package com.distrimind.ood;

import java.io.InputStream;
import java.util.Calendar;

import com.distrimind.util.export.License;
import com.distrimind.util.version.Description;
import com.distrimind.util.version.Person;
import com.distrimind.util.version.PersonDeveloper;
import com.distrimind.util.version.Version;

public class OOD
{
    public static final Version VERSION;
    public static final License LICENSE=new License(License.PredefinedLicense.GNU_LGPL_v3_0);
    static
    {
	Calendar c1=Calendar.getInstance();
	c1.set(2013, 3, 1);
	Calendar c2=Calendar.getInstance();
	c2.set(2016, 1, 8);
	VERSION=new Version("Object Oriented Database", 1, 4, 0, Version.Type.Stable, 0, c1.getTime(), c2.getTime());
	try
	{
	    InputStream is=OOD.class.getResourceAsStream("build.txt");
	    VERSION.loadBuildNumber(is);
	    VERSION.addCreator(new Person("mahdjoub", "jason"));
	    Calendar c=Calendar.getInstance();
	    c.set(2013, 3, 1);
	    VERSION.addDeveloper(new PersonDeveloper("mahdjoub", "jason", c.getTime()));
	
	    c=Calendar.getInstance();
	    c.set(2016, 1, 8);
	    Description d=new Description(1,4,0,Version.Type.Stable, 0, c.getTime());
	    d.addItem("One databse is associated to one package. Now, its is possible to load several database/packages into the same file.");
	    d.addItem("OOD works now with HSQLDB or Apache Derby.");
	    VERSION.addDescription(d);

	    c=Calendar.getInstance();
	    c.set(2016, 1, 5);
	    d=new Description(1,3,0,Version.Type.Stable, 0, c.getTime());
	    d.addItem("Adding dependency with Utils and updating OOD consequently.");
	    VERSION.addDescription(d);

	    c=Calendar.getInstance();
	    c.set(2016, 1, 1);
	    d=new Description(1,2,0,Version.Type.Stable, 0, c.getTime());
	    d.addItem("Correcting some bugs into the documentation.");
	    d.addItem("Upgrading to HSQLDB 2.3.3 and Commons-Net 3.4.");
	    VERSION.addDescription(d);

	    c=Calendar.getInstance();
	    c.set(2013, 10, 18);
	    d=new Description(1,1,0,Version.Type.Stable, 0, c.getTime());
	    d.addItem("Improving function Table.alterRecords(...) and class AlterRecordFilter (see documentation).");
	    VERSION.addDescription(d);
	
	    c=Calendar.getInstance();
	    c.set(2013, 3, 24);
	    d=new Description(1,0,0,Version.Type.Stable, 0, c.getTime());
	    d.addItem("Releasing Oriented Object Database as a stable version.");
	    VERSION.addDescription(d);
	}
	catch(Exception e)
	{
	    e.printStackTrace();
	}
    }    
}
