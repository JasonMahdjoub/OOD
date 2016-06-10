
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
package com.distrimind.ood;

import java.io.InputStream;
import java.util.Calendar;

import com.distrimind.util.export.License;
import com.distrimind.util.version.Description;
import com.distrimind.util.version.Person;
import com.distrimind.util.version.PersonDeveloper;
import com.distrimind.util.version.Version;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.5
 * @since OOD 1.0
 */
public class OOD
{
    public static final Version VERSION;
    public static final License LICENSE=new License(License.PredefinedLicense.CeCILL_C_v1_0);
    static
    {
	Calendar c1=Calendar.getInstance();
	c1.set(2013, 3, 1);
	Calendar c2=Calendar.getInstance();
	c2.set(2016, 5, 10);
	VERSION=new Version("Object Oriented Database", "OOD", 1, 6, 1, Version.Type.Stable, 0, c1.getTime(), c2.getTime());
	try
	{
	    InputStream is=OOD.class.getResourceAsStream("build.txt");
	    VERSION.loadBuildNumber(is);
	    VERSION.addCreator(new Person("mahdjoub", "jason"));
	    Calendar c=Calendar.getInstance();
	    c.set(2013, 3, 1);
	    VERSION.addDeveloper(new PersonDeveloper("mahdjoub", "jason", c.getTime()));
	
	    c=Calendar.getInstance();
	    c.set(2016, 5, 3);
	    Description d=new Description(1,6,1,Version.Type.Stable, 0, c.getTime());
	    d.addItem("Correction a bug into the constructor of ByteTabConvertibleFieldAccessor.");
	    d.addItem("Adding version tests.");
	    d.addItem("Changing license to CECILL-C.");

	    c=Calendar.getInstance();
	    c.set(2016, 2, 11);
	    d=new Description(1,6,0,Version.Type.Stable, 0, c.getTime());
	    d.addItem("Updating with Utils 1.6.");
	    d.addItem("Adding database backup tools.");
	    d.addItem("renaming alterRecord functions to updateRecord.");
	    d.addItem("Adding functions Table.addRecord(record), Table.updateRecord(record).");

	    c=Calendar.getInstance();
	    c.set(2016, 2, 4);
	    d=new Description(1,5,2,Version.Type.Stable, 0, c.getTime());
	    d.addItem("Updating with Utils 1.5");
	    d.addItem("Adding encryption keys encoding/decoding.");
	    d.addItem("Correcting bugs with ByteTabConvertibleFieldAccessor class.");

	    c=Calendar.getInstance();
	    c.set(2016, 2, 1);
	    d=new Description(1,5,1,Version.Type.Stable, 0, c.getTime());
	    d.addItem("Updating with Utils 1.4.");
	    d.addItem("Adding AllTestsNG.xml file.");

	    c=Calendar.getInstance();
	    c.set(2016, 1, 15);
	    d=new Description(1,5,0,Version.Type.Stable, 0, c.getTime());
	    d.addItem("Adding iterator functionality in class Table.");
	    d.addItem("Adding ByteTabObjectConverter class.");
	    d.addItem("Adding DefaultByteTabObjectConverter class.");
	    d.addItem("Adding ByteTabConvertibleFieldAccessor class.");
	    d.addItem("Adding function addByteTabObjectConverter in DatabaseWrapper class.");
	    d.addItem("Adding possibility to use Object tabs as an alternative of use of maps when reffering to fields.");
	    d.addItem("Optimizing use of SQL database.");
	    d.addItem("Linking with Utils 1.3.");
	    
	    
	    
	    VERSION.addDescription(d);

	    c=Calendar.getInstance();
	    c.set(2016, 1, 14);
	    d=new Description(1,4,1,Version.Type.Stable, 0, c.getTime());
	    d.addItem("Adding some close statements corrections.");
	    d.addItem("Adding some multi-thread optimisations.");
	    VERSION.addDescription(d);

	    c=Calendar.getInstance();
	    c.set(2016, 1, 8);
	    d=new Description(1,4,0,Version.Type.Stable, 0, c.getTime());
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
