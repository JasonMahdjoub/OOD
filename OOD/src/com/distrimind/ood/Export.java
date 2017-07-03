
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

import java.io.File;
import java.net.URL;
import java.util.ArrayList;

import javax.lang.model.SourceVersion;

import com.distrimind.ood.database.TestDatabase;
import com.distrimind.util.EmptyClass;
import com.distrimind.util.Utils;
import com.distrimind.util.export.BinaryDependency;
import com.distrimind.util.export.Dependency;
import com.distrimind.util.export.DirectorySourceDependency;
import com.distrimind.util.export.Exports;
import com.distrimind.util.export.JarDependency;
import com.distrimind.util.export.JarSourceDependancy;
import com.distrimind.util.export.JavaProjectSource;
import com.distrimind.util.export.License;
import com.distrimind.util.export.TestNGFile;
import com.distrimind.util.export.TestSuite;
import com.distrimind.util.export.Exports.ExportProperties;
import com.distrimind.util.export.License.PredefinedLicense;

/**
 * 
 * @author Jason Mahdjoub
 * @version 1.2
 * @since OOD 1.0
 */
class Export
{
    static void export() throws Exception
    {
	Exports exports=new Exports();
	File root_dir=new File("/home/jason/git_projects/OOD/OOD");
	File src_dir=new File(root_dir, "src");
	File srctests_dir=new File(root_dir, "Tests");
	String UtilsVersion=Utils.VERSION.getFileHeadVersion();
	Package root_package=Export.class.getPackage();
	ArrayList<File> additional_files=new ArrayList<>();
	System.out.println(OOD.VERSION);
	System.out.println(Utils.VERSION);
	System.out.println("Java version : "+System.getProperty("java.version"));
	
	License licenses[]=new License[1];
	licenses[0]=new License(new File("/home/jason/projets/commons-net-3.6/LICENSE.txt"));
	ArrayList<BinaryDependency> dependencies=new ArrayList<BinaryDependency>();
	dependencies.add(new JarDependency("commons-net", 
		new JarSourceDependancy(false, new File("/home/jason/projets/commons-net-3.6/commons-net-3.6-sources.jar"), null, null),
		org.apache.commons.net.SocketClient.class.getPackage(), 
		licenses, new File("/home/jason/projets/commons-net-3.6/commons-net-3.6.jar"), null, null));
	
	License[] licensesGnuCryto = { new License(new File("/home/jason/git_projects/Gnu-Crypt/GNU-Crypto/LICENSE"))};
	
	dependencies.add(new JarDependency("GNU-Crypto",
		new JarSourceDependancy(false,
			new File(
				"/home/jason/git_projects/Gnu-Crypt/GNU-Crypto/export/GNU-Crypto-0.99a.jar"),
			null, null),
		gnu.Version.class.getPackage(),
		licensesGnuCryto,
		new File(
			"/home/jason/git_projects/Gnu-Crypt/GNU-Crypto/export/GNU-Crypto-0.99a.jar"),
		null, null));
	
	
	licenses=new License[1];
	licenses[0]=new License(new File("/home/jason/projets/hsqldb/doc/hsqldb_lic.txt"));
	dependencies.add(new JarDependency("HSQLDB", 
		new DirectorySourceDependency(false, new File("/home/jason/projets/hsqldb/src")),
		org.apache.commons.net.SocketClient.class.getPackage(), 
		licenses, 
		new File("/home/jason/projets/hsqldb/lib/hsqldb.jar"), 
		Dependency.getDefaultBinaryExcludeRegex(), Dependency.getDefaultBinaryIncludeRegex()));

	licenses=new License[1];
	licenses[0]=new License(new File("/home/jason/projets/db-derby/LICENSE"));
	dependencies.add(new JarDependency("Derby", 
		new DirectorySourceDependency(false, new File("/home/jason/projets/db-derby/src/java/engine")),
		org.apache.commons.net.SocketClient.class.getPackage(), 
		licenses, 
		new File("/home/jason/projets/db-derby/lib/derby.jar"), 
		Dependency.getDefaultBinaryExcludeRegex(), Dependency.getDefaultBinaryIncludeRegex()));
	additional_files.add(new File("/home/jason/projets/db-derby/DERBY_NOTICE"));
	File utilsjarfile=new File("/home/jason/git_projects/Utils/exports/Utils-"+UtilsVersion+"_withSource.jar");
	licenses=new License[1];
	licenses[0]=Utils.LICENSE;
	dependencies.add(new JarDependency("Utils", 
		new JarSourceDependancy(true, utilsjarfile),
		Utils.class.getPackage(), 
		licenses, utilsjarfile, Dependency.getDefaultBinaryExcludeRegex(), Dependency.getDefaultBinaryIncludeRegex()));

	
	licenses=new License[1];
	licenses[0]=OOD.LICENSE;
	
	JavaProjectSource javaProjectSource=new JavaProjectSource(root_dir, src_dir, root_package, licenses, 
		"com/distrimind/ood/build.txt", 
		null, "OOD is an Object Oriented Data which aims to manage database only with Java language without using SQL querries", 
		OOD.VERSION,SourceVersion.RELEASE_7,
		dependencies,additional_files,new File("/usr/lib/jvm/java-7-openjdk-amd64"),
		Dependency.getRegexMatchClass(Export.class), null);
	javaProjectSource.setVerbose(false);
	
	
	dependencies=new ArrayList<BinaryDependency>();
	licenses=new License[1];
	licenses[0]=new License(PredefinedLicense.APACHE_LICENSE_V2_0);
	String testNGDir=".eclipse/org.eclipse.platform_4.6.3_1473617060_linux_gtk_x86_64/plugins/org.testng.eclipse_6.10.0.201612030230/lib/";
	
	dependencies.add(new JarDependency("TestNG", 
		org.testng.TestNG.class.getPackage(), 
		licenses, new File("/home/jason/"+testNGDir+"/testng.jar")));
	dependencies.add(new JarDependency("TestNG-jcommander", 
		org.testng.TestNG.class.getPackage(), 
		licenses, new File("/home/jason/"+testNGDir+"/jcommander.jar")));
	dependencies.add(new JarDependency("TestNG-snakeyaml", 
		org.testng.TestNG.class.getPackage(), 
		licenses, new File("/home/jason/"+testNGDir+"/snakeyaml.jar")));
	dependencies.add(new JarDependency("TestNG-bsh-2.0b4", 
		org.testng.TestNG.class.getPackage(), 
		licenses, new File("/home/jason/"+testNGDir+"/bsh-2.0b4.jar")));
	licenses=new License[1];
	licenses[0]=new License(PredefinedLicense.ECLIPSE_PUBLIC_LICENSE_V1_0);
	dependencies.add(new JarDependency("JUnit", 
		org.testng.TestNG.class.getPackage(), 
		licenses, new File("/opt/eclipse/plugins/org.junit_4.12.0.v201504281640/junit.jar")));
	licenses=new License[1];
	licenses[0]=Utils.LICENSE;
	dependencies.add(new JarDependency("Utils-Tests", null,
		EmptyClass.class.getPackage(), 
		licenses, new File("/home/jason/git_projects/Utils/exports/Utils-Tests-"+UtilsVersion+"_withDependencies.jar"), null, Dependency.getRegexMatchPackage(EmptyClass.class.getPackage())));
	
	javaProjectSource.setTestSuiteSource(root_dir, srctests_dir, TestDatabase.class.getPackage(), 
		dependencies, null, new TestSuite(new TestNGFile(EmptyClass.class.getPackage(), "AllTestsNG.xml"), new TestNGFile(TestDatabase.class.getPackage(), "AllTestsNG.xml")));
	
	javaProjectSource.setGitHUBLink(new URL("https://github.com/JazZ51/OOD.git"));
	
	
	exports.setProject(javaProjectSource);
	exports.setExportDirectory(new File(root_dir, "exports"));
	exports.setTemporaryDirectory(new File(root_dir, ".tmp_export"));
	
	ArrayList<ExportProperties> export_properties=new ArrayList<>();
	export_properties.add(new ExportProperties(true, Exports.SourceCodeExportType.SOURCE_CODE_IN_SEPERATE_FILE, true));
	export_properties.add(new ExportProperties(false, Exports.SourceCodeExportType.NO_SOURCE_CODE, false));
	export_properties.add(new ExportProperties(false, Exports.SourceCodeExportType.SOURCE_CODE_IN_JAR_FILE, false));
	export_properties.add(new ExportProperties(true, Exports.SourceCodeExportType.SOURCE_CODE_IN_JAR_FILE, false));
	export_properties.add(new ExportProperties(true, Exports.SourceCodeExportType.NO_SOURCE_CODE, false));
	
	exports.setExportsSenarios(export_properties);
	
	exports.export();
    }
    
    
    public static void main(String args[]) throws Exception
    {
	export();
    }
}
