/*
 * Object Oriented Database (created by Jason MAHDJOUB (jason.mahdjoub@free.fr)) Copyright (c)
 * 2012, JBoss Inc., and individual contributors as indicated by the @authors
 * tag.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */


package ood.export;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPListParseEngine;

import ood.version.Version;

public class Export
{
    public final static String[] excluded_packages={"ood.export", "ood.tests"};
    private final static String hsqldbPath="../hsqldb/";
    private final static String hsqldbJarFile=hsqldbPath+"lib/hsqldb.jar";
    private final static String hsqldbSrcDir=hsqldbPath+"src/";
    private final static String hsqldbVersion="2.2.9";
    private final static String ExportPathTmp=".export/";
    private final static String ExportPathFinal="export/";
    
    private final static String FTPURL="ftpperso.free.fr";
    private final static int FTPPORT=21;
    private final static String FTPLOGIN="objorienteddatabase";

    final private boolean export_source;
    final private boolean include_hsqldb;
    final private boolean export_only_jar_file;
    
    public Export(boolean _export_source, boolean _include_hsqldb, boolean _export_only_jar_file)
    {
	export_source=_export_source;
	include_hsqldb=_include_hsqldb;
	export_only_jar_file=_export_only_jar_file;
    }
    
    public void process() throws Exception
    {
	File exporttmp=new File(ExportPathTmp);
	if (exporttmp.exists())
	{
	    if (exporttmp.isDirectory())
		FileTools.deleteDirectory(exporttmp);
	    else
		exporttmp.delete();
	}
	
	if (!exporttmp.mkdir())
	    throw new IllegalAccessError("");
	
	File jardirectory=new File(exporttmp, "jardir");
	if (!jardirectory.mkdir())
	    throw new IllegalAccessError("");
	
	FileTools.copyFolderToFolder(new File("bin").getAbsolutePath(), "", new File("bin").getAbsolutePath(), jardirectory.getAbsolutePath());
	if (export_source)
	{
	    FileTools.copyFolderToFolder(new File("src").getAbsolutePath(), "", new File("src").getAbsolutePath(), jardirectory.getAbsolutePath());
	}
	
	excludePackages(jardirectory);
	
	if (include_hsqldb)
	{
	    File hsqldbtmp=new File(ExportPathTmp,"hsqldb");
	    hsqldbtmp.mkdir();
	    
	    FileTools.unzipFile(new File(hsqldbJarFile), hsqldbtmp.getAbsoluteFile());
	    if (export_source)
	    {
		File hsqldbsrctmp=new File(hsqldbSrcDir);

		FileTools.copyFolderToFolder(hsqldbsrctmp.getAbsolutePath(), "", hsqldbsrctmp.getAbsolutePath(), hsqldbtmp.getAbsolutePath());
	    }
	    new File(hsqldbtmp, "META-INF/MANIFEST.MF").delete();
	    FileTools.copyFolderToFolder(hsqldbtmp.getAbsolutePath(), "", hsqldbtmp.getAbsolutePath(), jardirectory.getAbsolutePath());
	}
	File metainfdir=new File(jardirectory, "META-INF");
	if (!metainfdir.exists())
	    metainfdir.mkdir();
	else if(metainfdir.isFile())
	    throw new IllegalAccessError();
	
	createManifestFile(new File(metainfdir, "MANIFEST.MF"));
	createVersionFile(new File(jardirectory, "version.html"));
	FileTools.copy(new File("LICENSE.TXT").getAbsolutePath(), (new File(jardirectory, "LICENSE.TXT")).getAbsolutePath());
	
	File exportdir=new File(exporttmp, "exportdir");
	exportdir.mkdir();
	File jarfile=new File(exportdir, getJarFileName());
	FileTools.zipDirectory(jardirectory, false, jarfile);
	
	
	
	if (!export_only_jar_file)
	{
	    File javadocdir=new File(exportdir, "doc");
	    javadocdir.mkdir();
	    File srcforjavadoc=new File(exporttmp, "srcforjavadoc");
	    srcforjavadoc.mkdir();
	    FileTools.copyFolderToFolder(new File("src").getAbsolutePath(), "", new File("src").getAbsolutePath(), srcforjavadoc.getAbsolutePath());
	    excludePackages(srcforjavadoc);
	    if (!include_hsqldb || !export_source)
	    {
		File hsqldbsrc=new File(hsqldbSrcDir);
		FileTools.copyFolderToFolder(hsqldbsrc.getAbsolutePath(), "", hsqldbsrc.getAbsolutePath(), srcforjavadoc.getAbsolutePath());
	    }
	    
	    
	    String command="javadoc -link http://docs.oracle.com/javase/7/docs/api/ -protected -sourcepath "+srcforjavadoc.getAbsolutePath()+" -d "+javadocdir.getAbsolutePath()+
		    " -version -author -subpackages ood "+(include_hsqldb?"-subpackages org":"");
	    System.out.println("\n*************************\n\n" +
	    		"Generating documentation\n" +
	    		"\n*************************\n\n");
	    execExternalProcess(command, true, true);
	    
	    FileTools.copy(new File("LICENSE.TXT").getAbsolutePath(), (new File(exportdir, "LICENSE.TXT")).getAbsolutePath());
	    createVersionFile(new File(exportdir, "OOD_version.html"));
	    
	    if (!include_hsqldb && export_source)
	    {
		File used_doc_folder=new File("./doc");
		if (used_doc_folder.exists())
		{
		    if (used_doc_folder.isFile())
			used_doc_folder.delete();
		    else
			FileTools.deleteDirectory(used_doc_folder);
		}
		used_doc_folder.mkdir();
		FileTools.copyFolderToFolder(javadocdir.getAbsolutePath(), "", javadocdir.getAbsolutePath(), used_doc_folder.getAbsolutePath());
		createVersionFile(new File(used_doc_folder, "OOD.html"));
	    }
	    /*File docdstdir=new File(exportdir, "doc");
	    docdstdir.mkdir();
	    FileTools.copyFolderToFolder(javadocdir.getAbsolutePath(), "", javadocdir.getAbsolutePath(), docdstdir.getAbsolutePath());*/
	    FileTools.zipDirectory(exportdir, false, new File(getExportDirectory(), getZipFileName()));
	}
	else
	{
	    FileTools.copy(jarfile.getAbsolutePath(), new File(getExportDirectory(), getJarFileName()).getAbsolutePath());
	}
	FileTools.deleteDirectory(exporttmp);
    }
    private File getExportDirectory()
    {
	File res=new File(Export.ExportPathFinal);
	if (!res.exists())
	    res.mkdir();
	else if(res.isFile())
	    throw new IllegalAccessError();
	res=new File(res, "ood-"+Version.OOD.getMajor()+"."+Version.OOD.getMinor());
	if (!res.exists())
	    res.mkdir();
	else if(res.isFile())
	    throw new IllegalAccessError();
	res=new File(res, include_hsqldb?"WithHSQLDB":"WithoutHSQLDB");
	if (!res.exists())
	    res.mkdir();
	else if(res.isFile())
	    throw new IllegalAccessError();

	return res;
    }
    
    private static void execExternalProcess(String command, final boolean screen_output, final boolean screen_erroutput) throws IOException, InterruptedException
    {
	Runtime runtime = Runtime.getRuntime();
	final Process process = runtime.exec(command);

	// Consommation de la sortie standard de l'application externe dans un Thread separe
	new Thread() {
		public void run() {
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
				String line = "";
				try {
					while((line = reader.readLine()) != null) {
						if (screen_output)
						{
						    System.out.println(line);
						}
					}
				} finally {
					reader.close();
				}
			} catch(IOException ioe) {
				ioe.printStackTrace();
			}
		}
	}.start();

	// Consommation de la sortie d'erreur de l'application externe dans un Thread separe
	new Thread() {
		public void run() {
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
				String line = "";
				try {
					while((line = reader.readLine()) != null) {
						if (screen_erroutput)
						{
						    System.out.println(line);
						}
					}
				} finally {
					reader.close();
				}
			} catch(IOException ioe) {
				ioe.printStackTrace();
			}
		}
	}.start();
	process.waitFor();
    }

    private void excludePackages(File _directory)
    {
	for (String s : excluded_packages)
	{
	    ArrayList<String> dirs=splitPoint(s);
	    File f=new File(_directory.getAbsolutePath());
	    for (int i=0;i<dirs.size();i++)
	    {
		f=new File(f, dirs.get(i));
		if (!f.exists())
		    throw new IllegalAccessError();
	    }
	    FileTools.deleteDirectory(f);
	}
	
    }
    
    private String getJarFileName()
    {
	return "ood-"+
		Integer.toString(Version.OOD.getMajor())+
		"."+
		Integer.toString(Version.OOD.getMinor())+
		"."+
		Integer.toString(Version.OOD.getRevision())+
		Version.OOD.getType()+
		((Version.OOD.getType().equals(Version.Type.Beta) || Version.OOD.getType().equals(Version.Type.Alpha))?Integer.toString(Version.OOD.getAlphaBetaVersion()):"")+
		(include_hsqldb?("+hsqldb-"+hsqldbVersion):"")+
		(export_source?"_withsrc":"")+
		".jar";
    }
    private String getZipFileName()
    {
	return "ood-"+
		Integer.toString(Version.OOD.getMajor())+
		"."+
		Integer.toString(Version.OOD.getMinor())+
		"."+
		Integer.toString(Version.OOD.getRevision())+
		Version.OOD.getType()+
		((Version.OOD.getType().equals(Version.Type.Beta) || Version.OOD.getType().equals(Version.Type.Alpha))?Integer.toString(Version.OOD.getAlphaBetaVersion()):"")+
		(include_hsqldb?("+hsqldb-"+hsqldbVersion):"")+
		(export_source?"_withsrc":"")+
		"_withdoc.zip";
    }
    
    private void createVersionFile(File f) throws IOException
    {
	FileWriter fw=new FileWriter(f);
	BufferedWriter b=new BufferedWriter(fw);
	b.write(Version.OOD.getHTMLCode());
	b.flush();
	b.close();
	fw.close();
    }
    
    private void createManifestFile(File f) throws IOException
    {
	FileWriter fw=new FileWriter(f);
	BufferedWriter b=new BufferedWriter(fw);
	b.write(getManifest());
	b.flush();
	b.close();
	fw.close();
    }
    private String getManifest()
    {
	String res="Manifest-Version: 1.0\n" +
		"Export-Package: org.hsqldb;uses:=\"org.hsqldb.server,org.hsqldb.jdbc,org.hsqldb.types,org.hsqldb.rowio,org.hsqldb.store,org.hsqldb.result,org.hsqldb.persist,org.hsqldb.error,org.hsqldb.navigator,org.hsqldb.lib,org.hsqldb.rights,org.hsqldb.index,org.hsqldb.dbinfo,org.hsqldb.lib.java,org.hsqldb.scriptio\",org.hsqldb.auth;uses:=\"org.hsqldb.jdbc,org.hsqldb.types,org.hsqldb.lib,javax.security.auth.callback,javax.security.auth.login,javax.security.auth,javax.naming.directory,javax.naming,javax.naming.ldap,javax.net.ssl\",org.hsqldb.dbinfo;uses:=\"org.hsqldb.rights,org.hsqldb.resources,org.hsqldb,org.hsqldb.store,org.hsqldb.persist,org.hsqldb.lib,org.hsqldb.types,org.hsqldb.result,org.hsqldb.index\",org.hsqldb.error;uses:=\"org.hsqldb.resources,org.hsqldb,org.hsqldb.result,org.hsqldb.lib\",org.hsqldb.index;uses:=\"org.hsqldb.persist,org.hsqldb.types,org.hsqldb,org.hsqldb.navigator,org.hsqldb.rights,org.hsqldb.error,org.hsqldb.lib,org.hsqldb.rowio\",org.hsqldb.jdbc;uses:=\"org.hsqldb.types,org.hsqldb,org.hsqldb.navigator,org.hsqldb.result,org.hsqldb.lib.java,org.hsqldb.lib,org.hsqldb.error,javax.sql,org.hsqldb.persist,javax.naming,javax.naming.spi,org.hsqldb.jdbc.pool,javax.xml.parsers,org.w3c.dom,org.xml.sax,javax.xml.stream,javax.xml.transform.stax,javax.xml.transform.dom,org.w3c.dom.bootstrap,javax.xml.transform,javax.xml.bind.util,javax.xml.transform.stream,javax.xml.transform.sax\",org.hsqldb.jdbc.pool;uses:=\"javax.sql,org.hsqldb.jdbc,org.hsqldb.lib,javax.naming,javax.transaction.xa,org.hsqldb,org.hsqldb.error\",org.hsqldb.lib;uses:=\"org.hsqldb.lib.java,org.hsqldb.store,org.hsqldb\",org.hsqldb.lib.java,org.hsqldb.lib.tar;uses:=\"org.hsqldb.lib\",org.hsqldb.lib.tar.rb,org.hsqldb.navigator;uses:=\"org.hsqldb,org.hsqldb.error,org.hsqldb.rowio,org.hsqldb.result,org.hsqldb.types,org.hsqldb.lib,org.hsqldb.index,org.hsqldb.persist\",org.hsqldb.persist;uses:=\"org.hsqldb.lib,org.hsqldb,org.hsqldb.store,org.hsqldb.rowio,javax.crypto.spec,org.hsqldb.error,javax.crypto,org.hsqldb.scriptio,org.hsqldb.navigator,org.hsqldb.lib.java,org.hsqldb.types,org.hsqldb.result,org.hsqldb.index,org.hsqldb.lib.tar,org.hsqldb.dbinfo\",org.hsqldb.resources;uses:=\"org.hsqldb.lib\",org.hsqldb.result;uses:=\"org.hsqldb.persist,org.hsqldb.types,org.hsqldb.error,org.hsqldb,org.hsqldb.navigator,org.hsqldb.rowio,org.hsqldb.store,org.hsqldb.lib\",org.hsqldb.rights;uses:=\"org.hsqldb.types,org.hsqldb.error,org.hsqldb,org.hsqldb.lib,org.hsqldb.result\",org.hsqldb.rowio;uses:=\"org.hsqldb.types,org.hsqldb.error,org.hsqldb,org.hsqldb.lib,org.hsqldb.store,org.hsqldb.persist,org.hsqldb.lib.java\",org.hsqldb.sample,org.hsqldb.scriptio;uses:=\"org.hsqldb.persist,org.hsqldb,org.hsqldb.error,org.hsqldb.rowio,org.hsqldb.lib,org.hsqldb.types,org.hsqldb.store,org.hsqldb.result,org.hsqldb.navigator\",org.hsqldb.server;uses:=\"org.hsqldb.persist,org.hsqldb.jdbc,org.hsqldb.error,org.hsqldb,javax.net,javax.security.cert,javax.net.ssl,org.hsqldb.lib,org.hsqldb.types,org.hsqldb.result,org.hsqldb.resources,org.hsqldb.lib.java,org.hsqldb.store,org.hsqldb.rowio,org.hsqldb.navigator,javax.servlet,javax.servlet.http\",org.hsqldb.store;uses:=\"org.hsqldb.lib,org.hsqldb.types\",org.hsqldb.types;uses:=\"org.hsqldb.jdbc,org.hsqldb.error,org.hsqldb,org.hsqldb.lib,org.hsqldb.store,org.hsqldb.result,org.hsqldb.lib.java,org.hsqldb.persist,org.hsqldb.rights\",org.hsqldb.util;uses:=\"javax.swing,org.hsqldb.lib.java,javax.swing.border,org.hsqldb.lib,javax.swing.tree,javax.swing.table,javax.swing.event\"\n"+
	"Description: "+Version.OOD.getProgramName()+", working with HSQLDB "+hsqldbVersion+".\n"+
	"Version: "+Version.OOD.toStringShort()+"\n"+
	"Author: ";
	boolean first=true;
	for (ood.version.PersonDeveloper p : Version.OOD.getDevelopers())
	{
	    if (first)
		first=false;
	    else
		res+=", ";
	    res+=p.getFirstName()+" "+p.getName();
	}
	res+="\nBuilt-By: Jason Mahdjoub\n";
	res+="Import-Package: javax.crypto;resolution:=optional,javax.crypto.spec;resolution:=optional,javax.naming;resolution:=optional,javax.naming.directory;resolution:=optional,javax.naming.ldap;resolution:=optional,javax.naming.spi;resolution:=optional,javax.net;resolution:=optional,javax.net.ssl;resolution:=optional,javax.security.auth;resolution:=optional,javax.security.auth.callback;resolution:=optional,javax.security.auth.login;resolution:=optional,javax.security.cert;resolution:=optional,javax.servlet;resolution:=optional,javax.servlet.http;resolution:=optional,javax.sql;resolution:=optional,javax.swing;resolution:=optional,javax.swing.border;resolution:=optional,javax.swing.event;resolution:=optional,javax.swing.table;resolution:=optional,javax.swing.tree;resolution:=optional,javax.transaction.xa;resolution:=optional,javax.xml.bind.util;resolution:=optional,javax.xml.parsers;resolution:=optional,javax.xml.stream;resolution:=optional,javax.xml.transform;resolution:=optional,javax.xml.transform.dom;resolution:=optional,javax.xml.transform.sax;resolution:=optional,javax.xml.transform.stax;resolution:=optional,javax.xml.transform.stream;resolution:=optional,org.hsqldb;resolution:=optional,org.hsqldb.auth;resolution:=optional,org.hsqldb.dbinfo;resolution:=optional,org.hsqldb.error;resolution:=optional,org.hsqldb.index;resolution:=optional,org.hsqldb.jdbc;resolution:=optional,org.hsqldb.jdbc.pool;resolution:=optional,org.hsqldb.lib;resolution:=optional,org.hsqldb.lib.java;resolution:=optional,org.hsqldb.lib.tar;resolution:=optional,org.hsqldb.lib.tar.rb;resolution:=optional,org.hsqldb.navigator;resolution:=optional,org.hsqldb.persist;resolution:=optional,org.hsqldb.resources;resolution:=optional,org.hsqldb.result;resolution:=optional,org.hsqldb.rights;resolution:=optional,org.hsqldb.rowio;resolution:=optional,org.hsqldb.sample;resolution:=optional,org.hsqldb.scriptio;resolution:=optional,org.hsqldb.server;resolution:=optional,org.hsqldb.store;resolution:=optional,org.hsqldb.types;resolution:=optional,org.hsqldb.util;resolution:=optional,org.w3c.dom;resolution:=optional,org.w3c.dom.bootstrap;resolution:=optional,org.xml.sax;resolution:=optional\n";
	return res;
    }
    
    private static ArrayList<String> splitPoint(String s)
    {
	ArrayList<String> res=new ArrayList<String>(10);
	int last_index=0;
	for (int i=0;i<s.length();i++)
	{
	    if (s.charAt(i)=='.')
	    {
		if (i!=last_index)
		{
		    res.add(s.substring(last_index, i));
		}
		last_index=i+1;
	    }
	}
	if (s.length()!=last_index)
	{
	    res.add(s.substring(last_index));
	}
	
	return res;
    }
    
    public static void saveAndIncrementBuild(File _file) throws IOException
    {
	Version.OOD.setBuildNumber(Version.OOD.getBuildNumber()+1);
	FileWriter fw=new FileWriter(_file);
	BufferedWriter b=new BufferedWriter(fw);
	b.write(Integer.toString(Version.OOD.getBuildNumber()));
	b.flush();
	b.close();
	fw.close();
    }
    public static void updateFTP(FTPClient ftpClient, String _directory_dst, File _directory_src, File _current_file_transfert, File _current_directory_transfert) throws IOException, TransfertException
    {
	if (_current_directory_transfert==null || _directory_src.equals(_current_directory_transfert))
	{
	    try
	    {
		updateFTP(ftpClient, _directory_dst, _directory_src, _current_file_transfert);
	    }
	    catch(TransfertException e)
	    {
		e.current_directory_transfert=_directory_src;
		throw e;
	    }
	}
    }
    
    public static void updateFTP(FTPClient ftpClient, String _directory_dst, File _directory_src, File _current_file_transfert) throws IOException, TransfertException
    {
	ftpClient.changeWorkingDirectory("./");
	FTPListParseEngine ftplpe=ftpClient.initiateListParsing(_directory_dst);
	FTPFile files[]=ftplpe.getFiles();
	
	File current_file_transfert=_current_file_transfert;
	
	try
	{
	    for (File f : _directory_src.listFiles())
	    {
		if (f.isDirectory())
		{
		    if (!f.getName().equals("./") && !f.getName().equals("../"))
		    {
			if (_current_file_transfert!=null)
			{
			    if (!_current_file_transfert.getCanonicalPath().startsWith(f.getCanonicalPath()))
				continue;
			    else
				_current_file_transfert=null;
			}
			boolean found=false;
			for (FTPFile ff : files)
			{
			    if (f.getName().equals(ff.getName()))
			    {	
				if (ff.isFile())
				{ 
				    ftpClient.deleteFile(_directory_dst+ff.getName());
				}
				else
				    found=true;
				break;
			    }
			}
		
			if (!found)
			{
			    ftpClient.changeWorkingDirectory("./");
			    if (!ftpClient.makeDirectory(_directory_dst+f.getName()+"/"))
				System.err.println("Impossible to create directory "+_directory_dst+f.getName()+"/");
			}
			updateFTP(ftpClient, _directory_dst+f.getName()+"/", f, _current_file_transfert);
		    }
		}
		else
		{
		    if (_current_file_transfert!=null)
		    {
			if (!_current_file_transfert.equals(f.getCanonicalPath()))
			    continue;
			else
			    _current_file_transfert=null;
		    }
		    current_file_transfert=_current_file_transfert;
		    FTPFile found=null;
		    for (FTPFile ff : files)
		    {
			if (f.getName().equals(ff.getName()))
			{
			    if (ff.isDirectory())
			    {
				FileTools.removeDirectory(ftpClient, _directory_dst+ff.getName());
			    }
			    else
				found=ff;
			    break;
			}
		    }
		    if (found==null || (found.getTimestamp().getTimeInMillis()-f.lastModified())<0 || found.getSize()!=f.length())
		    {
			FileInputStream fis=new FileInputStream(f);
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			if (!ftpClient.storeFile(_directory_dst+f.getName(), fis))
			    System.err.println("Impossible to send file: "+_directory_dst+f.getName());
			fis.close();
			for (FTPFile ff : ftplpe.getFiles())
			{
			    if (f.getName().equals(ff.getName()))
			    {
				f.setLastModified(ff.getTimestamp().getTimeInMillis());
				break;
			    }
			}
		    }
		}
		
	    }
	}
	catch(IOException e)
	{
	    throw new TransfertException(current_file_transfert, null, e);
	}
	for (FTPFile ff : files)
	{
	    if (!ff.getName().equals(".") && !ff.getName().equals(".."))
	    {
		boolean found=false;
		for (File f : _directory_src.listFiles())
		{
		    if (f.getName().equals(ff.getName()) && f.isDirectory()==ff.isDirectory())
		    {
			found=true;
			break;
		    }
		}
		if (!found)
		{
		    if (ff.isDirectory())
		    {
			FileTools.removeDirectory(ftpClient, _directory_dst+ff.getName());
		    }
		    else
		    {
			ftpClient.deleteFile(_directory_dst+ff.getName());
		    }
		}
	    }
	}
    }
    
    private static void sendToWebSite() throws IOException
    {
	System.out.println("Enter your password :");
	byte b[]=new byte[100];
	int l=System.in.read(b);
	String pwd=new String(b, 0, l);
	
	boolean reconnect=true;
	long time=System.currentTimeMillis();
	File current_file_transfert=null;
	File current_directory_transfert=null;
	
	while (reconnect)
	{
	    FTPClient ftpClient=new FTPClient();
	    ftpClient.connect(FTPURL, 21);
	    try
	    {
		if (ftpClient.isConnected())
		{
		    System.out.println("Connected to server "+FTPURL+" (Port: "+FTPPORT+") !");
		    if(ftpClient.login(FTPLOGIN, pwd))
		    {
			ftpClient.setFileTransferMode(FTP.BINARY_FILE_TYPE);
			System.out.println("Logged as "+FTPLOGIN+" !");
			System.out.print("Updating...");
		    
		
			FTPFile files[]=ftpClient.listFiles("");
			FTPFile downloadroot=null;
			FTPFile docroot=null;
		
			for (FTPFile f : files)
			{
			    if (f.getName().equals("downloads"))
			    {
				downloadroot=f;
				if (docroot!=null)
				    break;
			    }
			    if (f.getName().equals("doc"))
			    {
				docroot=f;
				if (downloadroot!=null)
				    break;
			    }
			}
			if (downloadroot==null)
			{
			    //ftpClient.changeWorkingDirectory("/");
			    if (!ftpClient.makeDirectory("downloads"))
			    {
				System.err.println("Impossible to create directory: downloads");
			    }
			}
			if (docroot==null)
			{
			    //ftpClient.changeWorkingDirectory("/");
			    if (!ftpClient.makeDirectory("doc"))
			    {
				System.err.println("Impossible to create directory: doc");
			    }
			}
		
			updateFTP(ftpClient, "downloads/", new File(ExportPathFinal), current_file_transfert, current_directory_transfert);
			updateFTP(ftpClient, "doc/", new File("./doc"), current_file_transfert, current_directory_transfert);
			reconnect=false;
			
			System.out.println("[OK]");
			if (ftpClient.logout())
			{
			    System.out.println("Logged out from "+FTPLOGIN+" succesfull !");		    
			}
			else
			    System.err.println("Logged out from "+FTPLOGIN+" FAILED !");
		    }	
		    else
			System.err.println("Impossible to log as "+FTPLOGIN+" !");
	    
	    
		    ftpClient.disconnect();
		    System.out.println("Disconnected from "+FTPURL+" !");
		}
		else
		{
		    System.err.println("Impossible to get a connection to the server "+FTPURL+" !");
		}
		reconnect=false;
	    }
	    catch(TransfertException e)
	    {
		if (System.currentTimeMillis()-time>30000)
		{
		    System.err.println("A problem occured during the transfert...");
		    System.out.println("Reconnection in progress...");
		    try
		    {
			ftpClient.disconnect();
		    }
		    catch(Exception e2)
		    {
		    }
		    current_file_transfert=e.current_file_transfert;
		    current_directory_transfert=e.current_directory_transfert;
		    time=System.currentTimeMillis();
		}
		else
		{
		    System.err.println("A problem occured during the transfert. Transfert aborded.");
		    throw e.original_exception;
		}
	    }
	}
    }
    
    public static void main(String args[]) throws IllegalAccessException, Exception
    {
	saveAndIncrementBuild(new File("src/ood/version/build.txt"));
	FileTools.copy(new File("src/ood/version/build.txt").getAbsolutePath(), new File("bin/ood/version/build.txt").getAbsolutePath());
	new Export(true, true, false).process();
	new Export(true, false, false).process();
	new Export(false, true, false).process();
	new Export(false, false, false).process();
	Export e=new Export(true, true, true);
	e.process();
	FileTools.copy(new File(e.getExportDirectory(), e.getJarFileName()).getAbsolutePath(), new File("ood.jar").getAbsolutePath());
	new Export(false, true, true).process();
	new Export(true, false, true).process();
	new Export(false, false, true).process();
	System.out.println("\n**************************\n\nUpdating Web site ? (y[es]|n[o])");
	byte b[]=new byte[100];
	System.in.read(b);
	if (b[0]=='y' || b[0]=='Y')
	{
	    sendToWebSite();
	}
	
		
    }
}
