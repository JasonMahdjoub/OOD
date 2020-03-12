
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

import com.distrimind.ood.database.exceptions.DatabaseException;
import com.distrimind.util.Utils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

/**
 * 
 * @author Jason Mahdjoub
 * @version 2.0
 * @since OOD 2.5.0
 */
public class MySQLTestDatabase extends TestDatabase {
	private static DistantMySQLDatabaseFactory factoryA= new DistantMySQLDatabaseFactory("127.0.0.1", 3306, "databasetestAMySQL", "usertest", "passwordtest");
	private static DistantMySQLDatabaseFactory factoryB= new DistantMySQLDatabaseFactory("127.0.0.1", 3306, "databasetestBMySQL", "usertest", "passwordtest");
	private static final String dockerName="mysqlOOD";
	private static String volumeID;

	public MySQLTestDatabase() throws DatabaseException, NoSuchAlgorithmException, NoSuchProviderException {
		super();
	}
	@BeforeClass
	public void createMySQLDocker() throws InterruptedException {
		stopMySQLDocker();
		rmMySQLDocker();
		runMySQLDocker();
		Thread.sleep(20000);
		createMySQLDB();
	}

	@AfterClass
	public void deleteMySQLDocker()
	{
		stopMySQLDocker();
		rmMySQLDocker();
		rmMySQLVolume();
	}
	private void createMySQLDB()
	{

		String rootPw=getRootMySQLPassword();
		System.out.println("Create MySQL Database");
		File tmpScript=new File("tmpScriptDockerForOOD.bash");

		Process p=null;
		try {
			try(FileWriter fos=new FileWriter(tmpScript))
			{
				fos.write("docker exec "+dockerName+" mysql --connect-expired-password --user=\"root\" --password=\""+rootPw+"\" -Bse \"ALTER USER 'root'@'localhost' IDENTIFIED BY 'rootpassword'; CREATE USER 'usertest' IDENTIFIED by 'passwordtest';CREATE DATABASE databasetestAMySQL;CREATE DATABASE databasetestBMySQL;GRANT ALL PRIVILEGES ON databasetestAMySQL.* TO 'usertest';GRANT ALL PRIVILEGES ON databasetestBMySQL.* TO 'usertest';FLUSH PRIVILEGES;\"\n");
			}
			ProcessBuilder pb=new ProcessBuilder("bash", tmpScript.toString());
			p = pb.start();

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {

			assert p != null;
			flushOutput(p);
			Utils.flushAndDestroyProcess(p);
			if (tmpScript.exists())
				//noinspection ResultOfMethodCallIgnored
				tmpScript.delete();
		}
	}

	private void flushOutput(Process p) {
		try {
			try(BufferedReader br=new BufferedReader(new InputStreamReader(p.getInputStream()));BufferedReader brerr=new BufferedReader(new InputStreamReader(p.getErrorStream())))
			{
				boolean c;
				do {
					c=false;
					String line=br.readLine();
					if (line!=null) {
						System.out.println(line);
						c = true;
					}
					line=brerr.readLine();
					if (line!=null) {
						System.err.println(line);
						c = true;
					}
				} while (c);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String getRootMySQLPassword()
	{
		System.out.println("Get root password");
		Process p=null;
		File tmpScript=new File("tmpScriptDockerForOOD.bash");
		try {
			try(FileWriter fos=new FileWriter(tmpScript))
			{
				fos.write("docker logs "+dockerName+" 2>&1 | grep GENERATED | awk '{print $NF}'\n");
			}
			ProcessBuilder pb = new ProcessBuilder("bash", tmpScript.toString());
			p = pb.start();
			InputStreamReader isr = new InputStreamReader(p.getInputStream());
			BufferedReader br = new BufferedReader(isr);
			return br.readLine();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {
			assert p != null;
			flushOutput(p);
			Utils.flushAndDestroyProcess(p);
			if (tmpScript.exists())
				//noinspection ResultOfMethodCallIgnored
				tmpScript.delete();
		}
		return null;
	}

	private void runMySQLDocker()
	{
		System.out.println("RUN Mysql docker");
		Process p=null;
		try {
			p = Runtime.getRuntime().exec("docker run -p 3306:3306 --name="+dockerName+" -e MYSQL_ROOT_HOST=% -d mysql/mysql-server:latest");
			BufferedReader br=new BufferedReader(new InputStreamReader(p.getInputStream()));
			volumeID=br.readLine();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {
			assert p != null;
			flushOutput(p);
			Utils.flushAndDestroyProcess(p);
		}
	}
	private void stopMySQLDocker()
	{
		System.out.println("STOP Mysql docker");
		Process p=null;
		try {
			p = Runtime.getRuntime().exec("docker stop "+dockerName);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {
			assert p != null;
			flushOutput(p);
			Utils.flushAndDestroyProcess(p);
		}
	}
	private void rmMySQLDocker()
	{
		System.out.println("RM Mysql docker");
		Process p=null;
		try {
			p = Runtime.getRuntime().exec("docker rm "+dockerName);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {
			assert p != null;
			flushOutput(p);
			Utils.flushAndDestroyProcess(p);
		}
	}
	private void rmMySQLVolume()
	{
		System.out.println("RM Mysql volume "+volumeID);
		Process p=null;
		try {
			p = Runtime.getRuntime().exec("docker volume rm "+volumeID);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {
			assert p != null;
			flushOutput(p);
			Utils.flushAndDestroyProcess(p);
		}
	}
	@Override
	public DatabaseWrapper getDatabaseWrapperInstanceA() throws IllegalArgumentException, DatabaseException {
		return factoryA.newWrapperInstance();
	}

	@Override
	public DatabaseWrapper getDatabaseWrapperInstanceB() throws IllegalArgumentException, DatabaseException {
		return factoryB.newWrapperInstance();
	}

	@Override
	public void deleteDatabaseFilesA() throws IllegalArgumentException {

	}

	@Override
	public void deleteDatabaseFilesB() throws IllegalArgumentException {

	}

	@AfterClass
	public static void unloadDatabase()  {
		TestDatabase.unloadDatabase();
	}

	@Override
	public File getDatabaseBackupFileName() {
		return null;
	}

	@Override
	public boolean isTestEnabled(int _testNumber) {
		return true;
	}

	@Override
	public int getMultiTestsNumber() {
		return 200;
	}

	@Override
	public int getThreadTestsNumber() {
		return 200;
	}

	@Override
	public boolean isMultiConcurrentDatabase() {
		return true;
	}

	@Override
	public void testBackup() {

	}
}
