
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
public class PostgreSQLTestDatabase extends TestDatabase {
	private static final String dockerName="postgreSQLOOD";
	static final String rootPwd="rootPassword";
	static final String userName="postgres";
	private static final DistantPostgreSQLDatabaseFactory factoryA= new DistantPostgreSQLDatabaseFactory("127.0.0.1", 5432, "databasetestAPostgreSQL", userName, rootPwd);
	private static final DistantPostgreSQLDatabaseFactory factoryB= new DistantPostgreSQLDatabaseFactory("127.0.0.1", 5432, "databasetestBPostgreSQL", userName, rootPwd);

	public PostgreSQLTestDatabase() throws DatabaseException, NoSuchAlgorithmException, NoSuchProviderException {
		super();
	}
	@BeforeClass
	public void createPostgreSQLDocker() throws InterruptedException {
		stopPostgreSQLDocker();
		rmPostgreSQLDocker();
		runPostgreSQLDocker();
		Thread.sleep(1000);
		createPostgreSQLDB();
	}

	@AfterClass
	public void deletePostgreSQLDocker()
	{
		stopPostgreSQLDocker();
		rmPostgreSQLDocker();
	}
	private String createDatabaseBashQuery(String databaseName)
	{
		return "docker exec "+dockerName+" su -c \"psql -c \\\"CREATE DATABASE "+databaseName+";\\\"\" postgres";
	}
	private void createPostgreSQLDB()
	{

		System.out.println("Create MySQL Database");
		File tmpScript=new File("tmpPostGreScriptDockerForOOD.bash");

		Process p=null;
		try {
			try(FileWriter fos=new FileWriter(tmpScript))
			{
				fos.write(createDatabaseBashQuery(factoryA.databaseName));
				fos.write(" && ");
				fos.write(createDatabaseBashQuery(factoryB.databaseName));
				fos.write("\n");
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


	private void runPostgreSQLDocker()
	{
		System.out.println("RUN PostgreSQL docker");
		Process p=null;
		try {
			p = Runtime.getRuntime().exec("docker run -p 5432:5432 --name="+dockerName+" -e POSTGRES_PASSWORD="+rootPwd+" -d postgres");
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
	private void stopPostgreSQLDocker()
	{
		System.out.println("STOP PostgreSQL docker");
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
	private void rmPostgreSQLDocker()
	{
		System.out.println("RM PostgreSQL docker");
		Process p=null;
		try {
			p = Runtime.getRuntime().exec("docker rm -v "+dockerName);
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
