package com.distrimind.ood.database.tests;
/*
Copyright or © or Copr. Jason Mahdjoub (01/04/2013)

jason.mahdjoub@distri-mind.fr

This software (Object Oriented Database (OOD)) is a computer program 
whose purpose is to manage a local database with the object paradigm 
and the java language 

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

import com.distrimind.ood.database.CommonDecentralizedTests;
import com.distrimind.ood.database.DatabaseRecord;
import com.distrimind.ood.database.TableEvent;
import com.distrimind.ood.database.exceptions.DatabaseException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.0.0
 */
public abstract class TestDatabaseToOperateActionIntoDecentralizedNetwork extends CommonDecentralizedTests {
	protected final boolean useCentralDatabaseBackup;
	protected final boolean canSendIndirectTransactions;
	protected final boolean upgradeDatabaseVersionWhenConnectedWithPeers;
	protected final boolean upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion;
	protected final boolean hasToRemoveOldDatabase;

	protected TestDatabaseToOperateActionIntoDecentralizedNetwork(boolean useCentralDatabaseBackup, boolean canSendIndirectTransactions,
																boolean upgradeDatabaseVersionWhenConnectedWithPeers,
																boolean upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion,
																boolean hasToRemoveOldDatabase) {
		this.useCentralDatabaseBackup=useCentralDatabaseBackup;
		this.canSendIndirectTransactions=canSendIndirectTransactions;
		this.upgradeDatabaseVersionWhenConnectedWithPeers=upgradeDatabaseVersionWhenConnectedWithPeers;
		this.upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion=upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion;
		this.hasToRemoveOldDatabase=hasToRemoveOldDatabase;
	}



	@Override
	public boolean canInitCentralBackup()
	{
		return useCentralDatabaseBackup;
	}
	@Override
	protected boolean sendIndirectTransactions()
	{
		return canSendIndirectTransactions;
	}

	@DataProvider(name = "constructorParameters")
	public static Object[][] constructorParameters() {
		Object[][] res=new Object[24][5];
		int i=0;
		for (boolean canSendIndirectTransactions : new boolean[]{true, false}) {
			for (boolean useCentralDatabaseBackup : new boolean[]{true, false}) {
				for (boolean upgradeDatabaseVersionWhenConnectedWithPeers : new boolean[]{true, false}) {
					for (boolean upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion : useCentralDatabaseBackup?new boolean[]{false, true}:new boolean[]{false}) {
						for (boolean hasToRemoveOldDatabase : new boolean[]{false, true}) {
							res[i][0] = useCentralDatabaseBackup;
							res[i][1] = canSendIndirectTransactions;
							res[i][2] = upgradeDatabaseVersionWhenConnectedWithPeers;
							res[i][3] = upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion;
							res[i++][4] = hasToRemoveOldDatabase;
						}
					}
				}

			}
		}
		assert i==res.length;
		return res;
	}


	@Override
	public String toString() {
		return this.getClass().getSimpleName()+"{" +
				"useCentralDatabaseBackup=" + useCentralDatabaseBackup +
				", canSendIndirectTransactions=" + canSendIndirectTransactions +
				", upgradeDatabaseVersionWhenConnectedWithPeers=" + upgradeDatabaseVersionWhenConnectedWithPeers +
				", upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion=" + upgradeDatabaseVersionWhenConnectedWithCentralDatabaseVersion +
				", hasToRemoveOldDatabase=" + hasToRemoveOldDatabase +
				'}';
	}

	@Test(dependsOnMethods = "testSynchroAfterTestsBetweenThreePeers")
	public void testAction() throws Exception
	{
		System.out.println(this);

		doAction();
	}
	public abstract void doAction() throws Exception;



}
