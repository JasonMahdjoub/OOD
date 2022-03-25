package com.distrimind.ood.database.tests;
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

import com.distrimind.util.FileTools;
import com.distrimind.util.UtilClassLoader;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.sql.*;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 2.3.5
 */

public class H2TestAutoIncrement {
	@Test
	public void testAutoIncrement() throws ClassNotFoundException, SQLException {
		UtilClassLoader.getLoader().loadClass("org.h2.Driver");
		File databaseFile=new File("tmpH2Dir/h2");
		FileTools.deleteDirectory(databaseFile.getParentFile());
		databaseFile.getParentFile().mkdir();
		Connection c=null;
		try {
			c= DriverManager
				.getConnection("jdbc:h2:file:" + databaseFile.getAbsolutePath(), "SA", "");
			Statement s=c.createStatement();
			s.executeUpdate("CREATE  TABLE T11__(FLOATNUMBER_VALUE DOUBLE NOT NULL, PK1 INTEGER NOT NULL, PK2 BIGINT AUTO_INCREMENT(1,1) NOT NULL, CONSTRAINT T11____PK PRIMARY KEY(PK1, PK2));");
			s.close();
			PreparedStatement ps=c.prepareStatement("INSERT INTO T11__(FLOATNUMBER_VALUE, PK1) VALUES('20', '21');", Statement.RETURN_GENERATED_KEYS );
			Assert.assertEquals(ps.executeUpdate(), 1);
			ResultSet r=ps.getGeneratedKeys();
			Assert.assertTrue(r.next());
			Assert.assertEquals(r.getMetaData().getColumnCount(), 2 );
			Assert.assertEquals(r.getLong(2), 1);
			ps.close();

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (c!=null) {
				c.close();
				FileTools.deleteDirectory(databaseFile.getParentFile());
			}

		}
	}
}
