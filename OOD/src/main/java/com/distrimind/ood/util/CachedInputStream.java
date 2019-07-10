
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
package com.distrimind.ood.util;

import com.distrimind.util.io.RandomByteArrayInputStream;
import com.distrimind.util.io.RandomFileInputStream;
import com.distrimind.util.io.RandomInputStream;

import java.io.File;
import java.io.IOException;

/**
 * 
 * 
 * @author Jason Mahdjoub
 * @version 2.0
 * @since OOD 2.0.0
 */
public class CachedInputStream extends RandomInputStream {
	
	private byte[] data;
	private final File tmpFile;
	private RandomByteArrayInputStream bais;
	private RandomFileInputStream fis;
	
	CachedInputStream(byte[] data) throws IOException
	{
		if (data==null)
			throw new NullPointerException();
		this.data=data;
		this.tmpFile=null;
		reset();
	}
	CachedInputStream(File tmpFile) throws IOException
	{
		if (tmpFile==null)
			throw new NullPointerException();
		if (!tmpFile.exists())
			throw new IllegalArgumentException();
		this.data=null;
		this.tmpFile=tmpFile;
		reset();
	}

	@Override
	public long length() throws IOException {
		return data==null?fis.length():data.length;
	}

	@Override
	public void seek(long _pos) throws IOException {
		if (data==null)
			fis.seek(_pos);
		else
			bais.seek(_pos);
	}

	@Override
	public long currentPosition() throws IOException {
		if (data==null)
			return fis.currentPosition();
		else
			return bais.currentPosition();
	}

	@Override
	public void reset() throws IOException
	{
		close();
		if (data==null)
		{
			fis=new RandomFileInputStream(tmpFile);
		}
		else
		{
			bais=new RandomByteArrayInputStream(data);
		}
	}

	@Override
	public boolean isClosed() {
		if (data==null)
			return fis.isClosed();
		else
			return bais.isClosed();
	}

	@Override
	public void readFully(byte[] tab, int off, int len) throws IOException {
		if (data==null)
			fis.readFully(tab, off, len );
		else
			bais.readFully(tab, off, len );

	}

	@Override
	public int skipBytes(int n) throws IOException {
		if (data==null)
			return fis.skipBytes(n);
		else
			return bais.skipBytes(n);
	}

	@Deprecated
	@Override
	public String readLine() throws IOException {
		if (data==null)
			return fis.readLine();
		else
			return bais.readLine();
	}

	@Override
	public int read() throws IOException {
		if (fis!=null)
			return fis.read();
		else
			return bais.read();
	}
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (fis!=null)
			return fis.read(b, off, len);
		else
			return bais.read(b, off, len);
	}
	@Override
	public void close() throws IOException {
		if (data==null)
		{
			if (fis!=null)
			{
				fis.close();
				tmpFile.deleteOnExit();
				fis=null;
			}
		}
		else
		{
			if (bais!=null)
			{
				bais.close();
				bais=null;
				data=null;
			}
		}
	}
}
