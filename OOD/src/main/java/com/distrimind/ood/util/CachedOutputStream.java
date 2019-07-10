
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

import com.distrimind.util.RenforcedDecentralizedIDGenerator;
import com.distrimind.util.io.RandomByteArrayOutputStream;
import com.distrimind.util.io.RandomFileOutputStream;
import com.distrimind.util.io.RandomInputStream;
import com.distrimind.util.io.RandomOutputStream;

import java.io.File;
import java.io.IOException;


/**
 * 
 * 
 * @author Jason Mahdjoub
 * @version 2.0
 * @since OOD 2.0.0
 */
public class CachedOutputStream extends RandomOutputStream {
	private final int cacheSize;
	private File tmpFile;
	private RandomByteArrayOutputStream baos;
	private RandomFileOutputStream fos;
	private long writedData;
	private boolean removeFile=true;
	
	public CachedOutputStream(int cacheSize)
	{
		if (cacheSize<0)
			throw new IllegalArgumentException();
		this.cacheSize=cacheSize;
		this.tmpFile=null;
		baos=new RandomByteArrayOutputStream(cacheSize);
		fos=null;
		writedData=0;
	}
	public static File getNewTmpFile() throws IOException
	{
		RenforcedDecentralizedIDGenerator id=new RenforcedDecentralizedIDGenerator();
		return File.createTempFile("oodSynchroTmpFile."+id.getTimeStamp()+"."+id.getWorkerIDAndSequence(), "data");
	}

	@Override
	public void write(int b) throws IOException {
		if (writedData<cacheSize)
		{
			baos.write(b);
		}
		else if (writedData==cacheSize)
		{
			if (tmpFile==null)
				tmpFile=getNewTmpFile();
			fos=new RandomFileOutputStream(tmpFile);
			fos.write(baos.getBytes());
			fos.write(b);
			baos=null;
		}
		else
			fos.write(b);
		++writedData;
	}
	
	@Override
	public void write(byte[] b, int off, int len) throws IOException
	{
		if (writedData+len<cacheSize)
		{
			baos.write(b, off, len);
		}
		else if (writedData<cacheSize)
		{
			if (tmpFile==null)
				tmpFile=getNewTmpFile();
			fos=new RandomFileOutputStream(tmpFile);
			if (writedData>0)
				fos.write(baos.getBytes());
			fos.write(b, off, len);
			baos=null;
		}
		else
			fos.write(b, off, len);
		writedData+=len;
	}
	
	public CachedInputStream getCachedInputStream() throws IOException
	{
		try
		{
			removeFile=false;
			if (writedData<=cacheSize)
				return new CachedInputStream(baos.getBytes());
			else
				return new CachedInputStream(tmpFile);
		}
		finally
		{
			close();
		}
	}
	
	@Override
	public void close() throws IOException
	{
		if (baos!=null)
		{
			baos.close();
			baos=null;
			if (removeFile && tmpFile!=null)
				tmpFile.deleteOnExit();
		}
		else if (fos!=null)
		{
			fos.close();
			fos=null;
			if (removeFile && tmpFile!=null)
				tmpFile.deleteOnExit();
		}
	}

	@Override
	public long length() throws IOException {
		if (baos==null)
			return fos.length();
		else
			return baos.length();
	}

	@Override
	public void setLength(long newLength) throws IOException {
		if (baos==null)
			fos.setLength(newLength);
		else
			baos.setLength(newLength);
	}

	@Override
	public void seek(long _pos) throws IOException {
		if (baos==null)
			fos.seek(_pos);
		else
			baos.seek(_pos);
	}

	@Override
	public long currentPosition() throws IOException {
		if (baos==null)
			return fos.currentPosition();
		else
			return baos.currentPosition();
	}

	@Override
	public boolean isClosed() {
		if (baos==null)
			return fos.isClosed();
		else
			return baos.isClosed();
	}

	@Override
	protected RandomInputStream getRandomInputStreamImpl() throws IOException {
		if (baos==null)
			return fos.getRandomInputStream();
		else
			return baos.getRandomInputStream();
	}
}
