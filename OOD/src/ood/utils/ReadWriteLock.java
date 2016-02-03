package ood.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 
 * @author Jakob Jenkov
 * @author Jason Mahdjoub
 * @version 1.0
 *
 */
public class ReadWriteLock{

    private final Map<Thread, Integer> readingThreads = new HashMap<Thread, Integer>();
    private int writeRequests=0;
    
     private int writeAccesses    = 0;
     private Thread writingThread = null;

     private ReentrantLock lock=new ReentrantLock(true);
     private Condition cannotContinue=lock.newCondition();

     public void tryLockRead() throws InterruptedException{
	 this.tryLockRead(-1, TimeUnit.MILLISECONDS);
     }
     public void tryLockRead(long timeout_ms) throws InterruptedException{
	 this.tryLockRead(timeout_ms, TimeUnit.MILLISECONDS);
     }
    public void tryLockRead(long timeout, TimeUnit unit) throws InterruptedException{
	      try
	      {
		  lock.lock();
		  Thread callingThread = Thread.currentThread();
		  while(! canGrantReadAccess(callingThread)){
			if (timeout<0)
			    cannotContinue.await();
			else
			    cannotContinue.await(timeout, unit);

		  }

		  readingThreads.put(callingThread, new Integer((getReadAccessCount(callingThread) + 1)));
	      }
	      finally
	      {
		  lock.unlock();
	      }
	    }
    
    public void lockRead(){
	while(true)
	{
	    try
	    {
		this.tryLockRead();
		return;
	    }
	    catch(InterruptedException e)
	    {
	    }
	}
    }
    
    public ReadLock getAutoCloseableReadLock()
    {
	return new ReadLock();
    }
    
    public WriteLock getAutoCloseableWriteLock()
    {
	return new WriteLock();
    }
    
    public abstract class Lock implements AutoCloseable
    {
	@Override
	public abstract void close();
    }
    public class ReadLock extends Lock
    {
	protected ReadLock()
	{
	    ReadWriteLock.this.lockRead();
	}
	
	@Override
	public void close()
	{
	    ReadWriteLock.this.unlockRead();
	}
    }
    
    public class WriteLock extends Lock
    {
	protected WriteLock()
	{
	    ReadWriteLock.this.lockWrite();
	}
	
	@Override
	public void close()
	{
	    ReadWriteLock.this.unlockWrite();
	}
    }

    private boolean canGrantReadAccess(Thread callingThread){
      if( isWriter(callingThread) ) return true;
      if( hasWriter()             ) return false;
      if( isReader(callingThread) ) return true;
      if( hasWriteRequests()      ) return false;
      return true;
    }


    public void unlockRead(){
	try
	{
	    lock.lock();
	    Thread callingThread = Thread.currentThread();
	    if(!isReader(callingThread)){
		throw new IllegalMonitorStateException("Calling Thread does not" +
			" hold a read lock on this ReadWriteLock");
	    }
	    int accessCount = getReadAccessCount(callingThread);
	    if(accessCount == 1)
	    {
		readingThreads.remove(callingThread); 
	    }
	    else 
	    { 
		readingThreads.put(callingThread, new Integer((accessCount -1))); 
	    }
	    cannotContinue.signalAll();
	}
	finally
	{
	    lock.unlock();
	}
	
    }

    public int tryLockWrite() throws InterruptedException{
	return this.tryLockWrite(-1, TimeUnit.MILLISECONDS);
    }
    public int tryLockWrite(long timeout_ms) throws InterruptedException{
	return this.tryLockWrite(timeout_ms, TimeUnit.MILLISECONDS);
    }
    public int tryLockWrite(long timeout, TimeUnit unit) throws InterruptedException{
	Thread callingThread = Thread.currentThread();
	try
	{
	    lock.lock(); 
	    ++writeRequests;
	    while(! canGrantWriteAccess(callingThread)){
		if (timeout<0)
		    cannotContinue.await();
		else
		    cannotContinue.await(timeout, unit);
	    }
	    --writeRequests;
	    writingThread = callingThread;
	    return ++writeAccesses;
	}
	catch(InterruptedException e)
	{
	    --writeRequests;
	    cannotContinue.signalAll();
	    throw e;
	}
	finally
	{
	    lock.unlock();
	}
    }

    @Override
    public String toString()
    {
	try
	{
	    lock.lock();
	    return "Locker["+(writingThread==null?(writeAccesses+" Write"):(writeAccesses+" x "+writingThread.toString()))+","+(readingThreads.size()==0?"NoReads":(readingThreads.size()+" Reads"))+"]";
	}
	finally
	{
	    lock.unlock();
	}
    }
    
    public int lockWrite(){
	while(true)
	{
	    try
	    {
		return this.tryLockWrite();
		
	    }
	    catch(InterruptedException e)
	    {
	    }
	}
    }

    public void unlockWrite(){
	try
	{
	    lock.lock();
	    if(!isWriter(Thread.currentThread())){
		throw new IllegalMonitorStateException("Calling Thread does not hold the write lock on this ReadWriteLock");
	    }
	    writeAccesses--;
	    if(writeAccesses == 0){
		writingThread = null;
	    }
	    if (writeAccesses<0)
		throw new IllegalMonitorStateException("The number of  unlock is greater than the number of locks");
	    
	    cannotContinue.signalAll();
	}
	finally
	{
	    lock.unlock();
	}
	    
    }
    public void unlockWriteAndLockRead()
    {
	try
	{
	    lock.lock();
	    unlockWrite();
	    lockRead();
	}
	finally
	{
	    lock.unlock();
	}
    }
    public void unlockReadAndLockWrite()
    {
	try
	{
	    lock.lock();
	    unlockRead();
	    lockWrite();
	}
	finally
	{
	    lock.unlock();
	}
    }

    private boolean canGrantWriteAccess(Thread callingThread){
      if (isWriter(callingThread)) return true;
      if(isOnlyReader(callingThread) && writingThread==null)    return true;
      if(hasReaders()) return false;
      if(writingThread == null)          return true;
      return false;
    }
    

    private int getReadAccessCount(Thread callingThread){
      Integer accessCount = readingThreads.get(callingThread);
      if(accessCount == null) 
	  return 0;
      return accessCount.intValue();
    }


    private boolean hasReaders(){
      return readingThreads.size() > 0;
    }

    private boolean isReader(Thread callingThread){
      return readingThreads.get(callingThread) != null;
    }

    private boolean isOnlyReader(Thread callingThread){
      return readingThreads.size() == 1 &&
             readingThreads.get(callingThread) != null;
    }

    private boolean hasWriter(){
      return writingThread != null;
    }

    private boolean isWriter(Thread callingThread){
      return writingThread == callingThread;
    }

    private boolean hasWriteRequests(){
        return this.writeRequests > 0;
    }

  }
