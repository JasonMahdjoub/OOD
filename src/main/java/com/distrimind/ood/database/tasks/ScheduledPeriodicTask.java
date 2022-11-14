package com.distrimind.ood.database.tasks;
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

import com.distrimind.util.io.Integrity;
import com.distrimind.util.io.MessageExternalizationException;
import com.distrimind.util.io.SecuredObjectInputStream;
import com.distrimind.util.io.SecuredObjectOutputStream;

import java.io.IOException;
import java.time.*;

/**
 * @author Jason Mahdjoub
 * @version 1.0
 * @since OOD 3.2.0
 */
public final class ScheduledPeriodicTask extends AbstractScheduledTask {
	public static final long MIN_PERIOD_IN_MS=60L*60L*1000L;
	private long endTimeUTCInMs;
	private long periodInMs;
	private DayOfWeek dayOfWeek;
	private byte hour;
	private byte minute;

	ScheduledPeriodicTask(Class<? extends ITaskStrategy> strategyClass,
								 long periodInMs, byte dayOfWeek, byte hour, byte minute,
								 long endTimeUTCInMs) {
		this(strategyClass, periodInMs, dayOfWeek==-1?null:DayOfWeek.of(dayOfWeek), hour, minute, endTimeUTCInMs);
	}
	public ScheduledPeriodicTask(Class<? extends IDatabaseTaskStrategy> strategyClass,
								 byte hour, byte minute,DayOfWeek dayOfWeek,
								 long endTimeUTCInMs) {
		this(strategyClass, -1, dayOfWeek, hour, minute, endTimeUTCInMs);
	}
	public ScheduledPeriodicTask(Class<? extends IDatabaseTaskStrategy> strategyClass,
						  long periodInMs, byte hour, byte minute,DayOfWeek dayOfWeek,
						  long endTimeUTCInMs) {
		this(strategyClass, periodInMs, dayOfWeek, hour, minute, endTimeUTCInMs);
	}
	ScheduledPeriodicTask(Class<? extends ITaskStrategy> strategyClass,
								 long periodInMs, DayOfWeek dayOfWeek, byte hour, byte minute,
								 long endTimeUTCInMs) {
		super(strategyClass);
		if (periodInMs==-1 && hour==-1 && minute==-1 && dayOfWeek==null)
			throw new NullPointerException();
		if (hour!=-1 && (hour<0 || hour>23))
			throw new IllegalArgumentException("hour="+hour);
		if (minute!=-1 && (minute<0 || minute>59))
			throw new IllegalArgumentException("minute="+minute);
		if (periodInMs!=-1 && periodInMs<=MIN_PERIOD_IN_MS)
			throw new IllegalArgumentException("periodInMs="+periodInMs);
		this.periodInMs = periodInMs;
		this.dayOfWeek=dayOfWeek;
		this.hour=hour;
		this.minute=minute;
		this.endTimeUTCInMs=endTimeUTCInMs;
	}
	public ScheduledPeriodicTask(Class<? extends IDatabaseTaskStrategy> strategyClass,
								 byte hour, byte minute, DayOfWeek dayOfWeek) {
		this(strategyClass, -1, dayOfWeek, hour, minute, Long.MAX_VALUE);
	}
	public ScheduledPeriodicTask(Class<? extends IDatabaseTaskStrategy> strategyClass,
								 long periodInMs, byte hour, byte minute, DayOfWeek dayOfWeek) {
		this(strategyClass, periodInMs, dayOfWeek, hour, minute, Long.MAX_VALUE);
	}
	public ScheduledPeriodicTask(Class<? extends IDatabaseTaskStrategy> strategyClass, long periodInMs) {
		this(strategyClass, periodInMs, (byte)-1, (byte)-1, null);
	}

	public long getPeriodInMs() {
		return periodInMs;
	}

	public long getEndTimeUTCInMs() {
		return endTimeUTCInMs;
	}

	@Override
	public int getInternalSerializedSize() {
		return super.getInternalSerializedSize()+16;
	}

	@Override
	public void writeExternal(SecuredObjectOutputStream out) throws IOException {
		super.writeExternal(out);
		out.writeLong(endTimeUTCInMs);
		out.writeLong(periodInMs);
		out.writeEnum(dayOfWeek, true);
		out.writeByte(hour);
		out.writeByte(minute);
	}

	@Override
	public void readExternal(SecuredObjectInputStream in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		endTimeUTCInMs=in.readLong();
		periodInMs=in.readLong();
		dayOfWeek=in.readEnum(true);
		hour=in.readByte();
		minute=in.readByte();
		if (periodInMs==-1 && hour==-1 && minute==-1 && dayOfWeek==null)
			throw new MessageExternalizationException(Integrity.FAIL);
		if (hour!=-1 && (hour<0 || hour>23))
			throw new MessageExternalizationException(Integrity.FAIL);
		if (minute!=-1 && (minute<0 || minute>59))
			throw new MessageExternalizationException(Integrity.FAIL);
		if (periodInMs!=-1 && periodInMs<=MIN_PERIOD_IN_MS)
			throw new MessageExternalizationException(Integrity.FAIL);
	}
	public long getNextOccurrenceInTimeUTCAfter(long timeUTCInMs)
	{
		return getNextOccurrenceInTimeUTCAfter(timeUTCInMs, true);
	}
	private long getNextOccurrenceInTimeUTCAfter(long timeUTCInMs, boolean takeCurrentTimeMillisAsMinimumTime)
	{
		LocalDateTime ld= LocalDateTime.ofInstant(Instant.ofEpochMilli(timeUTCInMs), ZoneId.systemDefault());
		if (periodInMs!=-1)
			ld=ld.plus(Duration.ofMillis(periodInMs));
		if (minute!=-1)
		{
			if (periodInMs==-1 && hour==-1 && dayOfWeek==null)
				ld.plus(Duration.ofHours(1));
			int d=minute-ld.getMinute();
			if (d>0)
				ld=ld.plus(Duration.ofMinutes(d));
			else if (d<0)
				ld=ld.plus(Duration.ofMinutes(60-ld.getMinute()+minute));
		}
		if (hour!=-1)
		{
			if (periodInMs==-1 && minute==-1 && dayOfWeek==null)
				ld.plus(Duration.ofHours(24));
			int d=hour-ld.getHour();
			if (d>0)
				ld=ld.plus(Duration.ofHours(d));
			else if (d<0)
				ld=ld.plus(Duration.ofHours(24-ld.getHour()+hour));
		}
		if (dayOfWeek!=null)
		{
			if (periodInMs==-1)
				ld=ld.plus(Duration.ofDays(7));
			DayOfWeek dow=ld.getDayOfWeek();
			if (dow!=dayOfWeek)
			{
				int d=dayOfWeek.ordinal()-dow.ordinal();
				if (d>0)
					ld=ld.plus(Duration.ofDays(d));
				else if (d<0)
					ld=ld.plus(Duration.ofDays(7-dow.ordinal()+dayOfWeek.ordinal()));
			}
		}
		long res=ld.toEpochSecond(ZoneOffset.UTC);
		if (takeCurrentTimeMillisAsMinimumTime) {
			if ((res - timeUTCInMs) < ((System.currentTimeMillis() - timeUTCInMs) / 2))
				return getNextOccurrenceInTimeUTCAfter(System.currentTimeMillis(), false);
		}
		return res;
	}

	public DayOfWeek getDayOfWeek() {
		return dayOfWeek;
	}

	public byte getHour() {
		return hour;
	}

	public byte getMinute() {
		return minute;
	}
}
