package com.distrimind.ood;

import android.content.Context;
import android.os.Environment;
import android.os.StatFs;
import android.util.Log;

import com.distrimind.util.harddrive.AndroidHardDriveDetect;
import com.distrimind.util.harddrive.HardDriveDetect;
import com.distrimind.util.harddrive.Partition;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.NetworkInterface;
import java.nio.file.Files;

import androidx.core.content.ContextCompat;
import androidx.test.platform.app.InstrumentationRegistry;

public class StorageList {


    @Test
    public void testStorageList2()
    {
        Context context=InstrumentationRegistry.getInstrumentation().getTargetContext();
        File internalStorageVolume =
                context.getFilesDir();
        StatFs stat=new StatFs(internalStorageVolume.getPath());
        Log.i("liststorages", "Internal storage : "+internalStorageVolume+" "+Files.isWritable(internalStorageVolume.toPath())+" ; freeSpace="+ (stat.getAvailableBytes()));

        File[] externalStorageVolumes =
                ContextCompat.getExternalFilesDirs(context, null);
        for (File f : externalStorageVolumes) {
            stat=new StatFs(f.getPath());
            Log.i("liststorages", "External storage : " + f.toString() + Environment.getExternalStorageState(f) + " ; removable=" + Environment.isExternalStorageRemovable(f) + " ; " + Files.isWritable(f.toPath())+" ; freeSpace="+ (stat.getAvailableBytes()));
        }
    }

    @Test
    public void testStorageList3() throws IOException {
        AndroidHardDriveDetect.context=InstrumentationRegistry.getInstrumentation().getTargetContext();
        for (Partition p : HardDriveDetect.getInstance().getDetectedPartitions())
        {
            Log.i("liststorages", ""+p);
        }
    }
}
