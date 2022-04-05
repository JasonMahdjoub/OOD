package com.distrimind.ood;

import android.util.Log;

import com.distrimind.util.OSVersion;

import org.junit.Test;

public class AndroidVersion {
    @Test
    public void test()
    {
        Log.i("androidversion", System.getProperty("http.agent").toLowerCase());
        Log.i("androidversion", System.getProperty("os.name").toLowerCase());
        Log.i("androidversion", OSVersion.getCurrentOSVersion()+"");
    }
}
