package com.distrimind.ood;

import android.content.Context;
import android.util.Log;

import com.distrimind.util.OSVersion;

import androidx.test.platform.app.InstrumentationRegistry;
import androidx.test.ext.junit.runners.AndroidJUnit4;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import static org.junit.Assert.*;

/**
 * Instrumented test, which will execute on an Android device.
 *
 * @see <a href="http://d.android.com/tools/testing">Testing documentation</a>
 */
@RunWith(AndroidJUnit4.class)
public class ExampleInstrumentedTest {
    @Test
    public void useAppContext() throws ClassNotFoundException, SocketException {
        // Context of the app under test.
        Log.i("perso", ""+OSVersion.getCurrentOSVersion().getOS());

        Context appContext = InstrumentationRegistry.getInstrumentation().getTargetContext();

        //assertEquals("com.distrimind.ood_android_driver.test", appContext.getPackageName());
    }
}
