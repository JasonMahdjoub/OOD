package ood.utils;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ListClasses
{
    private static HashMap<Package, ArrayList<Class<?>>> cache=new HashMap<Package, ArrayList<Class<?>>>();
    
    /**
     * This method enables to list all classes contained into a given package
     * 
     * @param _package the name of the considered package
     * @return the list of classes
     * @throws ClassNotFoundException when a class was not found
     * @throws IOException when an IO Exception occurs
     */
    public static ArrayList<Class<?>> getClasses(Package _package) throws ClassNotFoundException, IOException {
	ArrayList<Class<?>> classes=cache.get(_package);
	if (classes!=null)
	    return classes;
    	// creation of the list which will be returned
    	classes = new ArrayList<Class<?>>();
     
    	// We get all CLASSPATH entries
    	String [] entries = System.getProperty("java.class.path")
    					.split(System.getProperty("path.separator"));
     
    	// For all these entries, we check if they contains a directory, or a jar file
    	for (int i = 0; i < entries.length; i++) {
    	    
    		if(entries[i].endsWith(".jar")){
    		    File jar=new File(entries[i]);
    		    if (jar.isFile())
    			classes.addAll(processJar(jar, _package));
    		}else
    		{
    		    File dir=new File(entries[i]);
    		    if (dir.isDirectory())
    		    {
    			classes.addAll(processDirectory(dir, _package));
    		    }
    			
    		}
     
    	}
    	cache.put(_package, classes);
    	return classes;
    }
     
    private static ArrayList<String> splitPoint(String s)
    {
	ArrayList<String> res=new ArrayList<String>(10);
	int last_index=0;
	for (int i=0;i<s.length();i++)
	{
	    if (s.charAt(i)=='.')
	    {
		if (i!=last_index)
		{
		    res.add(s.substring(last_index, i));
		}
		last_index=i+1;
	    }
	}
	if (s.length()!=last_index)
	{
	    res.add(s.substring(last_index));
	}
	
	return res;
    }
    
    /**
     * This method enables to list all classes contained into a directory for a given package
     * 
     * @param directory the considered directory
     * @param _package_name the package name
     * @return the list of classes
     */
    private static ArrayList<Class<?>> processDirectory(File _directory, Package _package) throws ClassNotFoundException {
    	ArrayList<Class<?>> classes = new ArrayList<Class<?>>();
     
    	// we generate the absolute path of the package
    	ArrayList<String> repsPkg = splitPoint(_package.getName());

    	for (int i = 0; i < repsPkg.size(); i++) {
    	    _directory=new File(_directory, repsPkg.get(i));
    	}
     
    	// if the directory exists and if it is a directory, we list it
    	if(_directory.exists() && _directory.isDirectory()){
    		// we filter the directory entries
    		FilenameFilter filter = new DotClassFilter();
    		File[] liste = _directory.listFiles(filter );
    		// for each element present on the directory, we add it into the classes list.
    		for (int i = 0; i < liste.length; i++) {
    			classes.add(Class.forName(_package.getName() + "." + liste[i].getName().substring(0, liste[i].getName().length()-6)));
    		}
    	}
     
    	return classes;
    }
     
    /**
     * This method enables to list all classes contained into a jar file for a given package
     *
     * @param _jar_path the considered jar file
     * @param _package_name the package name
     * @return the list of classes
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    private static ArrayList<Class<?>> processJar(File _jar_file, Package _package) throws IOException, ClassNotFoundException {
    	ArrayList<Class<?>> classes = new ArrayList<Class<?>>();
     
    	JarFile jfile = new JarFile(_jar_file);
    	String pkgpath = _package.getName().replace(".", "/");

    	// for each jar entry
    	for (Enumeration<JarEntry> entries = jfile.entries(); entries.hasMoreElements();) {
    		JarEntry element = entries.nextElement();
     
    		// if the name begins with the package path and ends with .class
    		if(element.getName().startsWith(pkgpath)
    			&& element.getName().endsWith(".class")){
     
    			String class_name = element.getName().substring(pkgpath.length() + 1, element.getName().length()-6);
     
    			classes.add(Class.forName(_package.getName() + "." + class_name));
         		}
     
    	}
    	jfile.close();
    	return classes;
    }
     
    /**
     * This class enables to filter the files of a directory. It accepts only .class files
     */
    protected static class DotClassFilter implements FilenameFilter{
     
    	public boolean accept(File arg0, String arg1) {
    		return arg1.endsWith(".class");
    	}
     
     
    }
    
    public static void main(String args[])
    {
	File f=new File("/home/jason/misfont.log");
	System.out.println(f.getName());
    }
}
