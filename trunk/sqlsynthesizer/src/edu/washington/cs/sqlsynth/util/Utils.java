package edu.washington.cs.sqlsynth.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class Utils {
	
	public static boolean VERBOSE = true;
	
	public static void checkTrue(boolean cond) {
		checkTrue(cond, "");
	}
	
	public static void checkTrue(boolean cond, String msg) {
		if(!cond) {
			throw new RuntimeException(msg);
		}
	}
	
	public static String translateSlashToDot(String str) {
		assert str != null;
		return str.replace('/', '.');
	}
	
	public static String translateDotToSlash(String str) {
		assert str != null;
		return str.replace('.', '/');
	}
	
	public static boolean isInteger(Object v) {
		try {
		   Integer.parseInt(v+"");
		   return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}
	
	public static Integer convertToInteger(String v) {
		try {
			double d = Double.parseDouble(v);
			return (int)d;
		} catch (NumberFormatException e) {
			throw new Error("not a numeric: " + v);
		}
	}
	
	public static void checkDirExistence(String dir) {
		File f = new File(dir);
		if(!f.isDirectory()) {
			throw new RuntimeException("File: " + f + " is not a dir");
		}
		if(!f.exists()) {
			throw new RuntimeException("Dir: " + f + " does not exist");
		}
	}
	
	public static void checkFileExistence(String dir) {
		File f = new File(dir);
		if(f.isDirectory()) {
			throw new RuntimeException("File: " + f + " is  a dir");
		}
		if(!f.exists()) {
			throw new RuntimeException("File: " + f + " does not exist");
		}
	}
	
	public static <T> void checkNoNull(T[] ts) {
		for(int i = 0; i < ts.length; i++) {
			if(ts[i] == null) {
				throw new RuntimeException("The " + i + "-th element is null.");
			}
		}
	}
	
	public static <T> void checkNotNull(T t) {
		if (t == null) {
			throw new RuntimeException("The provided value is null.");
		}
	}
	
	public static void checkPathEntryExistence(String path) {
		String[] entries = path.split(Globals.pathSep);
		for(String entry : entries) {
			File f = new File(entry);
			if(!f.exists()) {
				throw new RuntimeException("The entry: " + entry + " does not exist.");
			}
		}
	}
	
	//must wrap in a try - catch, since this will be used in a field initializer
	public static List<String> getClassesRecursive(String dir) {
		try {
			List<String> fileNames = new LinkedList<String>();
			for (File f : Files.getFileListing(new File(dir), ".class")) {
				fileNames.add(f.getAbsolutePath());
			}
			return fileNames;
		} catch (Throwable e) {
			throw new Error(e);
		}
	}
	
	//find all class files
	public static List<String> getJars(String dir, boolean recursive) throws FileNotFoundException {
		if(!recursive) {
			return getJars(dir);
		} else {
			List<String> fileNames = new LinkedList<String>();
			for(File f : Files.getFileListing(new File(dir), ".jar") ) {
				fileNames.add(f.getAbsolutePath());
			}
			return fileNames;
		}
	}
	
	//find all jar files, not this is not recursive
	public static List<String> getJars(String dir) {
		List<String> files = Files.findFilesInDir(dir, null, ".jar");
		List<String> fullPaths = new LinkedList<String>();
		for(String file : files) {
			fullPaths.add(dir + Globals.fileSep + file);
		}
		//System.out.println(fullPaths);
		return fullPaths;
	}
	
	public static Collection<String> extractClassFromPlugXMLFiles(String...fileNames) {
		Collection<String> classNames = new HashSet<String>();
		
		for(String fileName : fileNames) {
			if(!fileName.endsWith(".xml")) {
				throw new RuntimeException("The file is not an XML file: " + fileName);
			}
			String content = Files.readWholeAsString(fileName);
			Collection<String> classes = extractClasses(content);
			classNames.addAll(classes);
		}
		
		return classNames;
	}
	
	public static Collection<String> extractClassFromPluginXML(String pluginJarFile) throws IOException {
		if(!pluginJarFile.endsWith(".jar")) {
			throw new RuntimeException("The input file: " + pluginJarFile + " is not a jar file.");
		}
		String content = getPluginXMLContent(pluginJarFile);
		if(content != null) {
			return extractClasses(content);
		} else {
		    return Collections.<String>emptySet(); 
		}
	}
	
	//be aware, this can return null
	public static String getPluginXMLContent(String jarFilePath) throws IOException {
		ZipFile jarFile = new ZipFile(jarFilePath);
		ZipEntry entry = jarFile.getEntry("plugin.xml");
		if(entry == null) {
			return null;
		}
		BufferedReader in = new BufferedReader(
				new InputStreamReader(jarFile.getInputStream(entry)));
		StringBuilder sb = new StringBuilder();
		String line = in.readLine();
		while(line != null) {
		    sb.append(line);
		    sb.append(Globals.lineSep);
		    line = in.readLine();
		}
		return sb.toString();
	}
	
	public static Collection<String> extractClasses(String xmlContent) {
		final Set<String> classList = new LinkedHashSet<String>();
		try {
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
			DefaultHandler handler = new DefaultHandler() {
				public void startElement(String uri, String localName,
						String qName, Attributes attributes) throws SAXException {
					if(attributes != null) {
					    for(int i = 0; i < attributes.getLength(); i++) {
						    if(attributes.getQName(i).equals("class")) {
							    classList.add(attributes.getValue(i));
						    }
					    }
					}
				}
			};
			byte[] bytes = xmlContent.getBytes("UTF8");
			InputStream inputStream = new ByteArrayInputStream(bytes);
			InputSource source = new InputSource(inputStream);
			saxParser.parse(source, handler);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return classList;
	}
	
	public static String concatenate(Iterable<String> strs, String sep) {
		StringBuilder sb = new StringBuilder();
		int count = 0;
		for(String str : strs) {
			if(count != 0) {
				sb.append(sep);
			}
			sb.append(str);
			count++;
		}
		return sb.toString();
	}
	
	public static String concatenate(String[] strs, String sep) {
		StringBuilder sb = new StringBuilder();
		int count = 0;
		for(String str : strs) {
			if(count != 0) {
				sb.append(sep);
			}
			sb.append(str);
			count++;
		}
		return sb.toString();
	}
	
	public static String conToPath(List<String> strs) {
		StringBuilder sb = new StringBuilder();
		int count = 0;
		for(String str : strs) {
			if(count != 0) {
				sb.append(Globals.pathSep);
			}
			sb.append(str);
			count++;
		}
		return sb.toString();
	}
	
	public static <T> boolean hasOverlap(Set<T> s1, Set<T> s2) {
		Utils.checkNotNull(s1);
		Utils.checkNotNull(s2);
		for(T t : s1) {
			Utils.checkNotNull(t);
			if(s2.contains(t)) {
				return true;
			}
		}
		return false;
	}
	
	public static <T> boolean includedIn(T target, T[] array) {
		if(target == null) {
			throw new RuntimeException("target can not be null.");
		}
		for(T elem : array) {
			if(elem != null && elem.equals(target)) {
				return true;
			}
		}
		return false;
 	}
	
	public static boolean containIn(String dest, String[] strs) {
		for(String str : strs) {
			if(dest.indexOf(str) != -1) {
				return true;
			}
		}
		return false;
	}
	
	public static <T> T getFirst(Collection<T> coll) {
		List<T> list = new LinkedList<T>();
		list.addAll(coll);
		return list.get(0);
	}
	
	public static <T> Collection<T> iterableToCollection(Iterable<T> ts) {
		Collection<T> collection = new LinkedList<T>();
		for(T t : ts) {
			collection.add(t);
		}
		return collection;
 	}
	
	public static <T> void removeRedundant(Collection<T> coll) {
		Set<T> set = new LinkedHashSet<T>();
		set.addAll(coll);
		coll.clear();
		coll.addAll(set);
	}
	
	public static <T> Iterable<T> returnUniqueIterable(Iterable<T> coll) {
		Set<T> set = new LinkedHashSet<T>();
		for(T t : coll) {
			set.add(t);
		}
		return set;
	}
	
	//check if every element of its is included in all
	public static <T> boolean includedIn(Iterable<T> its, Iterable<T> all) {
		Collection<T> collection_its = iterableToCollection(its);
		Collection<T> collection_all = iterableToCollection(its);
		return collection_all.containsAll(collection_its);
	}
	
	/** This project-specific methods */
	public static <T> int countIterable(Iterable<T> c) {
		int count = 0;
		for(T t: c) {
			count++;
		}
		return count;
	}
	
	public static <T> void logCollection(Iterable<T> c) {
		Log.logln(dumpCollection(c));
	}
	
	public static <T> void dumpCollection(Iterable<T> c, PrintStream out) {
		out.println(dumpCollection(c));
	}
	public static <T> void dumpCollection(Iterable<T> c, String fileName) {
		try {
			Files.writeToFile(dumpCollection(c), fileName);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	public static <T> String dumpCollection(Iterable<T> c) {
		StringBuilder sb = new StringBuilder();
		int num = 0;
		for(T t : c) {
			sb.append(t);
			sb.append(Globals.lineSep);
			num ++;
		}
		sb.append("Num in total: " + num);
		return sb.toString();
	}
	
	public static <K, V> Map<K, V> sortByKey(Map<K, V> map, final boolean increase) {
        List<Entry<K, V>> list = new LinkedList<Entry<K, V>>(map.entrySet());
        Collections.sort(list, new Comparator() {
             public int compare(Object o1, Object o2) {
                     if(increase) {
                             return ((Comparable) ((Map.Entry) (o1)).getKey())
                         .compareTo(((Map.Entry) (o2)).getKey());
                     } else {
                             return ((Comparable) ((Map.Entry) (o2)).getKey())
                         .compareTo(((Map.Entry) (o1)).getKey());
                     }
             }
        });

       Map<K, V> result = new LinkedHashMap<K, V>();
       for (Iterator<Entry<K, V>> it = list.iterator(); it.hasNext();) {
           Map.Entry<K, V> entry = (Map.Entry<K, V>)it.next();
           result.put(entry.getKey(), entry.getValue());
       }
       return result;
   }
   
   public static <K, V> Map<K, V> sortByValue(Map<K, V> map, final boolean increase) {
        List<Entry<K, V>> list = new LinkedList<Entry<K, V>>(map.entrySet());
        Collections.sort(list, new Comparator() {
             public int compare(Object o1, Object o2) {
                     if(increase) {
                             return ((Comparable) ((Map.Entry) (o1)).getValue())
                         .compareTo(((Map.Entry) (o2)).getValue());
                     } else {
                             return ((Comparable) ((Map.Entry) (o2)).getValue())
                         .compareTo(((Map.Entry) (o1)).getValue());
                     }
             }
        });

       Map<K, V> result = new LinkedHashMap<K, V>();
       for (Iterator<Entry<K, V>> it = list.iterator(); it.hasNext();) {
           Map.Entry<K, V> entry = (Map.Entry<K, V>)it.next();
           result.put(entry.getKey(), entry.getValue());
       }
       return result;
   }
}