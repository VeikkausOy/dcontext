package fi.veikkaus.dcontext;

import java.io.*;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * Created by arau on 27.6.2016.
 */
public class DynamicClassLoader extends ClassLoader {

    private ArrayList<ReloadableClassLoader> loaders = new ArrayList<>();

    // class name => LoadedClass
    private HashMap<String, LoadedClass> loadedClasses = new HashMap<>();

    public DynamicClassLoader() {
        this(Thread.currentThread().getContextClassLoader());
    }

    /**
     * @param parentClassLoader
     *            the parent of the class loader that loads all the dynamic
     *            classes
     */
    public DynamicClassLoader(ClassLoader parentClassLoader) {
        super(parentClassLoader);
    }


    /**
     * Add a directory that contains the source of dynamic java code.
     *
     * @param classPaths
     * @return true if the add is successful
     */
    public boolean addLoader(File[] classPaths) throws ClassNotFoundException {
        File[] paths = new File[classPaths.length];
        try {
            for (int i = 0; i < classPaths.length; ++i) {
                paths[i] = classPaths[i].getCanonicalFile();
            }
        } catch (IOException e) {
            // ignore
        }

        ReloadableClassLoader src;
        synchronized (loaders) {
            src = new ReloadableClassLoader(paths, DynamicClassLoader.this);
            loaders.add(src);
//            info("add class paths " + paths);

//            loadClassesRecursively(loader);
        }
        reload(src);

        return true;
    }

    // quick and dirty
    private static String pairName(String pkg, String name) {
        return (pkg == null ? name : pkg + "." + name);
    }
    private void loadClassesRecursively(File dir, String pkg) throws ClassNotFoundException {
        for (File f : dir.listFiles()) {
            if (f.isDirectory()) {
                loadClassesRecursively(f, pairName(pkg, f.getName()));
            }
            if (f.isFile() && f.getName().endsWith(".class")) {
                System.out.println("loading " + f.getName());
                loadClass(pairName(pkg, f.getName().substring(0, f.getName().length()-".class".length())));
            }
        }
    }
    public void loadClassesRecursively(File dir) throws ClassNotFoundException {
        loadClassesRecursively(dir, null);
    }

    public HashMap<String, LoadedClass> loadedClasses() {
        return loadedClasses;
    }

    /**
     * Returns the up-to-date dynamic class by name.
     *
     * @param className
     * @return
     * @throws ClassNotFoundException
     *             if source file not found or compilation error
     */
    public Class loadClass(String className) throws ClassNotFoundException {

        LoadedClass loadedClass = null;
        synchronized (loadedClasses) {
            loadedClass = (LoadedClass) loadedClasses.get(className);
        }
        String resource = className.replace('.', '/') + ".class";
        Location l = locateResource(resource);

        // first access of a class
        if (loadedClass == null) {
            if (l == null) {
                info("delegating loading " + className + " to parent");
                return getParent().loadClass(className);
            } else {

                synchronized (this) {

                    // compile and load class
                    loadedClass = new LoadedClass(className, l.LOADER);

                    synchronized (loadedClasses) {
                        loadedClasses.put(className, loadedClass);
                    }
                }

                return loadedClass.clazz;
            }
        }

        // subsequent access
        if (hasChanged(l.LOADER)) {
            info("reloading " + className);
            // unload and load again
            unload(loadedClass.loader);
            reload(loadedClass.loader); // FIXME: expensive

            return loadClass(className);
        }

        return loadedClass.clazz;
    }

    private class Location {
        final ReloadableClassLoader LOADER;
        final File PATH;
        Location(ReloadableClassLoader loader, File path) {
            LOADER = loader;
            PATH = path;
        }
    }

    private Location locateResource(String resource) {
        for (int i = 0; i < loaders.size(); i++) {
            ReloadableClassLoader src = (ReloadableClassLoader) loaders.get(i);
            for (File dir : src.classPaths) {
                if (new File(dir, resource).exists()) {
                    return new Location(src, dir);
                }
            }
        }
        return null;
    }

    public boolean hasChanged(ReloadableClassLoader l) {
        for (Map.Entry<String, LoadedClass> e : loadedClasses.entrySet()) {
            if (e.getValue().loader == l && e.getValue().isChanged()) return true;
        }
        return false;
    }

    private void unload(ReloadableClassLoader src) {
        // clear loaded classes
        synchronized (loadedClasses) {
            for (Iterator iter = loadedClasses.values().iterator(); iter.hasNext();) {
                LoadedClass loadedClass = (LoadedClass) iter.next();
                if (loadedClass.loader == src) {
                    iter.remove();
                }
            }
        }

        // create new class loader
        src.reset();
    }

    private void reload(ReloadableClassLoader src) throws ClassNotFoundException {
        for (File f : src.classPaths) {
            loadClassesRecursively(f);
        }
    }

    public void reloadAll() throws ClassNotFoundException {
        for (ReloadableClassLoader loader : loaders) {
            unload(loader);
            reload(loader);
        }
    }

    /**
     * Get a resource from added source directories.
     *
     * @param resource
     * @return the resource URL, or null if resource not found
     */
    public URL getResource(String resource) {
        try {

            Location l = locateResource(resource);
            return l == null ?
                    getParent().getResource(resource):
                    new File(l.PATH, resource).toURL();

        } catch (MalformedURLException e) {
            // should not happen
            return null;
        }
    }

    public Enumeration<URL> getResources(String resource) throws IOException {
        try {
            List<URL> rv = new ArrayList<>();
            Location l = locateResource(resource);
            if (l != null) rv.add(l.PATH.toURL());
            Enumeration<URL> urls = getParent().getResources(resource);
            while (urls.hasMoreElements()) {
                rv.add(urls.nextElement());
            }
            return Collections.enumeration(rv);
        } catch (MalformedURLException e) {
            // should not happen
            return null;
        }
    }

    /**
     * Get a resource stream from added source directories.
     *
     * @param resource
     * @return the resource stream, or null if resource not found
     */
    public InputStream getResourceAsStream(String resource) {
        try {

            Location l = locateResource(resource);
            return l == null ?
                    getParent().getResourceAsStream(resource) :
                    new FileInputStream(new File(
                        l.PATH, resource));

        } catch (FileNotFoundException e) {
            // should not happen
            return null;
        }
    }

    /**
     * Create a proxy instance that implements the specified access interface
     * and delegates incoming invocations to the specified dynamic
     * implementation. The dynamic implementation may change at run-time, and
     * the proxy will always delegates to the up-to-date implementation.
     *
     * @param interfaceClass
     *            the access interface
     * @param implClassName
     *            the backend dynamic implementation
     * @return
     * @throws RuntimeException
     *             if an instance cannot be created, because of class not found
     *             for example
     */
    public <T> T newProxyInstance(Class<T> interfaceClass, String implClassName)
            throws RuntimeException {
        MyInvocationHandler handler = new MyInvocationHandler(
                implClassName);
        return (T) Proxy.newProxyInstance(this,
                new Class[] { interfaceClass }, handler);
    }

    private static class LoadedClass {
        String className;

        ReloadableClassLoader loader;

        File classFile;

        Class clazz;

        long lastModified;

        LoadedClass(String className, ReloadableClassLoader src) {
            this.className = className;
            this.loader = src;

            String path = className.replace('.', '/');
            // Assumption, file exists only in one directory!
            for (File dir : src.classPaths) {
                File f = new File(dir, path + ".class");
                if (f.exists()) {
                    this.classFile = f;
                }
            }

            loadClass();
        }

        boolean isChanged() {
            return classFile.lastModified() != lastModified;
        }

        void loadClass() {

            if (clazz != null) {
                return; // class already loaded
            }

            try {
                long before = System.currentTimeMillis();
                info("loading binary file " + classFile + " exist " + classFile.exists());

                // load class
                clazz = loader.classLoader.loadClass(className);

                // load class success, remember timestamp
                lastModified = classFile.lastModified();

                info("took " + (System.currentTimeMillis()-before + " ms"));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to load DynaCode class "
                        + classFile.getAbsolutePath());
            }

            info("Init " + clazz);
        }
    }

    private class MyInvocationHandler implements InvocationHandler {

        String backendClassName;

        Object backend;

        MyInvocationHandler(String className) {
            backendClassName = className;

            try {
                Class clz = loadClass(backendClassName);
                backend = newDynaCodeInstance(clz);

            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {

            // check if class has been updated
            Class clz = loadClass(backendClassName);
            if (clz != null && backend.getClass() != clz) {
                info("updating instance");
                if (backend instanceof Closeable) {
                    ((Closeable)backend).close();
                }
                backend = newDynaCodeInstance(clz);
            }

            try {
                // invoke on backend
                return method.invoke(backend, args);

            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }

        private Object newDynaCodeInstance(Class clz) {
            try {
                return clz.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to new instance of DynaCode class "
                                + clz.getName(), e);
            }
        }

    }

    /**
     * Extracts a classpath string from a given class loader. Recognizes only
     * URLClassLoader.
     */
    private static String extractClasspath(ClassLoader cl) {
        StringBuffer buf = new StringBuffer();

        while (cl != null) {
            if (cl instanceof URLClassLoader) {
                URL urls[] = ((URLClassLoader) cl).getURLs();
                for (int i = 0; i < urls.length; i++) {
                    if (buf.length() > 0) {
                        buf.append(File.pathSeparatorChar);
                    }
                    buf.append(urls[i].getFile().toString());
                }
            }
            cl = cl.getParent();
        }

        return buf.toString();
    }

    /**
     * Log a message.
     */
    private static void info(String msg) {
        System.out.println("[ContextServer] " + msg);
    }

}
