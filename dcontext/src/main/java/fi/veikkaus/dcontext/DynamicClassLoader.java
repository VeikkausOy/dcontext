package fi.veikkaus.dcontext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

public class DynamicClassLoader extends ClassLoader {

    private static int CLASS_LOADING_RETRIES = 10;
    private static int WAIT_COMPLICATION_MS = 500;// wait 500ms for complication to finish
    private static int RETRY_WAIT_MS = WAIT_COMPLICATION_MS;

    private static Logger logger = LoggerFactory.getLogger(DynamicClassLoader.class);

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
//            debug("add class paths " + paths);

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
                debug("loading " + f.getName());
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
        if (l == null) {
            debug("delegating loading " + className + " to parent");
            return getParent().loadClass(className);
        }

        // NOTE: This is an unsafe
        int retries = 0;
        while (retries < CLASS_LOADING_RETRIES) {
            long timeSinceCodeModified = System.currentTimeMillis() - l.lastModified();

            if (timeSinceCodeModified > WAIT_COMPLICATION_MS) {
                // first access of a class
                if (loadedClass == null) {
                    synchronized (this) {
                   // compile and load class
                        loadedClass = new LoadedClass(className, l.LOADER);

                        synchronized (loadedClasses) {
                            loadedClasses.put(className, loadedClass);
                        }
                    }
                }
                // subsequent access
                if (loadedClass.classFileExists()) { // compilation may be still running
                    if (loadedClass.clazz == null) {
                        loadedClass.loadClass();
                    } else if (hasChanged(l.LOADER)) {
                        debug("reloading " + className);
                        // unload and load again
                        unload(loadedClass.loader);
                        reload(loadedClass.loader); // FIXME: expensive

                        return loadClass(className);
                    }
                    return loadedClass.clazz;
                } else {
                    logger.warn("class file is missing.");
                }
            } else {
                logger.warn("code was modified recently.");
            }
            try {
                logger.warn("class is not yet ready, waiting " + RETRY_WAIT_MS + " ms for complication to complete.");
                Thread.sleep(RETRY_WAIT_MS);
            } catch (InterruptedException e) {
                logger.error("complication waiting was interrupted", e);
            }
            retries++;
        }
        throw new RuntimeException("Failed to load class safely, because constant modifications in the class path.");
    }
    public long recursiveLastModified(File path) {
        long max = path.lastModified();
        if (path.isDirectory()) {
            for (File f : path.listFiles()) {
                max = Math.max(max, recursiveLastModified(f));
            }
        }
        return max;
    }

    private class Location {
        final ReloadableClassLoader LOADER;
        final File PATH;
        Location(ReloadableClassLoader loader, File path) {
            LOADER = loader;
            PATH = path;
        }
        public long lastModified() {
            return recursiveLastModified(PATH);
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
            if (l != null) rv.add(new File(l.PATH, resource).toURL());
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
    public <T> T newProxyInstance(Class<T> interfaceClass,
                                  String implClassName,
                                  Class[] constructorClasses,
                                  Object[] constructorArgs)
            throws RuntimeException {
        MyInvocationHandler handler =
            new MyInvocationHandler(
                implClassName, constructorClasses, constructorArgs);
        return (T) Proxy.newProxyInstance(this,
                new Class[] { interfaceClass }, handler);
    }

    private static class LoadedClass {
        String className;

        ReloadableClassLoader loader;

        private File classFile;

        Class clazz;

        long lastModified;

        public File classFile() {
            if (classFile == null) {
                String path = className.replace('.', '/');
                // Assumption, file exists only in one directory!
                for (File dir : loader.classPaths) {
                    File f = new File(dir, path + ".class");
                    if (f.exists()) {
                        this.classFile = f;
                    }
                }
            }
            return classFile;
        }

        public boolean classFileExists() {
            File f = classFile();
            return f != null && f.exists();
        }

        LoadedClass(String className, ReloadableClassLoader src) {
            this.className = className;
            this.loader = src;
        }

        boolean isChanged() {
            return classFile().lastModified() != lastModified;
        }

        void loadClass() {

            if (clazz != null) {
                return; // class already loaded
            }

            try {
                long before = System.currentTimeMillis();
                debug("loading binary file " + classFile() + " exist " + classFile().exists());

                // load class
                clazz = loader.classLoader.loadClass(className);

                // load class success, remember timestamp
                lastModified = classFile().lastModified();

                debug("took " + (System.currentTimeMillis()-before + " ms"));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to load class "
                        + classFile().getAbsolutePath(), e);
            }

            debug("Init " + clazz);
        }
    }

    private class MyInvocationHandler implements InvocationHandler {

        String backendClassName;

        Object backend;

        Class[] constructorClasses;

        Object[] constructorArgs;

        MyInvocationHandler(String className, Class[] constructorClasses, Object[] constructorArgs) {
            backendClassName = className;
            this.constructorClasses = constructorClasses;
            this.constructorArgs = constructorArgs;

            try {
                Class clz = loadClass(backendClassName);
                backend = newDynamicCodeInstance(clz, constructorClasses, constructorArgs);

            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {

            // check if class has been updated
            Class clz = loadClass(backendClassName);
            if (clz != null && backend.getClass() != clz) {
                debug("updating instance");
                if (backend instanceof Closeable) {
                    ((Closeable)backend).close();
                }
                backend = newDynamicCodeInstance(clz, constructorClasses, constructorArgs);
            }

            try {
                // invoke on backend
                return method.invoke(backend, args);

            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }

        private Object newDynamicCodeInstance(Class clz, Class[] constructorClasses, Object[] constructorArgs) {
            ClassLoader oldCl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(DynamicClassLoader.this);
            try {
                return clz.getConstructor(constructorClasses).newInstance(constructorArgs);
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to create new instance of the dynamically loaded class "
                                + clz.getName(), e);
            } finally {
                Thread.currentThread().setContextClassLoader(oldCl);
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
    private static void debug(String msg) {
        logger.debug(msg);
    }

}
