package fi.veikkaus.dcontext;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by arau on 22.6.2016.
 */
public class ReloadableClassLoader {
    File[] classPaths;
    URL[] classUrls;

    URLClassLoader classLoader;
    ClassLoader parentClassLoader;

    ReloadableClassLoader(File[] classPaths, ClassLoader parentClassLoader) {
        this.classPaths = classPaths;
        this.parentClassLoader = parentClassLoader;

        classUrls = new URL[classPaths.length];
        for (int i = 0; i < classPaths.length; ++i) {
            try {
                this.classUrls[i] = classPaths[i].toURL();
            } catch (MalformedURLException e) {
                throw new AssertionError("should not happen", e);
            }
        }
        // class loader
        reset();
    }

    void reset() {
        classLoader = new URLClassLoader(classUrls, parentClassLoader);
    }

}
