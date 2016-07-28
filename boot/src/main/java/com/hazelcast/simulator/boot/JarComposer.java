package com.hazelcast.simulator.boot;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

public class JarComposer {
    static File createWorkerJar(Class... testClasses) {
        File workerJar;
        Set<String> createdDirectories = new HashSet<String>();
        try {
            workerJar = File.createTempFile("simulator-boot", ".jar");
            workerJar.deleteOnExit();
            FileOutputStream baos = new FileOutputStream(workerJar);
            JarOutputStream jar = new JarOutputStream(baos);

            for (Class testClass : testClasses) {
                copyResource(createdDirectories, jar, testClass);
                Class[] declaredClasses = testClass.getDeclaredClasses();
                for (Class declaredClass : declaredClasses) {
                    copyResource(createdDirectories, jar, declaredClass);
                }

            }
            jar.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return workerJar;
    }

    private static void copyResource(Set<String> existingDirectories, JarOutputStream jar, Class declaredClass) throws IOException {
        String transformedName = declaredClass.getName().replace('.', '/');
        String resourceName = transformedName + ".class";
        int lastSlash = transformedName.lastIndexOf('/');
        String dir = transformedName.substring(0, lastSlash + 1);
        if (existingDirectories.add(dir)) {
            jar.putNextEntry(new ZipEntry(dir));
        }
        jar.putNextEntry(new ZipEntry(resourceName));
        InputStream is = declaredClass.getClassLoader().getResourceAsStream(resourceName);
        byte[] bytes = IOUtils.toByteArray(is);
        jar.write(bytes);
        jar.closeEntry();

        // try to copy anonymous inner classes
        String javaVersion = System.getProperty("java.version");
        if (javaVersion.startsWith("1.8.")) {
            tryToCopyAnonymousInnerClasses(existingDirectories, jar, declaredClass, transformedName);
        }
    }

    private static void tryToCopyAnonymousInnerClasses(Set<String> existingDirectories, JarOutputStream jar, Class declaredClass,
                                                       String transformedName) throws IOException {
        String resourceName;
        int lastSlash;
        String dir;
        InputStream is;
        byte[] bytes;
        for (int i = 1; ; i++) {
            resourceName = transformedName + "$" + i + ".class";
            lastSlash = transformedName.lastIndexOf('/');
            dir = transformedName.substring(0, lastSlash + 1);
            if (existingDirectories.add(dir)) {
                jar.putNextEntry(new ZipEntry(dir));
            }
            jar.putNextEntry(new ZipEntry(resourceName));
            is = declaredClass.getClassLoader().getResourceAsStream(resourceName);
            if (is == null) {
                return;
            }
            bytes = IOUtils.toByteArray(is);
            jar.write(bytes);
            jar.closeEntry();
        }
    }
}
