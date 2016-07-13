package com.hazelcast.simulator.boot;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.newFile;

final class SimulatorBootUtils {

    private SimulatorBootUtils() {
    }

    static void downloadFile(String url, String targetFilename) {
        FileOutputStream fos = null;
        try {
            ReadableByteChannel rbc = Channels.newChannel(new URL(url).openStream());
            fos = new FileOutputStream(targetFilename);
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        } catch (Exception e) {
            throw new RuntimeException("Could not download: " + url);
        } finally {
            closeQuietly(fos);
        }
    }

    static void unzip(String zipFile, File folder) {
        byte[] buffer = new byte[1024];
        File file = newFile(zipFile);
        ZipInputStream zis = null;
        try {
            zis = new ZipInputStream(new FileInputStream(file));
            ZipEntry ze = zis.getNextEntry();

            FileOutputStream fos = null;
            while (ze != null) {
                try {
                    String fileName = ze.getName();
                    File newFile = new File(folder, fileName);

                    // create all non exists folders else you will hit FileNotFoundException for compressed folder
                    ensureExistingDirectory(newFile.getParent());

                    if (ze.isDirectory()) {
                        ensureExistingDirectory(newFile);
                    } else {
                        fos = new FileOutputStream(newFile);
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                } finally {
                    closeQuietly(fos);
                }
                ze = zis.getNextEntry();
            }

            zis.closeEntry();
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            closeQuietly(zis);
        }
    }
}
