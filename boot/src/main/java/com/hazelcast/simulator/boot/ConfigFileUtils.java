package com.hazelcast.simulator.boot;

import com.hazelcast.config.Config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;

public class ConfigFileUtils {
    private static final String HAZELCAST_CLIENT_CONFIG_FILENAME = "client-hazelcast.xml";
    private static final String HAZELCAST_DEFAULT_CLIENT_CONFIG_FILENAME = "simulator-client-hazelcast-default.xml";

    private static final String HAZELCAST_CONFIG_FILENAME = "hazelcast.xml";
    private static final String HAZELCAST_DEFAULT_CONFIG_FILENAME = "simulator-hazelcast-default.xml";

    private static final String LOG4J_FILENAME = "log4j.xml";
    private static final String LOG4J_DEFAULT_FILENAME = "simulator-log4j-default.xml";

    private static final String WORKER_SCRIPT_FILENAME = "worker.sh";
    private static final String WORKER_DEFAULT_SCRIPT_FILENAME = "simulator-worker.sh";



    static String getClientHazelcastConfig() {
        return getConfigAsString(HAZELCAST_CLIENT_CONFIG_FILENAME, HAZELCAST_DEFAULT_CLIENT_CONFIG_FILENAME);
    }

    static String getHazelcastConfig() {
        return getConfigAsString(HAZELCAST_CONFIG_FILENAME, HAZELCAST_DEFAULT_CONFIG_FILENAME);
    }

    static String getLog4jConfig() {
        return getConfigAsString(LOG4J_FILENAME, LOG4J_DEFAULT_FILENAME);
    }

    static String getWorkerScript() {
        return getConfigAsString(WORKER_SCRIPT_FILENAME, WORKER_DEFAULT_SCRIPT_FILENAME);
    }

    private static String getConfigAsString(String configFilename, String defaultConfigFilename) {
        InputStream in = null;
        try {
            in = loadFromWorkingDirectory(configFilename);
            if (in == null) {
                in = loadFromClasspath(configFilename);
            }
            if (in == null) {
                in = loadDefaultFromClasspath(defaultConfigFilename);
            }
            if (in == null) {
                return null;
            }
            return readFrom(in);
        } finally {
            closeQuietly(in);
        }
    }

    private static String readFrom(InputStream inputStream) {
        StringBuilder stringBuilder = new StringBuilder();
        try {
            int content;
            while ((content = inputStream.read()) != -1) {
                stringBuilder.append((char) content);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            closeQuietly(inputStream);
        }

        return stringBuilder.toString();
    }

    private static InputStream loadFromClasspath(String filename) {
        URL url = Config.class.getClassLoader().getResource(filename);
        if (url == null) {
            return null;
        }

        return Config.class.getClassLoader().getResourceAsStream(filename);
    }

    private static InputStream loadFromWorkingDirectory(String filename) {
        File file = new File(filename);
        if (!file.exists()) {
            return null;
        }
        try {
            return new FileInputStream(file);
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    private static InputStream loadDefaultFromClasspath(String defaultFilename) {
        return Config.class.getClassLoader().getResourceAsStream(defaultFilename);
    }
}
