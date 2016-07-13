package com.hazelcast.simulator.boot;

import com.hazelcast.simulator.cluster.WorkerConfigurationConverter;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.ClusterLayoutParameters;
import com.hazelcast.simulator.coordinator.Coordinator;
import com.hazelcast.simulator.coordinator.CoordinatorParameters;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.provisioner.ComputeServiceBuilder;
import com.hazelcast.simulator.provisioner.Provisioner;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.utils.jars.HazelcastJARs;
import org.apache.commons.io.IOUtils;
import org.jclouds.compute.ComputeService;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.hazelcast.simulator.test.TestPhase.GLOBAL_WARMUP;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isCloudProvider;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.lang.String.format;
import static java.util.Collections.singleton;

public class SimulatorAPI {

    private static final boolean IS_LOCAL_DIST_ZIP = true;
    private static final String SIMULATOR_VERSION = "0.8-SNAPSHOT";
    private static final String TARGET_DIRECTORY = "/tmp";

    private static final String ZIP_URL
            = format("https://repo1.maven.org/maven2/com/hazelcast/simulator/dist/%s/dist-%s.zip",
            SIMULATOR_VERSION, SIMULATOR_VERSION);
    private static final String TMP_ZIP_FILENAME = TARGET_DIRECTORY + "dist.zip";

    public static void runTest(int boxCount, int memberCount, int clientCount, Class... classes) {
        try {
            File targetDirectory = ensureExistingDirectory(TARGET_DIRECTORY);

            String tmpZipFilename;
            if (IS_LOCAL_DIST_ZIP) {
                tmpZipFilename = format("~/.m2/repository/com/hazelcast/simulator/dist/%s/dist-%s.zip",
                        SIMULATOR_VERSION, SIMULATOR_VERSION);
            } else {
                tmpZipFilename = TMP_ZIP_FILENAME;
                downloadFile(ZIP_URL, TMP_ZIP_FILENAME);
            }

            unzip(tmpZipFilename, targetDirectory);
            makeScriptFilesExecutable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        System.setProperty("SIMULATOR_HOME_OVERRIDE", TARGET_DIRECTORY + "/hazelcast-simulator-" + SIMULATOR_VERSION);
        File simulatorHome = FileUtils.getSimulatorHome();
        File confDir = new File(simulatorHome, "conf");
        ensureExistingDirectory(confDir);

        System.out.println("Simulator Home will be: " + simulatorHome);

        SimulatorProperties properties = new SimulatorProperties();
        properties.set("CLOUD_PROVIDER", "static");
        //properties.set("CLOUD_PROVIDER", "ec2");

        ComputeService computeService = (isCloudProvider(properties) ? new ComputeServiceBuilder(properties).build() : null);
        Bash bash = new Bash(properties);
        String hazelcastVersionSpec = properties.getHazelcastVersionSpec();
        HazelcastJARs hazelcastJARs = HazelcastJARs.newInstance(bash, properties, singleton(hazelcastVersionSpec));
        boolean enterpriseEnabled = false;

        ComponentRegistry componentRegistry = new ComponentRegistry();
        if (boxCount == 0) {
            componentRegistry.addAgent("127.0.0.1", "127.0.0.1");
        }

        Provisioner provisioner = new Provisioner(properties, computeService, bash, hazelcastJARs, enterpriseEnabled,
                componentRegistry);
        if (boxCount > 0) {
            //provisioner.scale(1);
        }
        provisioner.installSimulator();

        TestSuite testSuite = new TestSuite();
        TestCase testCase = new TestCase("testId");
        Class testClass = classes[0];
        testCase.setProperty("class", testClass.getName());
        testSuite.addTest(testCase);
        testSuite.setDurationSeconds(20);
        componentRegistry.addTests(testSuite);

        File workerJar = createWorkerJar(classes);

        runCoordinator(properties, componentRegistry, testSuite, workerJar);
    }

    private static File createWorkerJar(Class... testClasses) {
        File workerJar;
        Set<String> existingDirectories = new HashSet<String>();
        try {
            workerJar = File.createTempFile("simulator-boot", ".jar");
            FileOutputStream baos = new FileOutputStream(workerJar);
            JarOutputStream jar = new JarOutputStream(baos);

            for (Class testClass : testClasses) {
                copyResource(existingDirectories, jar, testClass);
                Class[] declaredClasses = testClass.getDeclaredClasses();
                for (Class declaredClass : declaredClasses) {
                    copyResource(existingDirectories, jar, declaredClass);
                }

            }
            jar.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return workerJar;
    }

    private static void copyResource(Set<String> existingDirectories, JarOutputStream jar, Class declaredClass) throws IOException {
        String canonicalName = declaredClass.getName();
        String transformedName = canonicalName.replace('.', '/');
        String resourceName = transformedName + ".class";
        System.out.println("Copying resource: " + resourceName);

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
        tryToCopyAnonymousInnerClasses(existingDirectories, jar, declaredClass, transformedName);
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

    private static void runCoordinator(SimulatorProperties properties, ComponentRegistry componentRegistry, TestSuite testSuite,
                                       File workerJar) {
        String workerClassPath = workerJar.getAbsolutePath();
        boolean uploadHazelcastJARs = true;
        boolean enterpriseEnabled = false;
        boolean verifyEnabled = false;
        boolean parallel = true;
        boolean refreshJvm = false;
        TargetType targetType = TargetType.ALL;
        int targetCount = 0;
        TestPhase lastTestPhaseToSync = GLOBAL_WARMUP;
        int workerVmStartupDelayMs = 5000;
        CoordinatorParameters coordinatorParameters = new CoordinatorParameters(properties, workerClassPath, uploadHazelcastJARs,
                enterpriseEnabled, verifyEnabled, parallel, refreshJvm,
                targetType, targetCount, lastTestPhaseToSync, workerVmStartupDelayMs);

        boolean autoCreateHzInstance = true;
        int workerStartupTimeout = 5000;
        String memberJvmOptions = "";
        String clientJvmOptions = "";
        String memberHzConfig = createHazelcastConfig();

        String clientHzConfig = "";
        String log4jConfig = "";
        String workerScript = createRunScript();
        boolean monitorPerformance = true;
        WorkerParameters workerParameters = new WorkerParameters(properties,
                autoCreateHzInstance,
                workerStartupTimeout,
                memberJvmOptions,
                clientJvmOptions,
                memberHzConfig,
                clientHzConfig,
                log4jConfig,
                workerScript,
                monitorPerformance);

        String clusterConfiguration = null;
        int defaultHzPort = 5701;
        String licenseKey = "";

        WorkerConfigurationConverter workerConfigurationConverter = new WorkerConfigurationConverter(defaultHzPort,
                licenseKey, workerParameters,
                properties, componentRegistry);
        int memberWorkerCount = 1;
        int clientWorkerCount = 0;
        int dedicatedMemberMachineCount = 0;
        int agentCount = 1;
        ClusterLayoutParameters clusterLayoutParameters = new ClusterLayoutParameters(clusterConfiguration,
                workerConfigurationConverter, memberWorkerCount, clientWorkerCount, dedicatedMemberMachineCount, agentCount);
        Coordinator coordinator = new Coordinator(testSuite, componentRegistry, coordinatorParameters, workerParameters, clusterLayoutParameters);

        coordinator.run();
    }

    private static void downloadFile(String url, String targetFilename) {
        if (!new File(TMP_ZIP_FILENAME).exists()) {
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
    }

    private static void unzip(String zipFile, File folder) {
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

    private static void makeScriptFilesExecutable() {
        String chmodCommand = format("chmod +x %s%shazelcast-simulator-%s%sbin%s",
                TARGET_DIRECTORY, File.separator, SIMULATOR_VERSION, File.separator, File.separator);
        execute(chmodCommand + ".*");
        execute(chmodCommand + "*");
    }

    private static String createRunScript() {
        return "#!/bin/bash\n" +
                "#\n" +
                "# Script to start up a Simulator Worker. To customize the behavior of the worker, including Java configuration,\n" +
                "# copy this file into the 'work dir' of simulator. See the end of this file for examples for different profilers.\n" +
                "#\n" +
                "# External variables have a @ symbol in front to distinguish them from regular bash variables.\n" +
                "#\n" +
                "\n" +
                "# Automatic exit on script failure.\n" +
                "set -e\n" +
                "\n" +
                "# redirecting output/error to the right logfiles.\n" +
                "exec > worker.out\n" +
                "exec 2>worker.err\n" +
                "\n" +
                "JVM_ARGS=\"-XX:OnOutOfMemoryError=\\\"touch;-9;worker.oome\\\" \\\n" +
                "          -Dhazelcast.logging.type=log4j \\\n" +
                "          -Dlog4j.configuration=file:@LOG4J_FILE \\\n" +
                "          -DSIMULATOR_HOME=@SIMULATOR_HOME \\\n" +
                "          -DpublicAddress=@PUBLIC_ADDRESS \\\n" +
                "          -DagentIndex=@AGENT_INDEX \\\n" +
                "          -DworkerType=@WORKER_TYPE \\\n" +
                "          -DworkerId=@WORKER_ID \\\n" +
                "          -DworkerIndex=@WORKER_INDEX \\\n" +
                "          -DworkerPort=@WORKER_PORT \\\n" +
                "          -DworkerPerformanceMonitorIntervalSeconds=@WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS \\\n" +
                "          -DautoCreateHzInstance=@AUTO_CREATE_HZ_INSTANCE \\\n" +
                "          -DhzConfigFile=@HZ_CONFIG_FILE\"\n" +
                "\n" +
                "# Include the member/client-worker jvm options\n" +
                "JVM_ARGS=\"@JVM_OPTIONS ${JVM_ARGS}\"\n" +
                "\n" +
                "CLASSPATH=@CLASSPATH\n" +
                "\n" +
                "MAIN=\n" +
                "case \"@WORKER_TYPE\" in\n" +
                "    CLIENT )        MAIN=com.hazelcast.simulator.worker.ClientWorker;;\n" +
                "    MEMBER )        MAIN=com.hazelcast.simulator.worker.MemberWorker;;\n" +
                "    INTEGRATION_TEST )   MAIN=com.hazelcast.simulator.worker.IntegrationTestWorker;;\n" +
                "esac\n" +
                "\n" +
                "java -classpath ${CLASSPATH} ${JVM_ARGS} ${MAIN}\n" +
                "\n" +
                "# Convert all hdr files to hgrm files so they can easily be plot using\n" +
                "# http://hdrhistogram.github.io/HdrHistogram/plotFiles.html\n" +
                "for HDR_FILE in *.hdr; do\n" +
                "        # prevent getting *.hdr as result in case of empty directory\n" +
                "        [ -f \"$HDR_FILE\" ] || break\n" +
                "        FILE_NAME=\"${HDR_FILE%.*}\"\n" +
                "        java -cp ${CLASSPATH}  org.HdrHistogram.HistogramLogProcessor -i ${HDR_FILE} -o ${FILE_NAME}\n" +
                "done\n" +
                "\n" +
                "\n" +
                "\n" +
                "#########################################################################\n" +
                "# Yourkit\n" +
                "#########################################################################\n" +
                "#\n" +
                "# When YourKit is enabled, a snapshot is created an put in the worker home directory. So when the artifacts are downloaded, the\n" +
                "# snapshots are included and can be loaded with your YourKit GUI.\n" +
                "#\n" +
                "# To upload the libyjpagent, create a 'upload' directory in the working directory and place the libypagent.so there. Then\n" +
                "# it will be automatically uploaded to all workers.\n" +
                "#\n" +
                "# For more information about the YourKit setting, see:\n" +
                "#   http://www.yourkit.com/docs/java/help/agent.jsp\n" +
                "#   http://www.yourkit.com/docs/java/help/startup_options.jsp\n" +
                "#\n" +
                "# java -agentpath:$(pwd)/libyjpagent.so=dir=$(pwd),sampling -classpath ${CLASSPATH} ${JVM_ARGS} ${MAIN}\n" +
                "\n" +
                "\n" +
                "\n" +
                "#########################################################################\n" +
                "# HProf\n" +
                "#########################################################################\n" +
                "#\n" +
                "# By default a 'java.hprof.txt' is created in the worker directory. Which can be downloaded with the\n" +
                "# 'provisioner --download' command after the test has run.\n" +
                "#\n" +
                "# For configuration options see:\n" +
                "#   http://docs.oracle.com/javase/7/docs/technotes/samples/hprof.html\n" +
                "#\n" +
                "# java -agentlib:hprof=cpu=samples,depth=10 -classpath ${CLASSPATH} ${JVM_ARGS} ${MAIN}\n" +
                "\n" +
                "\n" +
                "\n" +
                "#########################################################################\n" +
                "# Linux Perf\n" +
                "#########################################################################\n" +
                "#\n" +
                "# https://perf.wiki.kernel.org/index.php/Tutorial#Sampling_with_perf_record\n" +
                "#\n" +
                "# The settings is the full commandline for 'perf record' excluding the actual arguments for the java program\n" +
                "# to start; these will be provided by the Agent. Once the coordinator completes, all the artifacts (including\n" +
                "# the perf.data created by perf) can be downloaded with 'provisioner --download'. Another option is to log into\n" +
                "# the agent machine and do a 'perf report' locally.\n" +
                "#\n" +
                "# TODO:\n" +
                "# More work needed on documentation to get perf running correctly.\n" +
                "#\n" +
                "# If you get the following message:\n" +
                "#           Kernel address maps (/proc/{kallsyms,modules}) were restricted.\n" +
                "#           Check /proc/sys/kernel/kptr_restrict before running 'perf record'.\n" +
                "# Apply the following under root:\n" +
                "#           echo 0 > /proc/sys/kernel/kptr_restrict\n" +
                "# To make it permanent, add it to /etc/rc.local\n" +
                "#\n" +
                "# If you get the following message while doing call graph analysis (-g)\n" +
                "#            No permission to collect stats.\n" +
                "#            Consider tweaking /proc/sys/kernel/perf_event_paranoid.\n" +
                "# Apply the following under root:\n" +
                "#           echo -1 > /proc/sys/kernel/perf_event_paranoid\n" +
                "# To make it permanent, add it to /etc/rc.local\n" +
                "#\n" +
                "# perf record -o perf.data --quiet java -classpath ${CLASSPATH} ${JVM_ARGS} ${MAIN}\n" +
                "\n" +
                "\n" +
                "\n" +
                "#########################################################################\n" +
                "# VTune\n" +
                "#########################################################################\n" +
                "#\n" +
                "# It requires Intel VTune to be installed on the system.\n" +
                "#\n" +
                "# The settings is the full commandline for the amplxe-cl excluding the actual arguments for the java program\n" +
                "# to start; these will be provided by the Agent. Once the coordinator completes, all the artifacts can be downloaded with\n" +
                "# 'provisioner --download'.\n" +
                "#\n" +
                "# To see within the JVM, make sure that you locally have the same Java version (under the same path) as the simulator. Else\n" +
                "# VTune will not be able to see within the JVM.\n" +
                "#\n" +
                "# Reference to amplxe-cl commandline options:\n" +
                "# https://software.intel.com/sites/products/documentation/doclib/iss/2013/amplifier/lin/ug_docs/GUID-09766DB6-3FA8-445B-8E70-5BC9A1BE7C55.htm#GUID-09766DB6-3FA8-445B-8E70-5BC9A1BE7C55\n" +
                "#\n" +
                "# /opt/intel/vtune_amplifier_xe/bin64/amplxe-cl -collect hotspots java -classpath ${CLASSPATH} ${JVM_ARGS} ${MAIN}\n" +
                "\n" +
                "\n" +
                "\n" +
                "#########################################################################\n" +
                "# NUMA Control\n" +
                "#########################################################################\n" +
                "#\n" +
                "# NUMA Control. It allows to start member with a specific numactl settings.\n" +
                "# numactl binary has to be available on PATH\n" +
                "#\n" +
                "# Example: NUMA_CONTROL=numactl -m 0 -N 0\n" +
                "# It will bind members to node 0.\n" +
                "# numactl -m 0 -N 0 java -classpath ${CLASSPATH} ${JVM_ARGS} ${MAIN}\n" +
                "\n" +
                "\n" +
                "\n" +
                "#########################################################################\n" +
                "# DStat\n" +
                "#########################################################################\n" +
                "#\n" +
                "# dstat --epoch -m --all --noheaders --nocolor --output dstat.csv 5 > /dev/null &\n" +
                "# java -classpath ${CLASSPATH} ${JVM_ARGS} ${MAIN}\n" +
                "# kill $(jobs -p)\n" +
                "\n" +
                "\n" +
                "\n" +
                "#########################################################################\n" +
                "# OpenOnload\n" +
                "#########################################################################\n" +
                "#\n" +
                "# The network stack for Solarflare network adapters (new lab).\n" +
                "#\n" +
                "# onload --profile=latency java -classpath ${CLASSPATH} ${JVM_ARGS} ${MAIN}";
    }

    private static String createHazelcastConfig() {
        return "<hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config\n" +
                "                               http://www.hazelcast.com/schema/config/hazelcast-config-3.6.xsd\"\n" +
                "           xmlns=\"http://www.hazelcast.com/schema/config\"\n" +
                "           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n" +
                "\n" +
                "    <group>\n" +
                "        <name>workers</name>\n" +
                "    </group>\n" +
                "\n" +
                "    <network>\n" +
                "        <port port-count=\"200\" auto-increment=\"true\">5701</port>\n" +
                "        <join>\n" +
                "            <multicast enabled=\"false\"/>\n" +
                "            <tcp-ip enabled=\"true\">\n" +
                "                <!--MEMBERS-->\n" +
                "            </tcp-ip>\n" +
                "        </join>\n" +
                "    </network>\n" +
                "\n" +
                "    <properties>\n" +
                "        <property name=\"hazelcast.phone.home.enabled\">false</property>\n" +
                "    </properties>\n" +
                "\n" +
                "    <!--LICENSE-KEY-->\n" +
                "\n" +
                "    <!--MANAGEMENT_CENTER_CONFIG-->\n" +
                "\n" +
                "    <map name=\"MapStore*\">\n" +
                "        <map-store enabled=\"true\">\n" +
                "            <class-name>com.hazelcast.simulator.tests.map.helpers.MapStoreWithCounter</class-name>\n" +
                "            <write-delay-seconds>5</write-delay-seconds>\n" +
                "        </map-store>\n" +
                "    </map>\n" +
                "\n" +
                "    <map name=\"MapEvictAndStore*\">\n" +
                "        <map-store enabled=\"true\">\n" +
                "            <class-name>com.hazelcast.simulator.tests.map.helpers.MapStoreWithCounterPerKey</class-name>\n" +
                "            <write-delay-seconds>5</write-delay-seconds>\n" +
                "            <write-coalescing>false</write-coalescing>\n" +
                "        </map-store>\n" +
                "        <time-to-live-seconds>10</time-to-live-seconds>\n" +
                "        <max-size policy=\"PER_NODE\">5000</max-size>\n" +
                "    </map>\n" +
                "\n" +
                "    <map name=\"MapMaxSize*\">\n" +
                "        <eviction-policy>LRU</eviction-policy>\n" +
                "        <max-size policy=\"PER_NODE\">1000</max-size>\n" +
                "        <eviction-percentage>25</eviction-percentage>\n" +
                "        <min-eviction-check-millis>0</min-eviction-check-millis>\n" +
                "    </map>\n" +
                "\n" +
                "    <map name=\"NoBackup*\">\n" +
                "        <statistics-enabled>false</statistics-enabled>\n" +
                "        <backup-count>0</backup-count>\n" +
                "        <async-backup-count>0</async-backup-count>\n" +
                "    </map>\n" +
                "\n" +
                "    <map name=\"ObjectMap*\">\n" +
                "        <in-memory-format>OBJECT</in-memory-format>\n" +
                "    </map>\n" +
                "\n" +
                "    <map name=\"SerializationStrategyTest*\">\n" +
                "        <in-memory-format>BINARY</in-memory-format>\n" +
                "        <indexes>\n" +
                "            <index ordered=\"false\">stringVal</index>\n" +
                "        </indexes>\n" +
                "    </map>\n" +
                "\n" +
                "    <map name=\"ExtractorMapTest*\">\n" +
                "        <attributes>\n" +
                "            <attribute extractor=\"com.hazelcast.simulator.tests.map.ExtractorMapTest$PayloadExtractor\">\n" +
                "                payloadFromExtractor\n" +
                "            </attribute>\n" +
                "        </attributes>\n" +
                "    </map>\n" +
                "\n" +
                "    <map name=\"PortableExtractorMapTest*\">\n" +
                "        <attributes>\n" +
                "            <attribute extractor=\"com.hazelcast.simulator.tests.map.ExtractorMapTest$PayloadPortableExtractor\">\n" +
                "                payloadFromExtractor\n" +
                "            </attribute>\n" +
                "        </attributes>\n" +
                "    </map>\n" +
                "\n" +
                "\n" +
                "    <serialization>\n" +
                "        <portable-version>1</portable-version>\n" +
                "\n" +
                "        <data-serializable-factories>\n" +
                "            <data-serializable-factory factory-id=\"2000\">\n" +
                "                com.hazelcast.simulator.tests.synthetic.SyntheticSerializableFactory\n" +
                "            </data-serializable-factory>\n" +
                "            <data-serializable-factory factory-id=\"3000\">\n" +
                "                com.hazelcast.simulator.tests.syntheticmap.SyntheticMapSerializableFactory\n" +
                "            </data-serializable-factory>\n" +
                "            <data-serializable-factory factory-id=\"4000\">\n" +
                "                com.hazelcast.simulator.tests.map.domain.IdentifiedDataSerializableObjectFactory\n" +
                "            </data-serializable-factory>\n" +
                "        </data-serializable-factories>\n" +
                "\n" +
                "        <portable-factories>\n" +
                "            <portable-factory factory-id=\"10000001\">\n" +
                "                com.hazelcast.simulator.tests.map.domain.PortableObjectFactory\n" +
                "            </portable-factory>\n" +
                "            <portable-factory factory-id=\"10000002\">\n" +
                "                com.hazelcast.simulator.tests.map.helpers.ComplexDomainObjectPortableFactory\n" +
                "            </portable-factory>\n" +
                "            <portable-factory factory-id=\"5000\">\n" +
                "                com.hazelcast.simulator.tests.map.ExtractorMapTest$SillySequencePortableFactory\n" +
                "            </portable-factory>\n" +
                "            <portable-factory factory-id=\"5001\">\n" +
                "                com.hazelcast.simulator.tests.map.MultiValueMapTest$SillySequencePortableFactory\n" +
                "            </portable-factory>\n" +
                "        </portable-factories>\n" +
                "    </serialization>\n" +
                "\n" +
                "    <services>\n" +
                "        <service enabled=\"true\">\n" +
                "            <name>hz:impl:syntheticMapService</name>\n" +
                "            <class-name>com.hazelcast.simulator.tests.syntheticmap.SyntheticMapService</class-name>\n" +
                "        </service>\n" +
                "    </services>\n" +
                "\n" +
                "    <cache name=\"*\">\n" +
                "    </cache>\n" +
                "\n" +
                "    <cache name=\"maxCachSmall*\">\n" +
                "        <eviction max-size-policy=\"ENTRY_COUNT\" size=\"271\" eviction-policy=\"LFU\"/>\n" +
                "    </cache>\n" +
                "\n" +
                "    <cache name=\"maxCachMedium*\">\n" +
                "        <eviction max-size-policy=\"ENTRY_COUNT\" size=\"1000\" eviction-policy=\"LFU\"/>\n" +
                "    </cache>\n" +
                "\n" +
                "    <cache name=\"maxCachLarge*\">\n" +
                "        <eviction max-size-policy=\"ENTRY_COUNT\" size=\"10000\" eviction-policy=\"LFU\"/>\n" +
                "    </cache>\n" +
                "</hazelcast>\n";
    }
}
