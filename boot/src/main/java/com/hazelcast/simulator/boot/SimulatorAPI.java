package com.hazelcast.simulator.boot;

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
import com.hazelcast.simulator.utils.CommonUtils;
import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.utils.jars.HazelcastJARs;
import org.jclouds.compute.ComputeService;

import java.io.File;

import static com.hazelcast.simulator.boot.ConfigFileUtils.getClientHazelcastConfig;
import static com.hazelcast.simulator.boot.ConfigFileUtils.getHazelcastConfig;
import static com.hazelcast.simulator.boot.ConfigFileUtils.getLog4jConfig;
import static com.hazelcast.simulator.boot.ConfigFileUtils.getWorkerScript;
import static com.hazelcast.simulator.boot.SimulatorBootUtils.downloadFile;
import static com.hazelcast.simulator.boot.SimulatorBootUtils.unzip;
import static com.hazelcast.simulator.coordinator.WorkerParameters.initClientHzConfig;
import static com.hazelcast.simulator.coordinator.WorkerParameters.initMemberHzConfig;
import static com.hazelcast.simulator.test.TestPhase.GLOBAL_WARMUP;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_EC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isCloudProvider;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isStatic;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static java.lang.String.format;
import static java.util.Collections.singleton;

@SuppressWarnings("ConstantConditions")
public class SimulatorAPI {

    private static final String SIMULATOR_HOME_BASE_DIRECTORY = "/tmp";
    private static final boolean IS_LOCAL_DIST_ZIP = true;
    private static final String SIMULATOR_VERSION = CommonUtils.getSimulatorVersion();

    private static final String ZIP_URL
            = format("https://repo1.maven.org/maven2/com/hazelcast/simulator/dist/%s/dist-%s.zip",
            SIMULATOR_VERSION, SIMULATOR_VERSION);

    private static final String TMP_ZIP_FILENAME = SIMULATOR_HOME_BASE_DIRECTORY + "dist.zip";

    public static void runTest(int boxCount, int memberCount, int clientCount, int durationSeconds, Class... classes) {
        prepareSimulatorHomeDir();

        System.setProperty("SIMULATOR_HOME_OVERRIDE", SIMULATOR_HOME_BASE_DIRECTORY + "/hazelcast-simulator-" + SIMULATOR_VERSION);
        File simulatorHome = FileUtils.getSimulatorHome();
        File confDir = new File(simulatorHome, "conf");
        ensureExistingDirectory(confDir);

        SimulatorProperties properties = new SimulatorProperties();
        if (boxCount == 0) {
            properties.set("CLOUD_PROVIDER", PROVIDER_STATIC);
        } else {
            properties.set("CLOUD_PROVIDER", PROVIDER_EC2);
        }

        ComputeService computeService = (isCloudProvider(properties) ? new ComputeServiceBuilder(properties).build() : null);
        Bash bash = new Bash(properties);
        String hazelcastVersionSpec = properties.getHazelcastVersionSpec();
        HazelcastJARs hazelcastJARs = HazelcastJARs.newInstance(bash, properties, singleton(hazelcastVersionSpec));
        boolean enterpriseEnabled = false;

        File agentsFile = newFile("agents.txt");
        System.out.println("Using agents file: " + agentsFile.getAbsolutePath());
        ComponentRegistry componentRegistry;
        if (isStatic(properties)) {
            componentRegistry = new ComponentRegistry();
            componentRegistry.addAgent("localhost", "localhost");
        } else {
            componentRegistry = loadComponentRegister(agentsFile);
        }

        Provisioner provisioner = new Provisioner(properties, computeService, bash, hazelcastJARs, enterpriseEnabled,
                componentRegistry);
        if (isStatic(properties)) {
            provisioner.installSimulator();
        } else {
            provisioner.scale(boxCount);
            componentRegistry = loadComponentRegister(agentsFile);
        }
        provisioner.shutdown();

        TestSuite testSuite = new TestSuite();
        TestCase testCase = new TestCase("testId");
        Class testClass = classes[0];
        testCase.setProperty("class", testClass.getName());
        testSuite.addTest(testCase);
        testSuite.setDurationSeconds(durationSeconds);
        componentRegistry.addTests(testSuite);

        File workerJar = JarComposer.createWorkerJar(classes);

        runCoordinator(properties, componentRegistry, testSuite, workerJar, memberCount, clientCount);
    }

    private static void prepareSimulatorHomeDir() {
        try {
            File targetDirectory = ensureExistingDirectory(SIMULATOR_HOME_BASE_DIRECTORY);
            String zipFilename;
            if (IS_LOCAL_DIST_ZIP) {
                zipFilename = format("~/.m2/repository/com/hazelcast/simulator/dist/%s/dist-%s.zip",
                        SIMULATOR_VERSION, SIMULATOR_VERSION);
            } else {
                zipFilename = TMP_ZIP_FILENAME;
                if (!new File(SimulatorAPI.TMP_ZIP_FILENAME).exists()) {
                    downloadFile(ZIP_URL, TMP_ZIP_FILENAME);
                }
            }

            unzip(zipFilename, targetDirectory);
            makeScriptFilesExecutable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static void runCoordinator(SimulatorProperties properties, ComponentRegistry componentRegistry, TestSuite testSuite,
                                       File workerJar, int memberWorkerCount, int clientWorkerCount) {
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
        CoordinatorParameters coordinatorParameters = new CoordinatorParameters(properties, workerClassPath,
                uploadHazelcastJARs, enterpriseEnabled, verifyEnabled, parallel, refreshJvm,
                targetType, targetCount, lastTestPhaseToSync, workerVmStartupDelayMs);

        boolean autoCreateHzInstance = true;
        int workerStartupTimeout = 5000;
        String memberJvmOptions = "";
        String clientJvmOptions = "";
        String memberHzConfig = getHazelcastConfig();
        String clientHzConfig = getClientHazelcastConfig();
        String log4jConfig = getLog4jConfig();
        String workerScript = getWorkerScript();
        boolean monitorPerformance = true;
        int defaultHzPort = 5701;
        String licenseKey = "";

        WorkerParameters workerParameters = new WorkerParameters(properties,
                autoCreateHzInstance,
                workerStartupTimeout,
                memberJvmOptions,
                clientJvmOptions,
                initMemberHzConfig(memberHzConfig, componentRegistry, defaultHzPort, licenseKey, properties),
                initClientHzConfig(clientHzConfig, componentRegistry, defaultHzPort, licenseKey),
                log4jConfig,
                workerScript,
                monitorPerformance);

        int dedicatedMemberMachineCount = 0;
        ClusterLayoutParameters clusterLayoutParameters = new ClusterLayoutParameters(null, null,
                memberWorkerCount, clientWorkerCount, dedicatedMemberMachineCount, componentRegistry.agentCount());
        Coordinator coordinator = new Coordinator(testSuite, componentRegistry, coordinatorParameters, workerParameters,
                clusterLayoutParameters);

        coordinator.run();
    }

    private static void makeScriptFilesExecutable() {
        String chmodCommand = format("chmod +x %s%shazelcast-simulator-%s%sbin%s",
                SIMULATOR_HOME_BASE_DIRECTORY, File.separator, SIMULATOR_VERSION, File.separator, File.separator);
        execute(chmodCommand + ".*");
        execute(chmodCommand + "*");
    }

}
