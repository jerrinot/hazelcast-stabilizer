package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.coordinator.operations.RcTestRunOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestStatusOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestStopOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerKillOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerScriptOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerStartOperation;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.coordinator.registry.WorkerQuery;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.utils.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.localResourceDirectory;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;
import static com.hazelcast.simulator.utils.SimulatorUtils.localIp;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CoordinatorTest {

    private static Coordinator coordinator;
    private static String hzConfig;
    private static Registry registry;
    private static String hzClientConfig;
    private static AgentData agentData;
    private static Agent agent;

    private int initialWorkerIndex;
    private int initialTestIndex;

    @BeforeClass
    public static void beforeClass() throws Exception {
        setupFakeEnvironment();

        hzConfig = FileUtils.fileAsText(new File(localResourceDirectory(), "hazelcast.xml"));
        hzClientConfig = FileUtils.fileAsText(new File(localResourceDirectory(), "client-hazelcast.xml"));

        File simulatorPropertiesFile = new File(getUserDir(), "simulator.properties");
        appendText("CLOUD_PROVIDER=embedded\n", simulatorPropertiesFile);

        SimulatorProperties simulatorProperties = loadSimulatorProperties();

        CoordinatorParameters coordinatorParameters = new CoordinatorParameters()
                .setSimulatorProperties(simulatorProperties)
                .setSkipShutdownHook(true);

        agent = new Agent(1, "127.0.0.1", simulatorProperties.getAgentPort(), 10, 60);
        agent.start();

        registry = new Registry();
        agentData = registry.addAgent(localIp(), localIp());

        coordinator = new Coordinator(registry, coordinatorParameters);
        coordinator.start();
    }

    @Before
    public void before() {
        initialWorkerIndex = agentData.getCurrentWorkerIndex();
        initialTestIndex = registry.getInitialTestIndex();
    }

    @After
    public void after() throws Exception {
        coordinator.workerKill(new RcWorkerKillOperation("js:java.lang.System.exit(0);", new WorkerQuery()));
    }

    @AfterClass
    public static void afterClass() {
        closeQuietly(coordinator);
        closeQuietly(agent);
        tearDownFakeEnvironment();
    }

    private TestSuite newBasicTestSuite() {
        return new TestSuite()
                .setDurationSeconds(5)
                .addTest(new TestCase("foo")
                        .setProperty("threadCount", 1)
                        .setProperty("class", SuccessTest.class));
    }

    private void assertTestCompletesEventually(final String testId) {
        assertTestStateEventually(testId, "completed");
    }

    private void assertTestStateEventually(final String testId, final String expectedState) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                String status = coordinator.testStatus(new RcTestStatusOperation(testId));
                System.out.println("Status: " + status + " expected: " + expectedState);
                assertEquals(expectedState, status);
            }
        });
    }

    @Test
    public void workersStart_multipleWorkers() throws Exception {
        assertEquals("A1_W" + (initialWorkerIndex + 1),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("member").setHzConfig(hzConfig)));
        assertEquals("A1_W" + (initialWorkerIndex + 2),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("member").setHzConfig(hzConfig)));
        assertEquals("A1_W" + (initialWorkerIndex + 3),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("member").setHzConfig(hzConfig)));
    }

    @Test
    public void workerStart_multipleClients() throws Exception {
        assertEquals("A1_W" + (initialWorkerIndex + 1),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("member").setHzConfig(hzConfig)));
        assertEquals("A1_W" + (initialWorkerIndex + 2),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("javaclient").setHzConfig(hzClientConfig)));
        assertEquals("A1_W" + (initialWorkerIndex + 3),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("javaclient").setHzConfig(hzClientConfig)));
    }

    @Test
    public void workerStart_multipleLiteMembers() throws Exception {
        // start regular member
        assertEquals("A1_W" + (initialWorkerIndex + 1),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("member").setHzConfig(hzConfig)));

        // start lite member
        assertEquals("A1_W" + (initialWorkerIndex + 2),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("litemember").setHzConfig(hzConfig)));

        // start another lite member
        assertEquals("A1_W" + (initialWorkerIndex + 3),
                coordinator.workerStart(new RcWorkerStartOperation()
                        .setWorkerType("litemember").setHzConfig(hzConfig)));
    }

    @Test
    public void testStartTest() throws Exception {
        coordinator.workerStart(new RcWorkerStartOperation()
                .setHzConfig(hzConfig));

        TestSuite suite = newBasicTestSuite()
                .setDurationSeconds(10);

        String testId = coordinator.testRun(new RcTestRunOperation(suite).setAsync(true));
        assertEquals(suite.getTestCaseList().get(0).getId(), testId);

        assertTestCompletesEventually(testId);
    }

    @Test
    public void testStopTest() throws Exception {
        coordinator.workerStart(new RcWorkerStartOperation()
                .setHzConfig(hzConfig));

        TestSuite suite = newBasicTestSuite()
                .setDurationSeconds(0);

        String testId = coordinator.testRun(new RcTestRunOperation(suite).setAsync(true));

        assertTestStateEventually(testId, "run");

        coordinator.testStop(new RcTestStopOperation(testId));

        assertTestCompletesEventually(testId);
    }

    @Test
    public void testRun() throws Exception {
        // start worker
        coordinator.workerStart(new RcWorkerStartOperation().setHzConfig(hzConfig));

        TestSuite suite = newBasicTestSuite();

        String response = coordinator.testRun(new RcTestRunOperation(suite).setAsync(false));

        assertNull(response);
    }

    @Test
    public void workerScript() throws Exception {
        coordinator.workerStart(new RcWorkerStartOperation().setHzConfig(hzConfig));

        String result = coordinator.workerScript(new RcWorkerScriptOperation("js:'a'"));

        assertEquals(format("A1_W%s=a", (initialWorkerIndex + 1)), result.replace("\n", ""));
    }
}
