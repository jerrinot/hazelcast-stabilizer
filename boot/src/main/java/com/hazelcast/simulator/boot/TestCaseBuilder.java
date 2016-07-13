package com.hazelcast.simulator.boot;

public class TestCaseBuilder {

    private Class[] classes;
    private int clientCount = 0;
    private int memberCount = 1;
    private int boxCount = 0;

    public static TestCaseBuilder testCase() {
        return new TestCaseBuilder();
    }

    public TestCaseBuilder boxCount(int boxCount) {
        this.boxCount = boxCount;
        return this;
    }

    public TestCaseBuilder memberCount(int memberCount) {
        this.memberCount = memberCount;
        return this;
    }

    public TestCaseBuilder clientCount(int clientCount) {
        this.clientCount = clientCount;
        return this;
    }

    public TestCaseBuilder addClasses(Class... classes) {
        this.classes = classes;
        return this;
    }

    public void run() {
        SimulatorAPI.runTest(boxCount, memberCount, clientCount, classes);
    }
}
