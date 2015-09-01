package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.mapreduce.aggregation.Aggregations;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.map.domain.DomainObject;
import com.hazelcast.simulator.tests.map.domain.DomainObjectFactory;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.MapStreamer;
import com.hazelcast.simulator.worker.loadsupport.MapStreamerFactory;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang3.RandomUtils.nextDouble;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.apache.commons.lang3.RandomUtils.nextLong;

public class SerializationStrategyTest {

    private enum Operation {
        GET_BY_STRING_INDEX,
        AGGREGATION
    }

    public enum Strategy {
        PORTABLE,
        SERIALIZABLE,
        DATA_SERIALIZABLE,
        IDENTIFIED_DATA_SERIALIZABLE
    }

    private static final ILogger LOGGER = Logger.getLogger(SerializationStrategyTest.class);
    private static final ThrottlingLogger THROTTLING_LOGGER = ThrottlingLogger.newLogger(LOGGER, 5000);

    public String basename = SerializationStrategyTest.class.getSimpleName();
    public Strategy strategy = Strategy.PORTABLE;

    public int itemCount = 1000000;
    public int recordsPerUnique = 10000;

    public double getAggregationProb = 0;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();


    private IMap<String, DomainObject> map;
    private Set<String> uniqueStrings;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        map = testContext.getTargetInstance().getMap(basename + "-" + testContext.getTestId());
        uniqueStrings = testContext.getTargetInstance().getSet(basename + "-" + testContext.getTestId());

        operationSelectorBuilder.addOperation(Operation.AGGREGATION, getAggregationProb)
                .addDefaultOperation(Operation.GET_BY_STRING_INDEX);

    }

    @Warmup(global = true)
    public void initDataLoad() {
        int uniqueStringsCount = itemCount / recordsPerUnique;
        String[] strings = generateUniqueStrings(uniqueStringsCount);

        MapStreamer<String, DomainObject> streamer = MapStreamerFactory.getInstance(map);
        DomainObjectFactory objectFactory = DomainObjectFactory.newFactory(strategy);
        for (int i = 0; i < itemCount; i++) {
            String indexedField = strings[RandomUtils.nextInt(0, uniqueStringsCount)];
            DomainObject o = createNewDomainObject(objectFactory, indexedField);
            streamer.pushEntry(o.getKey(), o);
        }
        streamer.await();
    }

    private String[] generateUniqueStrings(int uniqueStringsCount) {
        Set<String> stringsSet = new HashSet<String>(uniqueStringsCount);
        do {
            String randomString = RandomStringUtils.randomAlphabetic(30);
            stringsSet.add(randomString);
        } while (stringsSet.size() != uniqueStringsCount);
        uniqueStrings.addAll(stringsSet);
        return stringsSet.toArray(new String[uniqueStringsCount]);
    }

    private DomainObject createNewDomainObject(DomainObjectFactory objectFactory, String indexedField) {
        DomainObject o = objectFactory.newInstance();
        o.setKey(randomAlphanumeric(7));
        o.setStringVal(indexedField);
        o.setIntVal(nextInt(0, Integer.MAX_VALUE));
        o.setLongVal(nextLong(0, Long.MAX_VALUE));
        o.setDoubleVal(nextDouble(0.0, Double.MAX_VALUE));
        return o;
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        private String[] localUniqueStrings;
        private Supplier<String, DomainObject, String> supplier;

        public Worker() {
            super(operationSelectorBuilder);
            supplier = new Extractor();
        }

        @Override
        protected void beforeRun() {
            localUniqueStrings = uniqueStrings.toArray(new String[uniqueStrings.size()]);
            super.beforeRun();
        }

        @Override
        protected void timeStep(Operation operation) {
            switch (operation) {
                case GET_BY_STRING_INDEX:
                    String string = getUniqueString();
                    Predicate predicate = Predicates.equal("stringVal", string);
                    Set<Map.Entry<String, DomainObject>> entries = map.entrySet(predicate);
                    THROTTLING_LOGGER.log(Level.INFO, "GetByStringIndex: " + entries.size() + " entries");
                    break;
                case AGGREGATION:
                    Set<String> distinctValues = map.aggregate(supplier, Aggregations.<String, String, String>distinctValues());
                    THROTTLING_LOGGER.log(Level.INFO, "There are " + distinctValues.size() + " distinct values");
                    break;
                default:
                    throw new IllegalArgumentException("Unknown Operation: " + operation);
            }
        }

        public String getUniqueString() {
            int i = randomInt(localUniqueStrings.length);
            return localUniqueStrings[i];
        }
    }

    private static class Extractor extends Supplier<String, DomainObject, String> implements DataSerializable {
        @Override
        public String apply(Map.Entry<String, DomainObject> entry) {
            return entry.getValue().getStringVal();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {

        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {

        }
    }
}
