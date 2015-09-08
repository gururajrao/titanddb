package com.amazon.titan.graphstorage;

import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.amazon.titan.diskstorage.dynamodb.test.TestGraphUtil;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.tinkerpop.blueprints.Vertex;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by guru on 06/09/15.
 */
public class ConcurrencyTests {
    private static TitanGraph GRAPH = null;
    private static final Logger logger = LoggerFactory.getLogger(ConcurrencyTests.class);

    @BeforeClass
    public static void setUpGraph() {
        GRAPH = TestGraphUtil.instance().openGraphWithElasticSearch(BackendDataModel.SINGLE);
        createSchema(GRAPH);
        loadTestVertex(GRAPH);
    }

    private static void loadTestVertex(TitanGraph graph) {
        TitanVertex v = graph.addVertexWithLabel("fb");
        v.setProperty("fbid", "1");
        v.setProperty("ver", "1");
        v.setProperty("count", 0);
        graph.commit();
    }

    private static void createSchema(TitanGraph graph) {
        TitanManagement mgmt = graph.getManagementSystem();
        //Label
        mgmt.makeVertexLabel("fb");

        //Properties
        PropertyKey fbid = mgmt.makePropertyKey("fbid").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey ver = mgmt.makePropertyKey("ver").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey time = mgmt.makePropertyKey("time").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey threadID = mgmt.makePropertyKey("thread").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey count = mgmt.makePropertyKey("count").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();

        //Edges
        mgmt.makeEdgeLabel("frnd").make();

        //Indexes
        TitanIndex byFbid = mgmt.buildIndex("byFbid", Vertex.class).addKey(fbid).unique().buildCompositeIndex();
        TitanIndex byFbidAndVersion = mgmt.buildIndex("byFbidAndVersion", Vertex.class).addKey(fbid).addKey(ver).unique().buildCompositeIndex();

        mgmt.setConsistency(ver, ConsistencyModifier.LOCK);
        mgmt.setConsistency(byFbidAndVersion, ConsistencyModifier.LOCK);

        //Locks

        mgmt.commit();
    }

    @AfterClass
    public static void tearDownGraph() throws BackendException {
        TestGraphUtil.tearDownGraph(GRAPH);
    }

    protected TitanGraph getGraph() {
        return GRAPH;
    }

    @Test
    public void testVertexConcurrentChangesWithoutLocks() throws ExecutionException, InterruptedException {
        //Create a threadpool
        int poolSize = 50;
        List<Future<TitanVertex>> futures = new ArrayList<>(poolSize);
        ExecutorService service = Executors.newFixedThreadPool(poolSize);
        for (int i = 0; i < poolSize; i++) {
            futures.add(service.submit(new UpdateVertexUpdateTime(GRAPH, "1")));
        }

        for (Future<TitanVertex> future: futures){
            TitanVertex v = future.get();
        }

        TitanVertex latest = (TitanVertex) getGraph().getVertices("fbid", "1").iterator().next();
        Date time = new Date((Long) latest.getProperty("time"));
        logger.info("---------------- Value of time is now " + time + " ------------------");
        logger.info("---------------- Value of thread is now " + latest.getProperty("thread") + " ------------------");
        logger.info("---------------- Value of count is now " + latest.getProperty("count") + " ------------------");

        service.shutdown();

    }

    @Test
    public void testVertexConcurrentChangesWithLocks() throws ExecutionException, InterruptedException {
        //Create a threadpool
        int poolSize = 2;
        List<Future<TitanVertex>> futures = new ArrayList<>(poolSize);
        ExecutorService service = Executors.newFixedThreadPool(poolSize);
        for (int i = 0; i < poolSize; i++) {
            futures.add(service.submit(new UpdateVertexMVCC(GRAPH, "1")));
        }

        for (Future<TitanVertex> future: futures){
            try {
                TitanVertex v = future.get();
            }catch(ExecutionException ee){
                //Verify it was a permamnent locking exception
                logger.warn("------------ There was an exception in the callable -----------------");
                TitanException ex = (TitanException) ee.getCause();
                if(ex.isCausedBy(PermanentLockingException.class)){
                    logger.warn("------------ MVCC exception occured -------------");
                }else{
                    logger.warn("----------- Some other exception " + ex.getCause().getClass() + " ----------------");
                }

            }
        }

        TitanVertex latest = (TitanVertex) getGraph().getVertices("fbid", "1").iterator().next();
        Date time = new Date((Long) latest.getProperty("time"));
        logger.info("---------------- Value of time is now " + time + " ------------------");
        logger.info("---------------- Value of thread is now " + latest.getProperty("thread") + " ------------------");
        logger.info("---------------- Value of count is now " + latest.getProperty("count") + " ------------------");
        logger.info("---------------- Value of version is now " + latest.getProperty("ver") + " ------------------");

        service.shutdown();

    }
}
