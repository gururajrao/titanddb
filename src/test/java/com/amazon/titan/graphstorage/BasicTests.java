package com.amazon.titan.graphstorage;

import com.amazon.titan.diskstorage.dynamodb.BackendDataModel;
import com.amazon.titan.diskstorage.dynamodb.test.TestGraphUtil;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.BackendException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by guru on 03/09/15.
 */
public class BasicTests {
    private static TitanGraph GRAPH = null;
    private static final Logger logger = LoggerFactory.getLogger(BasicTests.class);

    @BeforeClass
    public static void setUpGraph() {
        GRAPH = TestGraphUtil.instance().openGraphWithElasticSearch(BackendDataModel.SINGLE);
    }

    @AfterClass
    public static void tearDownGraph() throws BackendException {
        TestGraphUtil.tearDownGraph(GRAPH);
    }

    protected TitanGraph getGraph() {
        return GRAPH;
    }

    @Test
    public void testSingleVertexWithNoProperties(){
        TitanGraph g = this.getGraph();
        logger.debug("----------------------------- Before addition --------------------------------------------------");
        TitanVertex v = g.addVertex();
        logger.debug("------------------------------ After adding vertex ----------------------------------------------");
        g.commit();
        logger.debug("------------------------------Post commit after adding---------------------------------------------");
    }
}
