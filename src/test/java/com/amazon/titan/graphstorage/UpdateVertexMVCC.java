package com.amazon.titan.graphstorage;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;

import java.util.Date;
import java.util.concurrent.Callable;

/**
 * Created by guru on 07/09/15.
 */
public class UpdateVertexMVCC implements Callable<TitanVertex> {
    TitanGraph graph;
    String fbid;

    @Override
    public TitanVertex call() throws Exception {
        System.out.println("-------------- Thread Id : " + Thread.currentThread().getId() + " --------------------");
        //Get the vertex corresponding to id
        TitanVertex v = (TitanVertex) graph.getVertices("fbid", "1").iterator().next();
        //Update the time, thread and count
        long now = new Date().getTime();
        v.setProperty("time", now);
        v.setProperty("thread", Thread.currentThread().getId());
        int count = v.getProperty("count");
        count++;
        v.setProperty("count", count);
        String ver = v.getProperty("ver");
        ver = "" + (Integer.parseInt(ver) + 1);
        v.setProperty("ver", ver);
        //Commit
        graph.commit();
        return v;
    }

    UpdateVertexMVCC(TitanGraph g, String fbid){
        this.graph = g;
        this.fbid = fbid;
    }
}
