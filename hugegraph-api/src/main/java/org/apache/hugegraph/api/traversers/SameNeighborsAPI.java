/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.api.traversers;

import java.util.Set;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.graph.EdgeAPI;
import org.apache.hugegraph.api.graph.VertexAPI;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.traversal.algorithm.SameNeighborTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.slf4j.Logger;

import org.apache.hugegraph.HugeGraph;

import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;

@Path("graphs/{graph}/traversers/sameneighbors")
@Singleton
public class SameNeighborsAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @QueryParam("vertex") String vertex,
                      @QueryParam("other") String other,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("max_degree")
                      @DefaultValue(HugeTraverser.DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("limit")
                      @DefaultValue(HugeTraverser.DEFAULT_ELEMENTS_LIMIT) long limit) {
        LOG.debug("Graph [{}] get same neighbors between '{}' and '{}' with " +
                  "direction {}, edge label {}, max degree '{}' and limit '{}'",
                  graph, vertex, other, direction, edgeLabel, maxDegree, limit);

        Id sourceId = VertexAPI.checkAndParseVertexId(vertex);
        Id targetId = VertexAPI.checkAndParseVertexId(other);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        HugeGraph g = graph(manager, graph);
        SameNeighborTraverser traverser = new SameNeighborTraverser(g);
        Set<Id> neighbors = traverser.sameNeighbors(sourceId, targetId, dir,
                                                    edgeLabel, maxDegree, limit);
        return manager.serializer(g).writeList("same_neighbors", neighbors);
    }
}
