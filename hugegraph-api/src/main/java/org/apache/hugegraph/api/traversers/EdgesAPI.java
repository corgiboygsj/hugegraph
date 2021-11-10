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

import java.util.Iterator;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.CompressInterceptor;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.store.Shard;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.server.RestServer;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.slf4j.Logger;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;

@Path("graphs/{graph}/traversers/edges")
@Singleton
public class EdgesAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @CompressInterceptor.Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("ids") List<String> stringIds) {
        LOG.debug("Graph [{}] get edges by ids: {}", graph, stringIds);

        E.checkArgument(stringIds != null && !stringIds.isEmpty(),
                        "The ids parameter can't be null or empty");

        Object[] ids = new Id[stringIds.size()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = HugeEdge.getIdValue(stringIds.get(i), false);
        }

        HugeGraph g = graph(manager, graph);

        Iterator<Edge> edges = g.edges(ids);
        return manager.serializer(g).writeEdges(edges, false);
    }

    @GET
    @Timed
    @Path("shards")
    @CompressInterceptor.Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String shards(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @QueryParam("split_size") long splitSize) {
        LOG.debug("Graph [{}] get vertex shards with split size '{}'",
                  graph, splitSize);

        HugeGraph g = graph(manager, graph);
        List<Shard> shards = g.metadata(HugeType.EDGE_OUT, "splits", splitSize);
        return manager.serializer(g).writeList("shards", shards);
    }

    @GET
    @Timed
    @Path("scan")
    @CompressInterceptor.Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String scan(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("start") String start,
                       @QueryParam("end") String end,
                       @QueryParam("page") String page,
                       @QueryParam("page_limit")
                       @DefaultValue(HugeTraverser.DEFAULT_PAGE_LIMIT) long pageLimit) {
        LOG.debug("Graph [{}] query edges by shard(start: {}, end: {}, " +
                  "page: {}) ", graph, start, end, page);

        HugeGraph g = graph(manager, graph);

        ConditionQuery query = new ConditionQuery(HugeType.EDGE_OUT);
        query.scan(start, end);
        query.page(page);
        if (query.paging()) {
            query.limit(pageLimit);
        }
        Iterator<Edge> edges = g.edges(query);

        return manager.serializer(g).writeEdges(edges, query.paging());
    }
}
