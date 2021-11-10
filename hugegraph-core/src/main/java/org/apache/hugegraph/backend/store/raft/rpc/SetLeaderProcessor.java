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

package org.apache.hugegraph.backend.store.raft.rpc;

import org.apache.hugegraph.backend.store.raft.RaftGroupManager;
import org.apache.hugegraph.backend.store.raft.RaftSharedContext;
import org.slf4j.Logger;

import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequestProcessor;
import com.baidu.hugegraph.util.Log;
import com.google.protobuf.Message;

public class SetLeaderProcessor
       extends RpcRequestProcessor<RaftRequests.SetLeaderRequest> {

    private static final Logger LOG = Log.logger(SetLeaderProcessor.class);

    private final RaftSharedContext context;

    public SetLeaderProcessor(RaftSharedContext context) {
        super(null, null);
        this.context = context;
    }

    @Override
    public Message processRequest(RaftRequests.SetLeaderRequest request,
                                  RpcRequestClosure done) {
        LOG.debug("Processing SetLeaderRequest {}", request.getClass());
        RaftGroupManager nodeManager = this.context.raftNodeManager(
                                       RaftSharedContext.DEFAULT_GROUP);
        try {
            nodeManager.setLeader(request.getEndpoint());
            RaftRequests.CommonResponse common = RaftRequests.CommonResponse.newBuilder()
                                                                            .setStatus(true)
                                                                            .build();
            return RaftRequests.SetLeaderResponse.newBuilder().setCommon(common).build();
        } catch (Throwable e) {
            RaftRequests.CommonResponse common = RaftRequests.CommonResponse.newBuilder()
                                                                            .setStatus(false)
                                                                            .setMessage(e.toString())
                                                                            .build();
            return RaftRequests.SetLeaderResponse.newBuilder().setCommon(common).build();
        }
    }

    @Override
    public String interest() {
        return RaftRequests.SetLeaderRequest.class.getName();
    }
}
