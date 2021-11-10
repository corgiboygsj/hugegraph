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

package org.apache.hugegraph.backend.store.rocksdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.page.PageState;
import org.apache.hugegraph.backend.query.Aggregate;
import org.apache.hugegraph.backend.query.Condition;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.IdPrefixQuery;
import org.apache.hugegraph.backend.query.IdRangeQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.serializer.BinaryBackendEntry;
import org.apache.hugegraph.backend.serializer.BinaryEntryIterator;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.backend.store.BackendEntryIterator;
import org.apache.hugegraph.backend.store.BackendTable;
import org.apache.hugegraph.backend.store.Shard;
import org.apache.hugegraph.exception.NotSupportException;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.StringEncoding;
import org.slf4j.Logger;

import com.baidu.hugegraph.iterator.FlatMapperIterator;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class RocksDBTable extends
                          BackendTable<RocksDBSessions.Session, BackendEntry> {

    private static final Logger LOG = Log.logger(RocksDBStore.class);

    private final RocksDBShardSpliter shardSpliter;

    public RocksDBTable(String database, String table) {
        super(String.format("%s+%s", database, table));
        this.shardSpliter = new RocksDBShardSpliter(this.table());
    }

    @Override
    protected void registerMetaHandlers() {
        this.registerMetaHandler("splits", (session, meta, args) -> {
            E.checkArgument(args.length == 1,
                            "The args count of %s must be 1", meta);
            long splitSize = (long) args[0];
            return this.shardSpliter.getSplits(session, splitSize);
        });
    }

    @Override
    public void init(RocksDBSessions.Session session) {
        // pass
    }

    @Override
    public void clear(RocksDBSessions.Session session) {
        // pass
    }

    @Override
    public void insert(RocksDBSessions.Session session, BackendEntry entry) {
        assert !entry.columns().isEmpty();
        for (BackendEntry.BackendColumn col : entry.columns()) {
            assert entry.belongToMe(col) : entry;
            session.put(this.table(), col.name, col.value);
        }
    }

    @Override
    public void delete(RocksDBSessions.Session session, BackendEntry entry) {
        if (entry.columns().isEmpty()) {
            session.delete(this.table(), entry.id().asBytes());
        } else {
            for (BackendEntry.BackendColumn col : entry.columns()) {
                assert entry.belongToMe(col) : entry;
                session.delete(this.table(), col.name);
            }
        }
    }

    @Override
    public void append(RocksDBSessions.Session session, BackendEntry entry) {
        assert entry.columns().size() == 1;
        this.insert(session, entry);
    }

    @Override
    public void eliminate(RocksDBSessions.Session session, BackendEntry entry) {
        assert entry.columns().size() == 1;
        this.delete(session, entry);
    }

    @Override
    public Number queryNumber(RocksDBSessions.Session session, Query query) {
        Aggregate aggregate = query.aggregateNotNull();
        if (aggregate.func() != Aggregate.AggregateFunc.COUNT) {
            throw new NotSupportException(aggregate.toString());
        }

        assert aggregate.func() == Aggregate.AggregateFunc.COUNT;
        assert query.noLimit();
        Iterator<BackendEntry.BackendColumn> results = this.queryBy(session, query);
        if (results instanceof RocksDBSessions.Countable) {
            return ((RocksDBSessions.Countable) results).count();
        }
        return IteratorUtils.count(results);
    }

    @Override
    public Iterator<BackendEntry> query(RocksDBSessions.Session session, Query query) {
        if (query.limit() == 0L && !query.noLimit()) {
            LOG.debug("Return empty result(limit=0) for query {}", query);
            return Collections.emptyIterator();
        }
        return newEntryIterator(this.queryBy(session, query), query);
    }

    protected BackendEntry.BackendColumnIterator queryBy(
            RocksDBSessions.Session session, Query query) {
        // Query all
        if (query.empty()) {
            return this.queryAll(session, query);
        }

        // Query by prefix
        if (query instanceof IdPrefixQuery) {
            IdPrefixQuery pq = (IdPrefixQuery) query;
            return this.queryByPrefix(session, pq);
        }

        // Query by range
        if (query instanceof IdRangeQuery) {
            IdRangeQuery rq = (IdRangeQuery) query;
            return this.queryByRange(session, rq);
        }

        // Query by id
        if (query.conditions().isEmpty()) {
            assert !query.ids().isEmpty();
            // NOTE: this will lead to lazy create rocksdb iterator
            return new BackendEntry.BackendColumnIteratorWrapper(new FlatMapperIterator<>(
                   query.ids().iterator(), id -> this.queryById(session, id)
            ));
        }

        // Query by condition (or condition + id)
        ConditionQuery cq = (ConditionQuery) query;
        return this.queryByCond(session, cq);
    }

    protected BackendEntry.BackendColumnIterator queryAll(
            RocksDBSessions.Session session, Query query) {
        if (query.paging()) {
            PageState page = PageState.fromString(query.page());
            byte[] begin = page.position();
            return session.scan(this.table(), begin, null, RocksDBSessions.Session.SCAN_ANY);
        } else {
            return session.scan(this.table());
        }
    }

    protected BackendEntry.BackendColumnIterator queryById(
            RocksDBSessions.Session session, Id id) {
        // TODO: change to get() after vertex and schema don't use id prefix
        return session.scan(this.table(), id.asBytes());
    }

    protected BackendEntry.BackendColumnIterator getById(
            RocksDBSessions.Session session, Id id) {
        byte[] value = session.get(this.table(), id.asBytes());
        if (value == null) {
            return BackendEntry.BackendColumnIterator.empty();
        }
        BackendEntry.BackendColumn
                col = BackendEntry.BackendColumn.of(id.asBytes(), value);
        return new BackendEntry.BackendColumnIteratorWrapper(col);
    }

    protected BackendEntry.BackendColumnIterator queryByPrefix(
            RocksDBSessions.Session session,
            IdPrefixQuery query) {
        int type = query.inclusiveStart() ?
                   RocksDBSessions.Session.SCAN_GTE_BEGIN : RocksDBSessions.Session.SCAN_GT_BEGIN;
        type |= RocksDBSessions.Session.SCAN_PREFIX_END;
        return session.scan(this.table(), query.start().asBytes(),
                            query.prefix().asBytes(), type);
    }

    protected BackendEntry.BackendColumnIterator queryByRange(
            RocksDBSessions.Session session,
            IdRangeQuery query) {
        byte[] start = query.start().asBytes();
        byte[] end = query.end() == null ? null : query.end().asBytes();
        int type = query.inclusiveStart() ?
                   RocksDBSessions.Session.SCAN_GTE_BEGIN : RocksDBSessions.Session.SCAN_GT_BEGIN;
        if (end != null) {
            type |= query.inclusiveEnd() ?
                    RocksDBSessions.Session.SCAN_LTE_END : RocksDBSessions.Session.SCAN_LT_END;
        }
        return session.scan(this.table(), start, end, type);
    }

    protected BackendEntry.BackendColumnIterator queryByCond(
            RocksDBSessions.Session session,
            ConditionQuery query) {
        if (query.containsScanRelation()) {
            E.checkArgument(query.relations().size() == 1,
                            "Invalid scan with multi conditions: %s", query);
            Condition.Relation scan = query.relations().iterator().next();
            Shard shard = (Shard) scan.value();
            return this.queryByRange(session, shard, query.page());
        }
        throw new NotSupportException("query: %s", query);
    }

    protected BackendEntry.BackendColumnIterator queryByRange(
            RocksDBSessions.Session session, Shard shard,
            String page) {
        byte[] start = this.shardSpliter.position(shard.start());
        byte[] end = this.shardSpliter.position(shard.end());
        if (page != null && !page.isEmpty()) {
            byte[] position = PageState.fromString(page).position();
            E.checkArgument(start == null ||
                            Bytes.compare(position, start) >= 0,
                            "Invalid page out of lower bound");
            start = position;
        }
        if (start == null) {
            start = ShardSpliter.START_BYTES;
        }
        int type = RocksDBSessions.Session.SCAN_GTE_BEGIN;
        if (end != null) {
            type |= RocksDBSessions.Session.SCAN_LT_END;
        }
        return session.scan(this.table(), start, end, type);
    }

    public boolean isOlap() {
        return false;
    }

    protected static final BackendEntryIterator newEntryIterator(
                                                BackendEntry.BackendColumnIterator cols,
                                                Query query) {
        return new BinaryEntryIterator<>(cols, query, (entry, col) -> {
            if (entry == null || !entry.belongToMe(col)) {
                HugeType type = query.resultType();
                // NOTE: only support BinaryBackendEntry currently
                entry = new BinaryBackendEntry(type, col.name);
            }
            entry.columns(col);
            return entry;
        });
    }

    protected static final long sizeOfBackendEntry(BackendEntry entry) {
        return BinaryEntryIterator.sizeOfEntry(entry);
    }

    private static class RocksDBShardSpliter extends ShardSpliter<RocksDBSessions.Session> {

        private static final String MEM_SIZE = "rocksdb.size-all-mem-tables";
        private static final String SST_SIZE = "rocksdb.total-sst-files-size";

        private static final String NUM_KEYS = "rocksdb.estimate-num-keys";

        public RocksDBShardSpliter(String table) {
            super(table);
        }

        @Override
        public List<Shard> getSplits(RocksDBSessions.Session session, long splitSize) {
            E.checkArgument(splitSize >= MIN_SHARD_SIZE,
                            "The split-size must be >= %s bytes, but got %s",
                            MIN_SHARD_SIZE, splitSize);

            Pair<byte[], byte[]> keyRange = session.keyRange(this.table());
            if (keyRange == null || keyRange.getRight() == null) {
                return super.getSplits(session, splitSize);
            }

            long size = this.estimateDataSize(session);
            if (size <= 0) {
                size = this.estimateNumKeys(session) * ESTIMATE_BYTES_PER_KV;
            }

            double count = Math.ceil(size / (double) splitSize);
            if (count <= 0) {
                count = 1;
            }

            Range range = new Range(keyRange.getLeft(),
                                    Range.increase(keyRange.getRight()));
            List<Shard> splits = new ArrayList<>((int) count);
            splits.addAll(range.splitEven((int) count));
            return splits;
        }

        @Override
        public long estimateDataSize(RocksDBSessions.Session session) {
            long mem = Long.parseLong(session.property(this.table(), MEM_SIZE));
            long sst = Long.parseLong(session.property(this.table(), SST_SIZE));
            return mem + sst;
        }

        @Override
        public long estimateNumKeys(RocksDBSessions.Session session) {
            return Long.parseLong(session.property(this.table(), NUM_KEYS));
        }

        @Override
        public byte[] position(String position) {
            if (START.equals(position) || END.equals(position)) {
                return null;
            }
            return StringEncoding.decodeBase64(position);
        }
    }
}
