/*
 * Copyright 2019 WeBank
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.webank.wedatasphere.linkis.metadata.service.impl;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.webank.wedatasphere.linkis.metadata.service.DataSourceService;

import com.webank.wedatasphere.linkis.metadata.util.Constants;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Created by shanhuang on 9/13/18.
 */
@Service
public class DataSourceServiceImpl implements DataSourceService {

    Logger logger = LoggerFactory.getLogger(DataSourceServiceImpl.class);

    ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public JsonNode getDbs(String userName) {
        logger.info("userName: " + userName);
        List<String> dbs = Lists.newArrayList();

        AWSGlue glueClient = AWSGlueClient.builder().withRegion(Constants.AWS_REGION).build();

        GetDatabasesRequest request = new GetDatabasesRequest();
        logger.info("start to call the glue client's getDatabases...");
        GetDatabasesResult result = glueClient.getDatabases(request);
        logger.info("got the result of getDatabases.");

        for (Database db : result.getDatabaseList()) {
            dbs.add(db.getName());
        }

        ArrayNode dbsNode = jsonMapper.createArrayNode();
        for(String db : dbs){
            ObjectNode dbNode = jsonMapper.createObjectNode();
            dbNode.put("dbName", db);
            dbsNode.add(dbNode);
        }
        return dbsNode;
    }

    @Override
    public JsonNode queryTables(String database, String userName) {
        List<Map<String, Object>> listTables = Lists.newArrayList();
        try {
            Map<String, String> map = Maps.newHashMap();
            map.put("dbName", database);
            map.put("userName", userName);
            //listTables =hiveMetaDao.getTablesByDbNameAndUser(map);
            Map<String, Object> tempTable = Maps.newHashMap();
            tempTable.put("NAME", "crm_users");
            tempTable.put("TYPE", "MANAGED_TABLE");
            tempTable.put("OWNER", "hive");
            tempTable.put("CREATE_TIME", 0);
            tempTable.put("LAST_ACCESS_TIME", 100000000);

            listTables.add(tempTable);

        } catch (Throwable e) {
            logger.error("Failed to list Tables:", e);
            throw new RuntimeException(e);
        }

        ArrayNode tables = jsonMapper.createArrayNode();
        for(Map<String, Object> table : listTables){
            ObjectNode tableNode = jsonMapper.createObjectNode();
            tableNode.put("tableName", (String) table.get("NAME"));
            tableNode.put("isView", table.get("TYPE").equals("VIRTUAL_VIEW"));
            tableNode.put("databaseName", database);
            tableNode.put("createdBy", (String) table.get("OWNER"));
            tableNode.put("createdAt", (Integer) table.get("CREATE_TIME"));
            tableNode.put("lastAccessAt", (Integer) table.get("LAST_ACCESS_TIME"));
            tables.add(tableNode);
        }
        return tables;
    }

    @Override
    public JsonNode queryTableMeta(String dbName, String tableName, String userName) {
        logger.info("getTable:" + userName);
        Map<String, String> param = Maps.newHashMap();
        param.put("dbName", dbName);
        param.put("tableName", tableName);
        //List<Map<String, Object>> columns = hiveMetaDao.getColumns(param);
        List<Map<String, Object>> columns = Lists.newArrayList();
        Map<String, Object>  idColumn = Maps.newHashMap();
        idColumn.put("COLUMN_NAME", "id");
        idColumn.put("TYPE_NAME", "integer");
        idColumn.put("COMMENT", "primary key");

        Map<String, Object>  nameColumn = Maps.newHashMap();
        nameColumn.put("COLUMN_NAME", "name");
        nameColumn.put("TYPE_NAME", "string");
        nameColumn.put("COMMENT", "user's name");

        columns.add(idColumn);
        columns.add(nameColumn);

        ArrayNode columnsNode = jsonMapper.createArrayNode();
        for(Map<String, Object> column : columns){
            ObjectNode fieldNode = jsonMapper.createObjectNode();
            fieldNode.put("columnName", (String) column.get("COLUMN_NAME"));
            fieldNode.put("columnType", (String) column.get("TYPE_NAME"));
            fieldNode.put("columnComment", (String) column.get("COMMENT"));
            fieldNode.put("partitioned", false);
            columnsNode.add(fieldNode);
        }
        //List<Map<String, Object>> partitionKeys = hiveMetaDao.getPartitionKeys(param);
        List<Map<String, Object>> partitionKeys = Lists.newArrayList();
        Map<String, Object> partKey = Maps.newHashMap();
        partKey.put("PKEY_NAME", "created_at_dt");
        partKey.put("PKEY_TYPE", "string");
        partKey.put("PKEY_COMMENT", "分区键");
        partitionKeys.add(partKey);

        for(Map<String, Object> partitionKey : partitionKeys){
            ObjectNode fieldNode = jsonMapper.createObjectNode();
            fieldNode.put("columnName", (String) partitionKey.get("PKEY_NAME"));
            fieldNode.put("columnType", (String) partitionKey.get("PKEY_TYPE"));
            fieldNode.put("columnComment", (String) partitionKey.get("PKEY_COMMENT"));
            fieldNode.put("partitioned", true);
            columnsNode.add(fieldNode);
        }
        return columnsNode;
    }
}
