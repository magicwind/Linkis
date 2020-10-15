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
import com.webank.wedatasphere.linkis.metadata.service.DataSourceService;

import com.webank.wedatasphere.linkis.metadata.util.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Created by shanhuang on 9/13/18.
 */
@Service
public class DataSourceServiceImpl implements DataSourceService {

    Logger logger = LogManager.getLogger(DataSourceServiceImpl.class);

    ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public JsonNode getDbs(String userName) throws Exception {
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
}
