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

package com.webank.wedatasphere.linkis.metadata.restful.api;

import com.webank.wedatasphere.linkis.metadata.restful.remote.DataSourceRestfulRemote;
import com.webank.wedatasphere.linkis.metadata.service.DataSourceService;
import com.webank.wedatasphere.linkis.metadata.util.Constants;
import com.webank.wedatasphere.linkis.server.Message;
import com.webank.wedatasphere.linkis.server.security.SecurityFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
/**
 * Created by shanhuang on 9/13/18.
 */
@Path("datasource")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Component
public class DataSourceRestfulApi implements DataSourceRestfulRemote {

    Logger logger = LogManager.getLogger(DataSourceRestfulApi.class);

    @Autowired
    DataSourceService dataSourceService;

    @GET
    @Path("dbs")
    public Response queryDatabaseInfo(@Context HttpServletRequest req){
        String userName = "hadoop";//SecurityFilter.getLoginUsername(req);
        logger.info("calling dbs api");
        try {
            JsonNode dbs = dataSourceService.getDbs(userName);
            return Message.messageToResponse(Message.ok("").data("dbs", dbs));
        } catch (Exception e) {
            return Message.messageToResponse(Message.error("Failed to get database(获取数据库失败)", e));
        }
    }
}
