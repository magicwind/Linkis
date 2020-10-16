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
import com.webank.wedatasphere.linkis.server.Message;
import com.webank.wedatasphere.linkis.server.security.SecurityFilter;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    Logger logger = LoggerFactory.getLogger(DataSourceRestfulApi.class);

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

    @GET
    @Path("tables")
    public Response queryTables(@QueryParam("database") String database, @Context HttpServletRequest req){
        String userName = SecurityFilter.getLoginUsername(req);
        try {
            JsonNode tables = dataSourceService.queryTables(database, userName);
            return Message.messageToResponse(Message.ok("").data("tables", tables));
        } catch (Exception e) {
            return Message.messageToResponse(Message.error("Failed to get tables in database(获取数据库的表失败)", e));
        }
    }

    @GET
    @Path("columns")
    public Response queryTableMeta(@QueryParam("database") String database,  @QueryParam("table") String table, @Context HttpServletRequest req){
        String userName = SecurityFilter.getLoginUsername(req);
        try {
            JsonNode columns = dataSourceService.queryTableMeta(database, table, userName);
            return Message.messageToResponse(Message.ok("").data("columns", columns));
        } catch (Exception e) {
            return Message.messageToResponse(Message.error("Failed to get data table structure(获取数据表结构失败)", e));
        }
    }
}
