/*
 * streams-persist-neo4j
 * Copyright (C) 2015 eidat.io
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.governing permissions and limitations
 * under the License.
 */

package org.apache.streams.graph.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.streams.core.StreamsDatum;
import org.apache.streams.graph.GraphHttpConfiguration;
import org.apache.streams.graph.GraphHttpPersistWriter;
import org.apache.streams.graph.GraphVertexReader;
import org.apache.streams.jackson.StreamsJacksonMapper;
import org.apache.streams.pojo.json.Activity;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Unit test for
 * @see {@link GraphVertexReader}
 *
 * Test that graph db http writes to neo4j rest API
 *
 *
 */
@Ignore("Need to find a way to launch neo4j during verify step to use this")
public class Neo4jHttpPersistWriterIT {

    private final static Logger LOGGER = LoggerFactory.getLogger(Neo4jHttpPersistWriterIT.class);

    private final static ObjectMapper mapper = StreamsJacksonMapper.getInstance();

    private GraphHttpConfiguration testConfiguration;

    private GraphHttpPersistWriter graphPersistWriter;

    @Before
    public void prepareTest() throws IOException {

        testConfiguration = new GraphHttpConfiguration();
        testConfiguration.setType(GraphHttpConfiguration.Type.NEO_4_J);
        testConfiguration.setGraph("data");
        testConfiguration.setHostname("localhost");
        testConfiguration.setPort(7474l);
        testConfiguration.setContentType("application/json");
        testConfiguration.setProtocol("http");

        graphPersistWriter = new GraphHttpPersistWriter(testConfiguration);

        graphPersistWriter.prepare(testConfiguration);
    }

    @Test
    public void testNeo4jHttpPersistWriter() throws IOException {

        InputStream testActivityFolderStream = Neo4jHttpPersistWriterIT.class.getClassLoader()
                .getResourceAsStream("activities");
        List<String> files = IOUtils.readLines(testActivityFolderStream, Charsets.UTF_8);

        for( String file : files) {
            LOGGER.info("File: " + file );
            InputStream testActivityFileStream = Neo4jHttpPersistWriterIT.class.getClassLoader()
                    .getResourceAsStream("activities/" + file);
            Activity activity = mapper.readValue(testActivityFileStream, Activity.class);
            activity.getActor().setId(activity.getActor().getObjectType());
            activity.getObject().setId(activity.getObject().getObjectType());
            StreamsDatum datum = new StreamsDatum(activity, activity.getVerb());
            graphPersistWriter.write( datum );
            LOGGER.info("Wrote: " + activity.getVerb() );
        }

        graphPersistWriter.cleanUp();

        // hit neo with http and check vertex/edge counts

    }
}
