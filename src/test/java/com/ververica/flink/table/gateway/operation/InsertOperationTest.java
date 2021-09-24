/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.table.gateway.operation;

import com.ververica.flink.table.gateway.rest.result.ResultKind;
import com.ververica.flink.table.gateway.rest.result.ResultSet;

import org.apache.flink.table.api.TableEnvironment;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link InsertOperation}.
 */
public class InsertOperationTest extends OperationTestBase {

	@Before
	public void setup() throws Exception {
		super.setup();
		TableEnvironment tableEnv = context.getExecutionContext().getTableEnvironment();
		tableEnv.executeSql("CREATE TABLE tbl_a (`a` STRING) with ('connector' = 'blackhole')");
	}

	@Test
	public void testInsertBatch() {
		InsertOperation insertOperation = new InsertOperation(context, "insert into tbl_a select 'a'", "insert");
		ResultSet resultSet = insertOperation.execute();
		assertEquals(resultSet.getResultKind(), ResultKind.SUCCESS_WITH_JOB);
	}
}
