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

import com.ververica.flink.table.gateway.context.ExecutionContext;
import com.ververica.flink.table.gateway.context.SessionContext;
import com.ververica.flink.table.gateway.deployment.ClusterDescriptorAdapterFactory;
import com.ververica.flink.table.gateway.rest.result.ColumnInfo;
import com.ververica.flink.table.gateway.rest.result.ConstantNames;
import com.ververica.flink.table.gateway.rest.result.ResultKind;
import com.ververica.flink.table.gateway.rest.result.ResultSet;
import com.ververica.flink.table.gateway.utils.SqlExecutionException;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Operation for INSERT command.
 */
public class InsertOperation extends AbstractJobOperation {
	private static final Logger LOG = LoggerFactory.getLogger(InsertOperation.class);

	private final String statement;
	// insert into sql match pattern
	private static final Pattern INSERT_SQL_PATTERN = Pattern.compile("(INSERT\\s+(INTO|OVERWRITE).*)",
		Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

	private final List<ColumnInfo> columnInfos;

	private boolean fetched = false;

	public InsertOperation(SessionContext context, String statement, String tableIdentifier) {
		super(context);
		this.statement = statement;

		this.columnInfos = Collections.singletonList(
			ColumnInfo.create(tableIdentifier, new BigIntType(false)));
	}

	@Override
	public ResultSet execute() {
		jobId = context.getExecutionContext().wrapClassLoader(() -> executeUpdateInternal(context.getExecutionContext()));
		String strJobId = jobId.toString();
		return ResultSet.builder()
			.resultKind(ResultKind.SUCCESS_WITH_JOB)
			.columns(ColumnInfo.create(ConstantNames.JOB_ID, new VarCharType(false, strJobId.length())))
			.data(Row.of(strJobId))
			.build();
	}

	@Override
	protected Optional<Tuple2<List<Row>, List<Boolean>>> fetchNewJobResults() {
		if (fetched) {
			return Optional.empty();
		} else {
			// for session mode, we can get job status from JM, because JM is a long life service.
			// while for per-job mode, JM will be also destroy after the job is finished.
			// so it's hard to say whether the job is finished/canceled
			// or the job status is just inaccessible at that moment.
			// currently only yarn-per-job is supported,
			// and if the exception (thrown when getting job status) contains ApplicationNotFoundException,
			// we can say the job is finished.
			boolean isGloballyTerminalState = clusterDescriptorAdapter.isGloballyTerminalState();

			if (isGloballyTerminalState) {
				// TODO get affected_row_count for batch job
				fetched = true;
				return Optional.of(Tuple2.of(Collections.singletonList(
					Row.of((long) Statement.SUCCESS_NO_INFO)), null));
			} else {
				// TODO throws exception if the job fails
				return Optional.of(Tuple2.of(Collections.emptyList(), null));
			}
		}
	}

	@Override
	protected List<ColumnInfo> getColumnInfos() {
		return columnInfos;
	}

	@Override
	protected void cancelJobInternal() {
		clusterDescriptorAdapter.cancelJob();
	}

	private <C> JobID executeUpdateInternal(ExecutionContext<C> executionContext) {
		TableEnvironment tableEnv = executionContext.getTableEnvironment();

		if (!INSERT_SQL_PATTERN.matcher(statement.trim()).matches()) {
			LOG.error("Session: {}. Only insert is supported now.", sessionId);
			throw new SqlExecutionException("Only insert is supported now");
		}

		String jobName = getJobName(statement);

		TableConfig tableConfig = tableEnv.getConfig();
		Configuration tblEnvConfig = tableConfig.getConfiguration();
		tblEnvConfig.set(PipelineOptions.NAME, jobName);

		// blocking deployment
		try {
			TableResult result;
			result = executionContext.wrapClassLoader(() -> tableEnv.executeSql(statement));
			JobClient jobClient = result.getJobClient().get();
			JobID jobID = jobClient.getJobID();
			this.clusterDescriptorAdapter =
					ClusterDescriptorAdapterFactory.create(context.getExecutionContext(), tblEnvConfig, sessionId, jobID);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Cluster Descriptor Adapter: {}", clusterDescriptorAdapter);
			}

			LOG.info("Session: {}. Submit flink job {} successfully, query: {}", sessionId, jobID.toString(), statement);

			return jobID;
		} catch (SqlParserException e) {
			LOG.error(String.format("Session: %s. Invalid SQL query.", sessionId), e);
			throw new SqlExecutionException("Invalid SQL statement.", e);
		} catch (Throwable e) {
			// catch everything such that the statement does not crash the executor
			throw new SqlExecutionException("Error running SQL job.", e);
		} finally {
			// restore to the origin configuration
			tblEnvConfig.removeConfig(PipelineOptions.NAME);
		}
	}
}
