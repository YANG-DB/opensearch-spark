/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import com.google.flatbuffers.FlexBuffers;
import lombok.val;
import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OptionList;
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedTableSpec;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.DataSourceType;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.statement.ProjectStatement;
import org.opensearch.sql.ppl.CatalystPlanContext;
import scala.Option;

import java.util.Optional;

import static java.util.Collections.emptyMap;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.map;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.option;
import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;

public interface ProjectionUtils {

    /**
     * build a CreateTableAsSelect operator base on the ProjectStatement node
     *         
     *    'CreateTableAsSelect [identity(age)], unresolvedtablespec(Some(parquet), optionlist(), None, None, None, false), false, false
     *           :- 'UnresolvedIdentifier [student_partition_bucket], false
     *                             - 'Project [*]
     *                                 - 'UnresolvedRelation [spark_catalog, default, flint_ppl_test], [], false
     * */
    static CreateTableAsSelect visitProject(LogicalPlan plan, ProjectStatement node, CatalystPlanContext context) {
        Optional<String> using = node.getUsing().map(Enum::name);
        Optional<UnresolvedExpression> options = node.getOptions();
        Optional<UnresolvedExpression> partitionColumns = node.getPartitionColumns();
        partitionColumns.map(Node::getChild);
//        new IdentityTransform(new FieldReference(partitionColumns));

        Optional<UnresolvedExpression> location = node.getLocation();
        UnresolvedIdentifier name = new UnresolvedIdentifier(seq(node.getTableQualifiedName().getParts()), false);
        UnresolvedTableSpec tableSpec = new UnresolvedTableSpec(map(emptyMap()), option(using), new OptionList(seq()), Option.empty(), Option.empty(), Option.empty(), false);
        return new CreateTableAsSelect(name, seq(), plan, tableSpec, map(emptyMap()), !node.isOverride(), false);   
    }
}
