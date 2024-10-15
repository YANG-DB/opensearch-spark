/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression.subquery;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

import java.util.List;

@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class ExistsSubquery extends UnresolvedExpression {
    private final UnresolvedPlan query;

    @Override
    public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
        return nodeVisitor.visitExistsSubquery(this, context);
    }
}
