/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

import java.util.List;

/** Expression node that includes a list of Expression nodes. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
public class AttributeList extends UnresolvedExpression {
  private final List<UnresolvedExpression> attrList;

  @Override
  public List<UnresolvedExpression> getChild() {
    return ImmutableList.copyOf(attrList);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitAttributeList(this, context);
  }
}
