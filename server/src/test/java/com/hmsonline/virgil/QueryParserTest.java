//
// Copyright (c) 2012 Health Market Science, Inc.
//
package com.hmsonline.virgil;

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author <a href=irieksts@healthmarketscience.com>Isaac Rieksts</a>
 *
 */
public class QueryParserTest {

  @Test
  public void testParseBasic() {
    String q = "id:123";
    Query result = QueryParser.parse(q);
    Assert.assertEquals("Size of EQ", 1, result.getEqStmt().keySet().size());
    Assert.assertEquals("Should see one eq statement", "123", result.getEqStmt().get("id"));
  }
  
  @Test
  public void testParseTwoWithAnd() {
    String q = "id:123 AND key:abc";
    Query result = QueryParser.parse(q);
    Assert.assertEquals("Size of EQ", 2, result.getEqStmt().keySet().size());
    Assert.assertEquals("Should see id eq statement", "123", result.getEqStmt().get("id"));
    Assert.assertEquals("Should see key eq statement", "abc", result.getEqStmt().get("key"));
  }
}
