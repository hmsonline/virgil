//
// Copyright (c) 2012 Health Market Science, Inc.
//
package com.hmsonline.virgil;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href=irieksts@healthmarketscience.com>Isaac Rieksts</a>
 *
 */
public class Query {
  private Map<String, String> eqStmt;

  public void addEq(String key, String value) {
    getEqStmt().put(key, value);
  }
  
  /**
   * @return the eqStmt
   */
  public Map<String, String> getEqStmt() {
    eqStmt = eqStmt != null? eqStmt : new HashMap<String, String>();
    return eqStmt;
  }

  /**
   * @param eqStmt the eqStmt to set
   */
  public void setEqStmt(Map<String, String> eqStmt) {
    this.eqStmt = eqStmt;
  }
}
