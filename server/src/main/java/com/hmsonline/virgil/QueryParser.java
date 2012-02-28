//
// Copyright (c) 2012 Health Market Science, Inc.
//
package com.hmsonline.virgil;

import org.apache.commons.lang.StringUtils;

/**
 * @author <a href=irieksts@healthmarketscience.com>Isaac Rieksts</a>
 *
 */
public class QueryParser {
  private static final String AND = " AND ";
  private static final String EQ_DELIM = ":";
  
  public static Query parse(String query){
    String [] peaces;
    Query q = new Query();
    if(StringUtils.isBlank(query)) {
      return q;
    }
    
    if(StringUtils.indexOf(query, AND) > -1) {
      peaces = StringUtils.split(query, AND);
    }
    else {
      peaces = new String [] {query};
    }
    
    for(String peace : peaces) {
      if(peace != null && peace.indexOf(EQ_DELIM) > -1) {
        String [] kv = peace.split(EQ_DELIM);
        q.addEq(StringUtils.trim(kv[0]), StringUtils.trim(kv[1]));
      }
    }
    
    return q;
  }
}
