package org.apache.virgil.triggers;

import org.apache.cassandra.db.RowMutation;

public interface Trigger {

    public void process(RowMutation rowMutation);
    
}
