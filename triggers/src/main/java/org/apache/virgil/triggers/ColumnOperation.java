package org.apache.virgil.triggers;

import java.nio.ByteBuffer;

public class ColumnOperation {
    private ByteBuffer name;
    private boolean isDelete;
    
    public ByteBuffer getName() {
        return name;
    }
    public void setName(ByteBuffer name) {
        this.name = name;
    }
    
    public boolean isDelete() {
        return isDelete;
    }
    public void setDelete(boolean isDelete) {
        this.isDelete = isDelete;
    }
}
