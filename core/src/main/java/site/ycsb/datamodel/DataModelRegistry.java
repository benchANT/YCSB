/**
 * Copyright (c) 2024 benchANT GmbH. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.datamodel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum DataModelRegistry {
    INSTANCE;
    private DataModelRegistry() {

    }
    public void addRegularField(String name) {
        this.addField(name, false, DataType.STRING);
    }
    public void addField(String name, boolean isSecondaryIndex, DataType type) {
        DataModelEntry m = new DataModelEntry(name, isSecondaryIndex, type);
        fields.put(name, m);
    }
    public DataModelEntry getField(String name) {
        return fields.get(name);
    }
    public List<DataModelEntry> findSiFields() {
        List<DataModelEntry> result = new ArrayList<>();
        for(String key : fields.keySet()) {
            DataModelEntry value = fields.get(key);
            if(value == null) continue;
            if(value.isSecondaryIndex()) {
                result.add(value);
            }
        }
        return result;
    }
    private final Map<String, DataModelEntry> fields = new HashMap<>();
}
