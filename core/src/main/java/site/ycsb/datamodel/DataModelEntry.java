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

public final class DataModelEntry {
    private final String name;
    private final boolean isSecondaryIndex;
    private final DataType type;
    DataModelEntry(String name, boolean isSecondaryIndex, DataType type) {
        this.name = name;
        this.isSecondaryIndex = isSecondaryIndex;
        this.type = type;
    }
    public boolean isNumeric() {
        return type == DataType.NUMERIC;
    }
    public boolean isSecondaryIndex() {
        return isSecondaryIndex;
    }
    public String getFieldName() {
        return name;
    }
}