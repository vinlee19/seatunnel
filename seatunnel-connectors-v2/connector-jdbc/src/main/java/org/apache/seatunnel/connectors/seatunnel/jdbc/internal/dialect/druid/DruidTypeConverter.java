/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.druid;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(TypeConverter.class)
public class DruidTypeConverter implements TypeConverter<BasicTypeDefine> {
    // Druid treats timestamps (including the __time column) as LONG,
    // with the value being the number of milliseconds
    // since 1970-01-01 00:00:00 UTC, not counting leap seconds.
    public static final int DRUID_TIMESTAMP_SCALE = 3;
    public static final long MAX_STRING_LENGTH = 0x7fffffff - 4;
    public static final String DRUID_TIMESTAMP = "TIMESTAMP";
    public static final String DRUID_FLOAT = "FLOAT";
    public static final String DRUID_DOUBLE = "DOUBLE";
    public static final String DRUID_BIGINT = "BIGINT";

    public static final String DRUID_VARCHAR = "VARCHAR";
    public static final String DRUID_JSON = "COMPELX<JSON>";

    public static final DruidTypeConverter INSTANCE = new DruidTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.DRUID;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        String druidType = typeDefine.getDataType().toUpperCase();

        switch (druidType) {
            case DRUID_BIGINT:
                builder.sourceType(DRUID_BIGINT);
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case DRUID_FLOAT:
                builder.sourceType(DRUID_FLOAT);
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case DRUID_DOUBLE:
                builder.sourceType(DRUID_DOUBLE);
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case DRUID_TIMESTAMP:
                builder.sourceType(String.format("%s(%d)", DRUID_TIMESTAMP, DRUID_TIMESTAMP_SCALE));
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(DRUID_TIMESTAMP_SCALE);
                break;

            case DRUID_VARCHAR:
            case DRUID_JSON:
                builder.sourceType(DRUID_VARCHAR);
                if (typeDefine.getLength() == -1) {
                    builder.columnLength(MAX_STRING_LENGTH);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(BasicType.STRING_TYPE);
                break;
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.DRUID, druidType, typeDefine.getName());
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());

        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
                builder.columnType(DRUID_BIGINT);
                builder.dataType(DRUID_BIGINT);
                break;
            case FLOAT:
                builder.columnType(DRUID_FLOAT);
                builder.dataType(DRUID_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(DRUID_DOUBLE);
                builder.dataType(DRUID_DOUBLE);
                break;
            case DECIMAL:
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP:
                builder.columnType(DRUID_VARCHAR);
                builder.dataType(DRUID_VARCHAR);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.DRUID,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }
}
