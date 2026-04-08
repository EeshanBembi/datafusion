// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::types::extension::DFExtensionType;
use arrow::datatypes::DataType;

/// Defines the extension type logic for the canonical `arrow.timestamp_with_offset` extension type.
///
/// Timestamp with offset values are stored as `Struct` arrays containing a UTC timestamp
/// and an offset in minutes. The default Arrow formatter is used for display.
///
/// See [`DFExtensionType`] for information on DataFusion's extension type mechanism.
impl DFExtensionType for arrow_schema::extension::TimestampWithOffset {
    fn storage_type(&self) -> DataType {
        // TimestampWithOffset stores no internal state to determine the timestamp precision.
        // The actual storage type depends on the time unit chosen by the producer.
        // Returning Null here is a placeholder; the actual DataType is validated at registration
        // time via ExtensionType::supports_data_type.
        DataType::Null
    }

    fn serialize_metadata(&self) -> Option<String> {
        // TimestampWithOffset has no metadata.
        None
    }
}
