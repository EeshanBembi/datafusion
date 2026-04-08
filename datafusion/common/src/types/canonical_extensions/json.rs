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
use arrow_schema::extension::ExtensionType;

/// Defines the extension type logic for the canonical `arrow.json` extension type.
///
/// JSON values are already stored as UTF-8 strings, so the default Arrow string
/// formatter is used for display.
///
/// See [`DFExtensionType`] for information on DataFusion's extension type mechanism.
impl DFExtensionType for arrow_schema::extension::Json {
    fn storage_type(&self) -> DataType {
        // JSON can be stored as Utf8, LargeUtf8, or Utf8View; Utf8 is the most common default.
        DataType::Utf8
    }

    fn serialize_metadata(&self) -> Option<String> {
        <arrow_schema::extension::Json as ExtensionType>::serialize_metadata(self)
    }
}
