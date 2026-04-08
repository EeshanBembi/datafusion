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

use arrow::array::{FixedSizeBinaryArray, Int8Array, RecordBatch, StringArray};
use arrow_schema::extension::{Bool8, Json, Uuid};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_expr::registry::MemoryExtensionTypeRegistry;
use insta::assert_snapshot;
use std::sync::Arc;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![uuid_field()]))
}

fn uuid_field() -> Field {
    Field::new("my_uuids", DataType::FixedSizeBinary(16), false).with_extension_type(Uuid)
}

async fn create_test_table() -> Result<DataFrame> {
    let schema = test_schema();

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(FixedSizeBinaryArray::from(vec![
            &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 5, 6],
        ]))],
    )?;

    let state = SessionStateBuilder::default()
        .with_extension_type_registry(Arc::new(
            MemoryExtensionTypeRegistry::new_with_canonical_extension_types(),
        ))
        .build();
    let ctx = SessionContext::new_with_state(state);

    ctx.register_batch("test", batch)?;

    ctx.table("test").await
}

#[tokio::test]
async fn test_pretty_print_extension_type_formatter() -> Result<()> {
    let result = create_test_table().await?.to_string().await?;

    assert_snapshot!(
        result,
        @r"
    +--------------------------------------+
    | my_uuids                             |
    +--------------------------------------+
    | 00000000-0000-0000-0000-000000000000 |
    | 00010203-0405-0607-0809-000102030506 |
    +--------------------------------------+
    "
    );

    Ok(())
}

fn bool8_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("my_bools", DataType::Int8, false).with_extension_type(Bool8),
    ]))
}

async fn create_bool8_test_table() -> Result<DataFrame> {
    let schema = bool8_test_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int8Array::from(vec![0, 1, 42, -1]))],
    )?;

    let state = SessionStateBuilder::default()
        .with_extension_type_registry(Arc::new(
            MemoryExtensionTypeRegistry::new_with_canonical_extension_types(),
        ))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_batch("test", batch)?;
    ctx.table("test").await
}

#[tokio::test]
async fn test_pretty_print_bool8() -> Result<()> {
    let result = create_bool8_test_table().await?.to_string().await?;

    assert_snapshot!(
        result,
        @r"
    +----------+
    | my_bools |
    +----------+
    | false    |
    | true     |
    | true     |
    | true     |
    +----------+
    "
    );

    Ok(())
}

fn json_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("my_json", DataType::Utf8, false).with_extension_type(Json::default()),
    ]))
}

async fn create_json_test_table() -> Result<DataFrame> {
    let schema = json_test_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            r#"{"key": "value"}"#,
            r#"[1, 2, 3]"#,
        ]))],
    )?;

    let state = SessionStateBuilder::default()
        .with_extension_type_registry(Arc::new(
            MemoryExtensionTypeRegistry::new_with_canonical_extension_types(),
        ))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_batch("test", batch)?;
    ctx.table("test").await
}

#[tokio::test]
async fn test_pretty_print_json() -> Result<()> {
    let result = create_json_test_table().await?.to_string().await?;

    assert_snapshot!(
        result,
        @r#"
    +------------------+
    | my_json          |
    +------------------+
    | {"key": "value"} |
    | [1, 2, 3]        |
    +------------------+
    "#
    );

    Ok(())
}
