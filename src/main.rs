use aws_sdk_dynamodb::{Client, Error};
use aws_sdk_dynamodb::model::{AttributeValue, Select};
use std::{process};

struct QueryParam {
    table: String,
    key: String,
    id: String,
}

async fn query(client: &Client, param: QueryParam) -> bool {
    let key = &param.key;
    let id = &param.id;

    match client
        .query()
        .table_name(param.table)
        .key_condition_expression("#key = :value".to_string())
        .expression_attribute_names("#key".to_string(), key.to_string())
        .expression_attribute_values(":value".to_string(), AttributeValue::N(id.to_string()))
        .select(Select::AllAttributes)
        .send()
        .await
    {
        Ok(resp) => {
            if resp.count > 0 {
                println!("Found a matching entry in the table:");
                // println!("{:?}", resp.items.unwrap_or_default());

                match resp.items {
                    Some(items) => {
                        for item in items {
                            println!("{:?}", item);

                            match item.get(&*"DataType") {
                                Some(type_attr) => {
                                    // println!("{:?}", type_attr.as_s());
                                    match type_attr.as_s() {
                                        Ok(data_type) => {
                                            // println!("{:?}", data_type);

                                            if Some(0) == data_type.find("Area_") {
                                                match item.get(&*"DataValue") {
                                                    Some(value_attr) => {
                                                        match value_attr.as_s() {
                                                            Ok(data_value) => {
                                                                println!("{:?}", data_value);
                                                            },
                                                            Err(_) => {}
                                                        }
                                                    },
                                                    _ => {}
                                                }
                                            }

                                            match data_type.as_str() {
                                                "Name" => {
                                                    match item.get(&*"DataValue") {
                                                        Some(value_attr) => {
                                                            match value_attr.as_s() {
                                                                Ok(data_value) => {
                                                                    println!("{:?}", data_value);
                                                                },
                                                                Err(_) => {}
                                                            }
                                                        },
                                                        _ => {}
                                                    }
                                                },
                                                "Elevation" => {
                                                    match item.get(&*"ElevationValue") {
                                                        Some(value_attr) => {
                                                            match value_attr.as_n() {
                                                                Ok(data_value) => {
                                                                    println!("{:?}", data_value);
                                                                },
                                                                Err(_) => {}
                                                            }
                                                        },
                                                        _ => {}
                                                    }
                                                },
                                                _ => {}
                                            }
                                        }
                                        Err(_) => {}
                                    }
                                },
                                _ => {}
                            }
                        }
                    },
                    _ => {}
                }

                // for item in resp.items {
                //     println!("{:?}", item);
                // }

                true
            } else {
                println!("Did not find a match.");
                false
            }
        }
        Err(e) => {
            println!("Got an error querying table:");
            println!("{}", e);
            process::exit(1);
        }
    }
}

struct QueryIndexParam {
    table: String,
    key: String,
    value: String,
    index_name: String
}

async fn query_index(client: &Client, param: QueryIndexParam) -> bool {
    let key = &param.key;
    let value = &param.value;

    match client
        .query()
        .table_name(param.table)
        .index_name(param.index_name)
        .key_condition_expression("#key = :value".to_string())
        .expression_attribute_names("#key".to_string(), key.to_string())
        .expression_attribute_values(":value".to_string(), AttributeValue::S(value.to_string()))
        .select(Select::AllAttributes)
        .send()
        .await
    {
        Ok(resp) => {
            if resp.count > 0 {
                println!("count: {}", resp.count);
                true
            } else {
                println!("resp.count = 0");
                false
            }
        },
        Err(e) => {
            println!("Got an error querying table:");
            println!("{}", e);
            process::exit(1);
        }
    }
}



#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = aws_config::load_from_env().await;
    let client = Client::new(&config);

    // scan
    let resp = client.scan().table_name("Mountains").send().await?;
    println!("Items in table:");
    if let Some(items) = resp.items {
        println!("   {:?}", items.len());
    }

    // query
    let query_param = QueryParam {
        table: "Mountains".to_string(),
        key: "Id".to_string(),
        id: "5".to_string(),
    };

    query(&client, query_param).await;

    // query index
    let query_index_param = QueryIndexParam {
        table: "Mountains".to_string(),
        key: "DataValue".to_string(),
        value: "Tag_百名山".to_string(),
        index_name: "DataValue_Id_Index".to_string()
    };

    query_index(&client, query_index_param).await;

    Ok(())
}
