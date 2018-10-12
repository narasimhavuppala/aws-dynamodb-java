/*
 * Copyright 2010-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.samples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//AWS API imports
import com.amazonaws.AmazonClientException;
import com.amazonaws.domain.Person;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

/**
 * This sample demonstrates how to make basic requests to Amazon DynamoDB using
 * the AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use DynamoDB
 * <p>
 * <b>Important:</b> Be sure to fill in your AWS access credentials in
 * ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows
 * users) before you try to run this sample.
 */
public class DynamoDBSample {
    static final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    static DynamoDB dynamoDB = new DynamoDB(client);

   /*
     * Create your credentials file at ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows users)
     * and save the following lines after replacing the underlined values with your own.
     * [default]
     * aws_access_key_id = YOUR_ACCESS_KEY_ID
     * aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
     */
    public static void main(String[] args) throws IOException {
        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon DynamoDB");
        System.out.println("===========================================\n");
        String tableName = "Person";

        try {
            createExampleTable(tableName);
            printTableInfo(tableName);
            testCrudOperations();
            listMyTables();
            updateExampleTable(tableName);
            deleteTable(tableName);
            System.out.println("============= END ==========================");

        } catch (Exception ace) {
            System.out.println("Error Message: " + ace.getMessage());

        }//end catch

    }//end main

    static void createExampleTable(String tableName) {
        try {
            CreateTableRequest request = new CreateTableRequest();
            request.setTableName(tableName);

            //Read and write thoroughput.
            ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput();
            provisionedThroughput.setReadCapacityUnits(5L);
            provisionedThroughput.setWriteCapacityUnits(5L);
            request.setProvisionedThroughput(provisionedThroughput);

            //Attribute definitions.
            //Dont put non key attributes in the attrib definitions since its schemaless
            List<AttributeDefinition> attributes = new ArrayList<AttributeDefinition>();
            attributes.add(new AttributeDefinition("id", ScalarAttributeType.N));

            request.setAttributeDefinitions(attributes);

            //Pk
            List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement("id", KeyType.HASH));
            request.setKeySchema(keySchema);

            //Local secondary index.
            /**List<KeySchemaElement> indexKeySchema = new ArrayList<KeySchemaElement>();
            indexKeySchema.add(new KeySchemaElement("id", KeyType.HASH));
            indexKeySchema.add(new KeySchemaElement("age", KeyType.RANGE));

            List<LocalSecondaryIndex> localSecondaryIndexes = new ArrayList<LocalSecondaryIndex>();
            localSecondaryIndexes.add(new LocalSecondaryIndex().withIndexName("age_index").withKeySchema(indexKeySchema)
                    .withProjection(new Projection().withProjectionType(ProjectionType.ALL)));
            request.setLocalSecondaryIndexes(localSecondaryIndexes); **/

            Table table = dynamoDB.createTable(request);
            System.out.println("Waiting for table " + tableName + " to be created...this may take a while...");
            table.waitForActive();
            System.out.println("Table :" + tableName + " created successfully.");
        }
        catch (Exception e) {
            System.err.println("CreateTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }

    }//end createTable

    /**
     * Add table items.
     */
    static void testCrudOperations(){
        System.out.println("===========================================");
        System.out.println("Testing CRUD operations");
        //Save the item (person).
        DynamoDBMapper mapper = new DynamoDBMapper(client);
        Person person = new Person();
        person.setId(1);
        person.setName("Derek Smith");
        person.setAge(42);
        mapper.save(person);
        System.out.println("Item saved successfully");

       // Retrieve the item.
        Person itemRetrieved = mapper.load(Person.class, 1);
        System.out.println("Item retrieved. Name: " + itemRetrieved.getName() + " ID :" +  itemRetrieved.getId() + " Age :" + itemRetrieved.getAge());

        // Update the item.
        itemRetrieved.setName("Kyle Smith");
        mapper.save(itemRetrieved);
        System.out.println("Item updated. Name: " + itemRetrieved.getName() + " ID :" +  itemRetrieved.getId() + " Age :" + itemRetrieved.getAge() );

        // Retrieve the updated item.
        DynamoDBMapperConfig config = new DynamoDBMapperConfig(DynamoDBMapperConfig.ConsistentReads.CONSISTENT);
        Person updatedItem = mapper.load(Person.class, 1, config);
        System.out.println("Retrieved the previously updated item:");
        System.out.println("Updated item retrieved. Name: " + updatedItem.getName() + " ID :" +  updatedItem.getId() +  " Age :" + updatedItem.getAge());

        // Delete the item.
        mapper.delete(updatedItem);

        // Try to retrieve deleted item.
        Person deletedItem = mapper.load(Person.class, updatedItem.getId(), config);
        if (deletedItem == null) {
            System.out.println("Done - Person item is deleted.");
        }


    }//end testCrudOperations


    /**
     * Update Table properties.
     */
    static void updateExampleTable(String tableName) {
        Table table = dynamoDB.getTable(tableName);
        System.out.println("===========================================");
        System.out.println("Modifying provisioned throughput for:" + tableName );
        try {
            table.updateTable(new ProvisionedThroughput().withReadCapacityUnits(6L).withWriteCapacityUnits(7L));
            table.waitForActive();
            printTableInfo(tableName);
        }
        catch (Exception e) {
            System.err.println("UpdateTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
    }//end updateExampleTable

    /**
     * List all tables.
     */
    static void listMyTables() {
        TableCollection<ListTablesResult> tables = dynamoDB.listTables();
        Iterator<Table> iterator = tables.iterator();
        System.out.println("===========================================");
        System.out.println("Listing table names");
        while (iterator.hasNext()) {
            Table table = iterator.next();
            System.out.println("Table : " + table.getTableName());
        }
    }

    /**
     * Delete table.
     * @param tableName
     */
    private static void deleteTable(String tableName) {
        Table table = dynamoDB.getTable(tableName);
        try {
            System.out.println("===========================================");
            System.out.println("Issuing DeleteTable request for:" + tableName);
            table.delete();
            System.out.println("Waiting for " + tableName + " to be deleted...this may take a while...");
            table.waitForDelete();
            System.out.println("Table " + tableName + " deleted successfully.");

        }
        catch (Exception e) {
            System.err.println("DeleteTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }

    }//end delete table.

    /**
     * Print table metadata
     */
    private static void printTableInfo(String table_name) {
        TableDescription table_info = client.describeTable(table_name).getTable();
        System.out.println("===========================================");
        System.out.println("Table info for : " +  table_name );
        if (table_info != null) {
            System.out.format("Table name  : %s\n",
                    table_info.getTableName());
            System.out.format("Table ARN   : %s\n",
                    table_info.getTableArn());
            System.out.format("Status      : %s\n",
                    table_info.getTableStatus());
            System.out.format("Item count  : %d\n",
                    table_info.getItemCount().longValue());
            System.out.format("Size (bytes): %d\n",
                    table_info.getTableSizeBytes().longValue());

            ProvisionedThroughputDescription throughput_info =
                    table_info.getProvisionedThroughput();
            System.out.println("Throughput");
            System.out.format("  Read Capacity : %d\n",
                    throughput_info.getReadCapacityUnits().longValue());
            System.out.format("  Write Capacity: %d\n",
                    throughput_info.getWriteCapacityUnits().longValue());


            List<AttributeDefinition> attributes =
                    table_info.getAttributeDefinitions();
            System.out.println("Attributes");
            for (AttributeDefinition a : attributes) {
                System.out.format("  %s (%s)\n",
                        a.getAttributeName(), a.getAttributeType());
            }
        }//table_info null check end

    }



}
