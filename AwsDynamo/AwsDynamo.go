package AwsDynamo

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/mhthrh/Aws-Dynamo/Entity"
)

type DbHandler struct {
	Db *dynamodb.DynamoDB
}

func New() DbHandler {
	var db DbHandler
	awsSession := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	db.Db = dynamodb.New(awsSession)
	return db
}
func (d *DbHandler) CreateTable() error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("ID"),
				AttributeType: aws.String("N"),
			},
			{
				AttributeName: aws.String("Title"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("ID"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("Title"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: aws.String(Entity.TableName),
	}
	create, err := d.Db.CreateTable(input)
	if err != nil {
		return err
	}
	fmt.Println(create)
	return nil
}
func (d *DbHandler) LoadTables() ([]string, error) {
	var tables []string
	input := &dynamodb.ListTablesInput{}

	for {
		result, err := d.Db.ListTables(input)
		if err != nil {
			return tables, err
		}
		for _, n := range result.TableNames {
			tables = append(tables, *n)
		}

		if result.LastEvaluatedTableName == nil {
			break
		}
	}
	return tables, nil
}
func (d DbHandler) Insert(i interface{}) error {
	a, err := dynamodbattribute.MarshalMap(i)
	if err != nil {
		return err
	}

	_, err = d.Db.PutItem(&dynamodb.PutItemInput{
		Item:      a,
		TableName: &Entity.TableName,
	})

	if err != nil {
		return err
	}
	return nil
}
func (d DbHandler) Select() ([]interface{}, error) {
	var retn []interface{}
	filter := expression.Name("ID").Equal(expression.Value(100)) // where clause in SQL
	query := expression.NamesList(expression.Name("ID"), expression.Name("Title"), expression.Name("Name"))
	expr, err := expression.NewBuilder().WithFilter(filter).WithProjection(query).Build()
	if err != nil {
		return nil, err
	}
	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 &Entity.TableName,
	}
	result, err := d.Db.Scan(params)
	if err != nil {
		return nil, err
	}

	for _, item := range result.Items {
		retn = append(retn, item)
	}
	return retn, nil
}
func (d DbHandler) Update(set, id, title string) error {

	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":r": {
				S: &set,
			},
		},
		TableName: &Entity.TableName,
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				N: &id,
			},
			"Title": {
				S: &title,
			},
		},
		ReturnValues:     aws.String("UPDATED_NEW"),
		UpdateExpression: aws.String("set FirstName = :r"),
	}

	if _, err := d.Db.UpdateItem(input); err != nil {
		return err
	}
	return nil
}
func (d DbHandler) Delete(id, title string) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				N: &id,
			},
			"Title": {
				S: &title,
			},
		},
		TableName: &Entity.TableName,
	}

	_, err := d.Db.DeleteItem(input)
	if err != nil {
		return err
	}
	return nil
}
