import { 
  APIGatewayProxyEvent, 
  APIGatewayProxyResult 
} from "aws-lambda";
import * as AWS from "aws-sdk";
import { ConfigurationServicePlaceholders } from "aws-sdk/lib/config_service_placeholders";

AWS.config.update({
  region: "ca-central-1"
});
const dynamodbDocumentClient = new AWS.DynamoDB.DocumentClient();
const dynamodbClient = new AWS.DynamoDB();

const DYNAMODB_TABLE_PREFIX = "rpi-temperature-";
const LOCATIONS_PATH = "/locations";
const TEMPERATURE_PATH = "/temperature";
const TEMPERATURES_PATH = "/temperatures";
const HEALTH_PATH = "/health";

const LOCATION_COLUMN = "location"
const TIME_COLUMN = "time"

interface Temperature {
  value: number;
  date: Date;
  location?: string;
}

const MS_PER_DAY = 1000 * 60 * 60 * 24;


export function getISODateStringFromDate(date: Date): string {
  let year = `${date.getFullYear()}`;
  let month = `${date.getMonth() + 1}`.length === 1 ? `0${date.getMonth() + 1}` : `${date.getMonth() + 1}`;
  let day = `${date.getDate()}`.length === 1 ? `0${date.getDate()}` : `${date.getDate()}`;
  return `${year}-${month}-${day}`
}

function getTimeStringFromDate(date: Date): string {
  return date.toLocaleTimeString("us-EN", {timeZone: "America/Vancouver"}).toLowerCase()
}

function getDifferenceBetweenDays(a: Date, b: Date): number {
  const utc1 = Date.UTC(a.getFullYear(), a.getMonth(), a.getDate());
  const utc2 = Date.UTC(b.getFullYear(), b.getMonth(), b.getDate());
  return Math.floor((utc2 - utc1) / MS_PER_DAY);
}

function subtractDaysFromDate(date: Date, days: number): Date {
  if (days == 0) {
    return date;
  }
  const dateCopy: Date = new Date(date);
  dateCopy.setDate(date.getDate() - 1)
  // console.log("Date subtracted to: ", dateCopy);
  return subtractDaysFromDate(dateCopy, --days);
}


async function checkIfTableExists(tableName: string): Promise<boolean> {
  const params = { TableName: tableName };
  try {
    const response = await dynamodbClient.describeTable(params).promise();
    if (response.Table.TableStatus == "ACTIVE") {
      console.log(`Table ${tableName} exists.`);
      return true;
    }
    console.log(`Table ${tableName} DOESNT exist.`);
  } catch (e) {
    console.error("checkIfTableExists error: ", e);
  }
  return false;
}


async function scanDynamoRecords(scanParams: any, itemArray: any, depth: number = 0): Promise<any> {
  console.log(`Scanning dynamodb at depth ${depth}`);
  try {
    const dynamoData = await dynamodbDocumentClient.scan(scanParams).promise();
    // console.log(`Dynamo Data`, dynamoData);
    itemArray = itemArray.concat(dynamoData.Items);
    if (scanParams.Limit && itemArray.length >= scanParams.Limit) {
      return itemArray;
    }
    if (dynamoData.LastEvaluatedKey) {
        console.log(`Performing another scan starting at ${dynamoData.LastEvaluatedKey}`);
        scanParams.ExclusiveStartkey = dynamoData.LastEvaluatedKey;
        return await scanDynamoRecords(scanParams, itemArray, depth++);
    }
    return itemArray;
  } catch (error) {
    console.error('Do your custom error handling here. I am just gonna log it: ', error);
  }
}


async function getMostRecentTemperature(date: string, location: string): Promise<APIGatewayProxyResult> {
  console.log(`Get most recent temperature for date ${date} and location ${location}`)
  // const dateTime: Date = new Date((new Date(parseInt(date))).toLocaleString("en-US", { timeZone: 'America/Vancouver' }));
  // console.log("Timezone: ", dateTime.toLocaleString("en-US", { timeZoneName: "short" }));
  // const dateISOString: string = dateTime.toISOString().slice(0, 10);
  // console.log(`Date ISO String ${dateISOString}`)
  const params = {
    TableName: DYNAMODB_TABLE_PREFIX + date,
    FilterExpression: `#locationColumn = :locationValue`,
    ExpressionAttributeValues: {
        ":locationValue": location
    },
    ExpressionAttributeNames: {
        "#locationColumn": LOCATION_COLUMN
    },
    // ScanIndexForward: false,
    // Limit: 1
  }
  const tableExists: boolean = await checkIfTableExists(params.TableName)
  if (!tableExists) {
    return buildResponse(500, `Error when checking if ${params.TableName} exists`);
  }

  console.log("Query params: ", params);
  let results = await scanDynamoRecords(params, []);
  results = results.sort((a: any, b: any) => {
    switch(true) {
      case a.time < b.time:
        return 1;
      case a.time > b.time:
        return -1;
      default:
        return 0;
    };
  });
  console.log("Results sample: ", results.length > 0 ? results.slice(0, 5) : "no results");
  return buildResponse(200, results.length > 0 ? results[0] : "");
}


async function getTemperatures(startDateString: string, endDateString: string, location: string): Promise<APIGatewayProxyResult> {
  try {
    console.log("Fetching te")
    if (!location) {
      return buildResponse(422, `Invalid location: ${location}`);
    }

    const startDate = new Date(startDateString);
    const endDate = new Date(endDateString);
    console.log("Start Date: ", startDate);
    console.log("End Date: ", endDate);
    let daysToFetch: number = getDifferenceBetweenDays(startDate, endDate)
    console.log("Days to fetch: ", daysToFetch + 1);

    if (daysToFetch < 0) {
      console.log(`Cannot fetch data for ${daysToFetch} days`);
      return buildResponse(422, `Invalid start or end date ${startDateString} ${endDateString}`);
    }

    if (daysToFetch > 6) {
      daysToFetch = 6;
      console.log(`Only fetching data for ${daysToFetch + 1} days`);
    }

    const dateStringList: string[] = [getISODateStringFromDate(endDate)];
    for (let i = 1; i <= daysToFetch; i++) {
      const resultDate: Date = subtractDaysFromDate(endDate, i)
      const resultDateString: string = getISODateStringFromDate(resultDate)
      dateStringList.push(resultDateString);
    }
    console.log("Days to fetch: ", dateStringList);

    let temperatureList: Temperature[] = [];
    for (let dateString of dateStringList) {
      const tableName = DYNAMODB_TABLE_PREFIX + dateString;
      const tableExists = await checkIfTableExists(tableName);
      if (!tableExists) {
        console.log(`Skipping temperatures for day ${dateString}`);
        continue;
      }
      const params = {
        TableName: tableName,
        FilterExpression: `#locationColumn = :locationValue`,
        ExpressionAttributeValues: {
          ":locationValue": location
        },
        ExpressionAttributeNames: {
          "#locationColumn": LOCATION_COLUMN
        }
      };
      console.log(`Scanning for ${location} temperatures for day ${dateString}`);
      temperatureList = await scanDynamoRecords(params, temperatureList);
    }

    console.log(`Results found: ${temperatureList.length}`);
    return buildResponse(200, temperatureList);

  } catch (e) {
    console.log("getTemperatures error: ", e);
    return buildResponse(500, e)
  }
}


function buildLocationsResponse(code: number, locations: string[] = []) {
  return buildResponse(code, { locations: locations });
}


async function getLocations(dateString: string): Promise<APIGatewayProxyResult> {
  console.log("getLocations running");
  const params = {
    TableName: DYNAMODB_TABLE_PREFIX + dateString
  }
  const tableExists = await checkIfTableExists(params.TableName);
  if (!tableExists) {
    console.log(`No table for date ${dateString}`);
    return buildLocationsResponse(200);
  }

  try {
    const results: Temperature[] = await scanDynamoRecords(params, []);
    console.log("Locations results: ", results);
    if (results.length < 1) {
      return buildLocationsResponse(200)
    }
    const locations: string[] = results.map((value: Temperature) =>  value.location)
    const uniqueLocations: string[] = Array.from(new Set(locations))
    return buildLocationsResponse(200, uniqueLocations.length > 0 ? uniqueLocations : [])

  } catch (e) {
    console.error("Error when scanning for locations: ", e);
    return buildResponse(500, e)
  }
  
}


export const handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    const params = event.queryStringParameters;
    let response: APIGatewayProxyResult;

    if (event.httpMethod === "GET") {
      if (event.path === LOCATIONS_PATH) {
        response = await getLocations(params.date);

      } else if (event.path === TEMPERATURE_PATH) {
        response = await getMostRecentTemperature(params.date, params.location);
      
      } else if (event.path === TEMPERATURES_PATH) {
        response = await getTemperatures(params.startDate, params.endDate, params?.location);
      
      } else if (event.path === HEALTH_PATH) {
        response = buildResponse(200);
      
      } else {
        response = buildResponse(404, `${event.httpMethod} ${event.path} not found`);
      }
    }
  
    return response;
  } catch (e) {
    return buildResponse(500, `error: ${e}`);
  }
  
}


function buildResponse(statusCode: number, body: any = {}): APIGatewayProxyResult {
  return {
    statusCode: statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Headers" : "Content-Type",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
    },
    body: JSON.stringify(body)
  };
}