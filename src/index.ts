import { 
  APIGatewayProxyEvent, 
  APIGatewayProxyResult 
} from "aws-lambda";
import * as AWS from "aws-sdk";

AWS.config.update({
  region: "ca-central-1"
});
const dynamodb = new AWS.DynamoDB.DocumentClient();

const DYNAMODB_TABLE_PREFIX = "rpi-temperature-";
const TEMPERATURE_PATH = "/temperature";
const TEMPERATURES_PATH = "/temperatures";
const HEALTH_PATH = "/health";

const LOCATION_COLUMN = "location"
const TIME_COLUMN = "time"





// function getDateFormattedFromTimestamp(dateTimestamp: string): string {
//   function join(t: Date, a: any, s: string) {
//     function format(m: any) {
//        let f = new Intl.DateTimeFormat('en', m);
//        return f.format(t);
//     }
//     return a.map(format).join(s);
//   }

//   const dateFormat = [{year: "numeric"}, {month: "numeric"}, {day: "numeric"}];
//   let dateString = join(new Date(dateTimestamp), dateFormat, "-");
//   const values = dateString.split("-");
//   const valuesWithLeadingZeros = [];

//   for (let value in values) {
//     valuesWithLeadingZeros.push(value.length < 2 ? `0${value}` : value);
//   }
//   dateString = valuesWithLeadingZeros.join("-");
//   return dateString;
// }


async function scanDynamoRecords(scanParams: any, itemArray: any, depth: number = 0): Promise<any> {
  console.log(`Scanning dynamodb at depth ${depth}`);
  try {
    const dynamoData = await dynamodb.scan(scanParams).promise();
    console.log(`Dynamo Data`, dynamoData);
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
  } catch(error) {
    console.error('Do your custom error handling here. I am just gonna log it: ', error);
  }
}

// async function queryDynamoRecords()


async function getMostRecentTemperature(date: string, location: string): Promise<APIGatewayProxyResult> {
  console.log(`Get most recent temperature for date ${date} and location ${location}`)
  // const dateTime: Date = new Date((new Date(parseInt(date))).toLocaleString("en-US", { timeZone: 'America/Vancouver' }));
  // console.log("Timezone: ", dateTime.toLocaleString("en-US", { timeZoneName: "short" }));
  // const dateISOString: string = dateTime.toISOString().slice(0, 10);
  // console.log(`Date ISO String ${dateISOString}`)
  let params = {
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


function getTemperatures(startDateTimestamp: string, endDateTimestamp: string, location: string = ""): APIGatewayProxyResult {
  // const startDateString = getDateFormattedFromTimestamp(startDateTimestamp);
  // const endDateString = getDateFormattedFromTimestamp(endDateTimestamp);
  return buildResponse(200);
}


export const handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  try {
    const queries = JSON.stringify(event.queryStringParameters);
    let response: APIGatewayProxyResult;
    const params = event.queryStringParameters;
  
    switch(true) {
      case event.httpMethod === "GET" && event.path === HEALTH_PATH:
        response = buildResponse(200);
        break;
      case event.httpMethod === "GET" && event.path === TEMPERATURE_PATH:
        response = await getMostRecentTemperature(params.date, params.location);
        break;
      case event.httpMethod === "GET" && event.path === TEMPERATURES_PATH:
        response = await getTemperatures(params.startDateTime, params.endDateTime, params?.location);
        break;
      default:
        response = buildResponse(404, `${event.httpMethod} ${event.path} not found`);
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