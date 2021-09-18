import pandas
import boto3

client = boto3.client('athena')

# function to query athena
def query_athena(query):
    DATABASE = 'athena_db_name'
    output = 's3_location_for_storing_query_results'
    RETRY_COUNT = 10
    res = []
    
    response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': DATABASE
            },
            ResultConfiguration={
                'OutputLocation': output,
            }
        )
    # get query execution id
    query_execution_id = response['QueryExecutionId']
    # get execution status
    for i in range(1, 1 + RETRY_COUNT):

        # get query execution
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            raise Exception("STATUS:" + query_execution_status)

        else:
            print("STATUS:" + query_execution_status)
            time.sleep(i)
    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        raise Exception('TIME OVER')
        
     # get query results
    response_query_result = client.get_query_results(QueryExecutionId=query_execution_id)
    res.append(response_query_result)
    
   
   
#Function to convert athena response into list format

def results_to_df(results):
    columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo'] ]
    listed_results = []
    for res in results['ResultSet']['Rows'][1:]:
        values = []
        for field in res['Data']:
            try:
                values.append(list(field.values())[0]) 
            except:
                values.append((' '))
                
        listed_results.append(dict(zip(columns, values)))

    return listed_results
    

# converting results to pandas data frame and then getting the dictionary from it
for r in res:
    x = results_to_df(r)
    df = pd.DataFrame(x)
    d = (df.T.to_dict().values())
