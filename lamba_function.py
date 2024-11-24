import boto3
import csv
import json
import io

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    # Replace with your bucket name and file key
    bucket_name = "my-data-api"
    object_key = "static-websites/file.json"

    try:
        # Fetch the JSON file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        json_content = response['Body'].read().decode('utf-8')

        # Parse the JSON data
        data = json.loads(json_content)

        # Validate JSON is an array of objects
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            return {
                'statusCode': 400,
                'body': "Invalid JSON format. Must be an array of objects."
            }

        # Convert JSON to CSV
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)

        # Write headers
        headers = data[0].keys()
        csv_writer.writerow(headers)

        # Write rows
        for item in data:
            csv_writer.writerow(item.values())

        # Return CSV as response
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/csv',
                'Content-Disposition': 'attachment; filename="export.csv"'
            },
            'body': csv_buffer.getvalue()
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error processing file: {str(e)}"
        }
