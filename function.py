from googleapiclient.discovery import build


def trigger_df_job(cloud_event,environment):   
 
    service = build('dataflow', 'v1b3')
    project = "mahdi-data-engineering"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "bq-load",  # Provide a unique name for the job
        "parameters": {
        "javascriptTextTransformGcsPath": "gs://mahdi-bkt-dataflow-metadata/udf.js",
        "JSONPath": "gs://mahdi-bkt-dataflow-metadata/bq.json",
        "javascriptTextTransformFunctionName": "transform",
        "outputTable": "mahdi-data-engineering.cricket_dataset.icc_odi_batsman_ranking",
        "inputFilePattern": "gs://mahdi-sports-stats/batsmen_rankings.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://mahdi-bkt-dataflow-metadata",
        }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)

    
