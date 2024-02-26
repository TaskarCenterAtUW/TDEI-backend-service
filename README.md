# TDEI-backend-service
TDEI backend service

## Message bbox Request 

```json
{
    "messageId": "job_id",
    "messageType": "workflow_identifier",
    "data": {
      "service" : "bbox_intersect",
      "parameters": {
        "tdei_dataset_id" : "tdei_dataset_id",
        "bbox" : [1,2,3,4]
      },
      "user_id": "user_id",
    } 
}
```

## Message bbox Response 

```json
{
    "messageId": "job_id",
    "messageType": "workflow_identifier",
    "data": {
      "service" : "file path",
      "parameters":{...},
      "upload_file_url" : "zip file path",
      "success": true|false,
      "message": "message" // if false the error string else empty string
    } 
  }
```
