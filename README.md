# TDEI-backend-service
TDEI backend service

## Message bbox Request 

```json
{
    "messageId": "job_id",
    "messageType": "workflow_identifier",
    "data": {
      "service" : "bbox_intersect",
      "params":{

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
      "params":{},
      "upload_file_url" : "zip file path",
      "success": true|false,
      "message": "message" // if false the error string else empty string
    } 
  }
```
