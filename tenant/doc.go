/*
The tenant domain encapsulates all the storage critical metadata services:
User
Organization
Bucket
URM's

These services are the cornerstone of all other metadata services. The intent is to have
a single location for all tenant related code. THis should facilitate faster bug resolution and
allow us to make changes to this service without effecting any dependant services.

When a new request for the tenant service comes in it should follow this pattern:
1 http_server_resource - this is where the request is parsed and rejected if the client didn't send

	the right information

2 middleware_resource_auth - We now confirm the user that generated the request has sufficient permission

	to accomplish this task, in some cases we adjust the request if the user is without the correct permissions

3 middleware_resource_metrics - Track RED metrics for this request
4 middleware_resource_logging - add logging around request duration and status.
5 service_resource - When a request reaches the service we verify the content for compatibility with the existing dataset,

	for instance if a resource has a "orgID" we will ensure the organization exists

6 storage_resource - Basic CRUD actions for the system.

This pattern of api -> middleware -> service -> basic crud helps us to break down the responsibilities into digestible
chunks and allows us to swap in or out any pieces we need depending on the situation. Currently the storage layer is using
a kv store but by breaking the crud actions into its own independent set of concerns we allow ourselves to move away from kv
if the need arises without having to be concerned about messing up some other pieces of logic.
*/
package tenant
