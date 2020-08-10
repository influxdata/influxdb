# github webhooks

You should configure your Organization's Webhooks to point at the `webhooks` service. To do this go to `github.com/{my_organization}` and click `Settings > Webhooks > Add webhook`. In the resulting menu set `Payload URL` to `http://<my_ip>:1619/github`, `Content type` to `application/json` and under the section `Which events would you like to trigger this webhook?` select 'Send me <b>everything</b>'. By default all of the events will write to the `github_webhooks` measurement, this is configurable by setting the `measurement_name` in the config file.

You can also add a secret that will be used by telegraf to verify the authenticity of the requests.

## Events

The titles of the following sections are links to the full payloads and details for each event. The body contains what information from the event is persisted. The format is as follows:
```
# TAGS
* 'tagKey' = `tagValue` type
# FIELDS 
* 'fieldKey' = `fieldValue` type
```
The tag values and field values show the place on the incoming JSON object where the data is sourced from. 

#### [`commit_comment` event](https://developer.github.com/v3/activity/events/types/#commitcommentevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'commit' = `event.comment.commit_id` string
* 'comment' = `event.comment.body` string

#### [`create` event](https://developer.github.com/v3/activity/events/types/#createevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'ref' = `event.ref` string
* 'refType' = `event.ref_type` string

#### [`delete` event](https://developer.github.com/v3/activity/events/types/#deleteevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'ref' = `event.ref` string
* 'refType' = `event.ref_type` string

#### [`deployment` event](https://developer.github.com/v3/activity/events/types/#deploymentevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'commit' = `event.deployment.sha` string
* 'task' = `event.deployment.task` string
* 'environment' = `event.deployment.environment` string
* 'description' = `event.deployment.description` string

#### [`deployment_status` event](https://developer.github.com/v3/activity/events/types/#deploymentstatusevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'commit' = `event.deployment.sha` string
* 'task' = `event.deployment.task` string
* 'environment' = `event.deployment.environment` string
* 'description' = `event.deployment.description` string
* 'depState' = `event.deployment_status.state` string
* 'depDescription' = `event.deployment_status.description` string

#### [`fork` event](https://developer.github.com/v3/activity/events/types/#forkevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'forkee' = `event.forkee.repository` string

#### [`gollum` event](https://developer.github.com/v3/activity/events/types/#gollumevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int

#### [`issue_comment` event](https://developer.github.com/v3/activity/events/types/#issuecommentevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool
* 'issue' = `event.issue.number` int

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'title' = `event.issue.title` string
* 'comments' = `event.issue.comments` int
* 'body' = `event.comment.body` string

#### [`issues` event](https://developer.github.com/v3/activity/events/types/#issuesevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool
* 'issue' = `event.issue.number` int
* 'action' = `event.action` string

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'title' = `event.issue.title` string
* 'comments' = `event.issue.comments` int

#### [`member` event](https://developer.github.com/v3/activity/events/types/#memberevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'newMember' = `event.sender.login` string
* 'newMemberStatus' = `event.sender.site_admin` bool

#### [`membership` event](https://developer.github.com/v3/activity/events/types/#membershipevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool
* 'action' = `event.action` string

**Fields:**
* 'newMember' = `event.sender.login` string
* 'newMemberStatus' = `event.sender.site_admin` bool

#### [`page_build` event](https://developer.github.com/v3/activity/events/types/#pagebuildevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int

#### [`public` event](https://developer.github.com/v3/activity/events/types/#publicevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int

#### [`pull_request_review_comment` event](https://developer.github.com/v3/activity/events/types/#pullrequestreviewcommentevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'action' = `event.action` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool
* 'prNumber' = `event.pull_request.number` int

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'state' = `event.pull_request.state` string
* 'title' = `event.pull_request.title` string
* 'comments' = `event.pull_request.comments` int
* 'commits' = `event.pull_request.commits` int
* 'additions' = `event.pull_request.additions` int
* 'deletions' = `event.pull_request.deletions` int
* 'changedFiles' = `event.pull_request.changed_files` int
* 'commentFile' = `event.comment.file` string
* 'comment' = `event.comment.body` string

#### [`pull_request` event](https://developer.github.com/v3/activity/events/types/#pullrequestevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'action' = `event.action` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool
* 'prNumber' = `event.pull_request.number` int

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'state' = `event.pull_request.state` string
* 'title' = `event.pull_request.title` string
* 'comments' = `event.pull_request.comments` int
* 'commits' = `event.pull_request.commits` int
* 'additions' = `event.pull_request.additions` int
* 'deletions' = `event.pull_request.deletions` int
* 'changedFiles' = `event.pull_request.changed_files` int

#### [`push` event](https://developer.github.com/v3/activity/events/types/#pushevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'ref' = `event.ref` string
* 'before' = `event.before` string
* 'after' = `event.after` string

#### [`repository` event](https://developer.github.com/v3/activity/events/types/#repositoryevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int

#### [`release` event](https://developer.github.com/v3/activity/events/types/#releaseevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'tagName' = `event.release.tag_name` string

#### [`status` event](https://developer.github.com/v3/activity/events/types/#statusevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'commit' = `event.sha` string
* 'state' = `event.state` string

#### [`team_add` event](https://developer.github.com/v3/activity/events/types/#teamaddevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
* 'teamName' = `event.team.name` string

#### [`watch` event](https://developer.github.com/v3/activity/events/types/#watchevent)

**Tags:**
* 'event' = `headers[X-Github-Event]` string
* 'repository' = `event.repository.full_name` string
* 'private' = `event.repository.private` bool
* 'user' = `event.sender.login` string
* 'admin' = `event.sender.site_admin` bool

**Fields:**
* 'stars' = `event.repository.stargazers_count` int
* 'forks' = `event.repository.forks_count` int
* 'issues' = `event.repository.open_issues_count` int
