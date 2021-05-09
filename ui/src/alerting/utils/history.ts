// Libraries
import {cloneDeep} from 'lodash'
import {fromFlux} from '@influxdata/giraffe'

// Utils
import {runQuery, RunQueryResult} from 'src/shared/apis/query'
import {parseSearchInput, searchExprToFlux} from 'src/eventViewer/utils/search'
import {findNodes} from 'src/shared/utils/ast'
import {readQueryParams} from 'src/shared/utils/queryParams'
import {event} from 'src/cloud/utils/reporting'

// Constants
import {
  HISTORY_TYPE_QUERY_PARAM,
  SEARCH_QUERY_PARAM,
} from 'src/alerting/constants/history'
import {MONITORING_BUCKET} from 'src/alerting/constants'

// Types
import {State as EventViewerState} from 'src/eventViewer/components/EventViewer.reducer'

import {
  CancelBox,
  StatusRow,
  NotificationRow,
  AlertHistoryType,
} from 'src/types'

import {
  LoadRowsOptions,
  Row,
  SearchExpr,
  SearchTagExpr,
} from 'src/eventViewer/types'

export const loadStatuses = (
  orgID: string,
  {offset, limit, since, until, filter}: LoadRowsOptions
): CancelBox<StatusRow[]> => {
  const start = since ? Math.round(since / 1000) : '-1h'
  const fluxFilter = filter ? searchExprToFlux(renameTagKeys(filter)) : null

  const query = `
from(bucket: "${MONITORING_BUCKET}")
  |> range(start: ${start}, stop: ${Math.round(until / 1000)})
  |> filter(fn: (r) => r._measurement == "statuses" and r._field == "_message")
  |> filter(fn: (r) => exists r._check_id and exists r._value and exists r._check_name and exists r._level)
  |> keep(columns: ["_time", "_value", "_check_id", "_check_name", "_level"])
  |> rename(columns: {"_time": "time",
                      "_value": "message",
                      "_check_id": "checkID",
                      "_check_name": "checkName",
                      "_level": "level"})
  |> group()${fluxFilter ? `\n  |> filter(fn: (r) => ${fluxFilter})` : ''}
  |> sort(columns: ["time"], desc: true)
  |> limit(n: ${limit}, offset: ${offset})
`

  event('runQuery', {context: 'alertHistory'})
  return processResponse(runQuery(orgID, query)) as CancelBox<StatusRow[]>
}

export const loadNotifications = (
  orgID: string,
  {offset, limit, since, until, filter}: LoadRowsOptions
): CancelBox<NotificationRow[]> => {
  const start = since ? Math.round(since / 1000) : '-1h'
  const fluxFilter = filter ? searchExprToFlux(renameTagKeys(filter)) : null

  const query = `
from(bucket: "${MONITORING_BUCKET}")
  |> range(start: ${start}, stop: ${Math.round(until / 1000)})
  |> filter(fn: (r) => r._measurement == "notifications")
  |> filter(fn: (r) => r._field !~ /^_/)
  |> filter(fn: (r) => exists r._check_id and exists r._check_name and exists r._notification_rule_id and exists r._notification_rule_name and exists r._notification_endpoint_id and exists r._notification_endpoint_name and exists r._level and exists r._sent)
  |> keep(columns: ["_time",
                    "_check_id",
                    "_check_name",
                    "_notification_rule_id",
                    "_notification_rule_name",
                    "_notification_endpoint_id",
                    "_notification_endpoint_name",
                    "_level",
                    "_sent"])
  |> rename(columns: {"_time": "time",
                      "_check_id": "checkID",
                      "_check_name": "checkName",
                      "_notification_rule_id": "notificationRuleID",
                      "_notification_rule_name": "notificationRuleName",
                      "_notification_endpoint_id": "notificationEndpointID",
                      "_notification_endpoint_name": "notificationEndpointName",
                      "_level": "level",
                      "_sent": "sent"})
  |> group()${fluxFilter ? `\n  |> filter(fn: (r) => ${fluxFilter})` : ''}
  |> sort(columns: ["time"], desc: true)
  |> limit(n: ${limit}, offset: ${offset})
`
  event('runQuery', {context: 'alertHistory'})
  return processResponse(runQuery(orgID, query)) as CancelBox<NotificationRow[]>
}

/*
  Rename the tag keys used in a search term to match the precise tag keys we
  query using Flux.

  For example if a user writes:

      "notification rule" == "my rule"

  Then we will rewrite "notification rule" as "notificationRule". The following
  tag keys would also be rewritten as "notificationRule":

  - "notificationRule"
  - "rule"
  - "notification_rule"
  - "_notification_rule"

  The rewrite rules for the "notificationEndpoint" work similarly.
*/
const renameTagKeys = (searchExpr: SearchExpr) => {
  const rewrittenExpr = cloneDeep(searchExpr)

  const tagExprNodes: SearchTagExpr[] = findNodes(
    rewrittenExpr,
    n => n && n.type === 'TagExpression'
  )

  const tagKeyNodes = tagExprNodes.map(n => n.left)

  for (const node of tagKeyNodes) {
    const normal = node.value
      .trim()
      .toLowerCase()
      .replace(' ', '')
      .replace('_', '')

    if (normal === 'notificationrule' || normal === 'rule') {
      node.value = 'notificationRule'
    } else if (normal === 'notificationEndpoint' || normal === 'endpoint') {
      node.value = 'notificationEndpoint'
    }
  }

  return rewrittenExpr
}

/*
  Convert a Flux CSV response into a list of objects.
*/
export const processResponse = ({
  promise: queryPromise,
  cancel,
}: CancelBox<RunQueryResult>): CancelBox<Row[]> => {
  const promise = queryPromise.then<Row[]>(resp => {
    if (resp.type !== 'SUCCESS') {
      return Promise.reject(new Error(resp.message))
    }

    const {table} = fromFlux(resp.csv)
    const rows: Row[] = []

    for (let i = 0; i < table.length; i++) {
      const row = {}

      for (const key of table.columnKeys) {
        row[key] = table.getColumn(key)[i]
      }

      rows.push(row)
    }

    return rows
  })

  return {
    promise,
    cancel,
  }
}

export const getInitialHistoryType = (): AlertHistoryType => {
  return readQueryParams()[HISTORY_TYPE_QUERY_PARAM] || 'statuses'
}

export const getInitialState = (): Partial<EventViewerState> => {
  const searchInput = readQueryParams()[SEARCH_QUERY_PARAM]

  if (!searchInput) {
    return {}
  }

  try {
    const searchExpr = parseSearchInput(searchInput)

    return {searchInput, searchExpr}
  } catch {
    return {searchInput}
  }
}
