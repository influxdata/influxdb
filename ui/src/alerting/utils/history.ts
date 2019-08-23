// Libraries
import {cloneDeep} from 'lodash'
import {fromFlux} from '@influxdata/giraffe'

// Utils
import {runQuery, RunQueryResult} from 'src/shared/apis/query'
import {searchExprToFlux} from 'src/eventViewer/utils/search'
import {findNodes} from 'src/shared/utils/ast'

// Constants
import {
  STATUS_BUCKET,
  NOTIFICATION_BUCKET,
} from 'src/alerting/constants/history'

// Types
import {CancelBox, StatusRow, NotificationRow} from 'src/types'

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
  const start = since ? Math.round(since / 1000) : '-60d'
  const fluxFilter = filter ? searchExprToFlux(renameTagKeys(filter)) : null

  const query = `
from(bucket: "${STATUS_BUCKET}")
  |> range(start: ${start}, stop: ${Math.round(until / 1000)})
  |> filter(fn: (r) => r._field == "_message")
  |> keep(columns: ["_time", "_value", "_check_id", "_check_name", "_level"])
  |> rename(columns: {"_time": "time",
                      "_value": "message",
                      "_check_id": "checkID",
                      "_check_name": "checkName",
                      "_level": "level"})
  |> group()${fluxFilter ? `\n  |> filter(fn: (r) => ${fluxFilter})` : ''}
  |> limit(n: ${limit}, offset: ${offset})
`

  return processResponse(runQuery(orgID, query)) as CancelBox<StatusRow[]>
}

export const loadNotifications = (
  orgID: string,
  {offset, limit, since, until, filter}: LoadRowsOptions
): CancelBox<NotificationRow[]> => {
  const start = since ? Math.round(since / 1000) : '-60d'
  const fluxFilter = filter ? searchExprToFlux(renameTagKeys(filter)) : null

  const query = `
from(bucket: "${NOTIFICATION_BUCKET}")
  |> range(start: ${start}, stop: ${Math.round(until / 1000)})
  |> filter(fn: (r) => r._field == "_message")
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
                      "_notification_rule_name": "notificationRule",
                      "_notification_endpoint_id": "notificationEndpointID",
                      "_notification_endpoint_name": "notificationEndpoint",
                      "_level": "level",
                      "_sent": "sent"})
  |> group()${fluxFilter ? `\n  |> filter(fn: (r) => ${fluxFilter})` : ''}
  |> limit(n: ${limit}, offset: ${offset})
`

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

  The rewrite rules for the "noficationEndpoint" work similarly.
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
const processResponse = ({
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
