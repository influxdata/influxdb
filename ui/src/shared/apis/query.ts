import Deferred from 'src/utils/Deferred'
import {getWindowVars} from 'src/variables/utils/getWindowVars'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'
import {client} from 'src/utils/api'

import {File} from '@influxdata/influx'

// Types
import {WrappedCancelablePromise, CancellationError} from 'src/types/promises'
import {VariableAssignment} from 'src/types/ast'

const MAX_ROWS = 50000

export interface ExecuteFluxQueryResult {
  csv: string
  didTruncate: boolean
  rowCount: number
}

export const runQuery = (
  orgID: string,
  query: string,
  extern?: File
): WrappedCancelablePromise<ExecuteFluxQueryResult> => {
  const deferred = new Deferred()

  const conn = client.queries.execute(orgID, query, extern)

  let didTruncate = false
  let rowCount = 0
  let csv = ''

  conn.stream.on('data', d => {
    rowCount++
    csv += d

    if (rowCount < MAX_ROWS) {
      return
    }

    didTruncate = true
    conn.cancel()
  })

  conn.stream.on('end', () => {
    const result: ExecuteFluxQueryResult = {
      csv,
      didTruncate,
      rowCount,
    }

    deferred.resolve(result)
  })

  conn.stream.on('error', err => {
    deferred.reject(err)
  })

  return {
    promise: deferred.promise,
    cancel: () => {
      conn.cancel()
      deferred.reject(new CancellationError())
    },
  }
}

/*
  Execute a Flux query that uses external variables.

  The external variables will be supplied to the query via the `extern`
  parameter.

  The query may be using the `windowPeriod` variable, which cannot be supplied
  directly but must be derived from the query. To derive the `windowPeriod`
  variable, we:

  - Fetch the AST for the query
  - Analyse the AST to find the duration of the query, if possible
  - Use the duration of the query to compute the value of `windowPeriod`

  This function will handle supplying the `windowPeriod` variable, if it is
  used in the query.
*/
export const executeQueryWithVars = (
  _url: string,
  orgID: string,
  query: string,
  variables?: VariableAssignment[]
): WrappedCancelablePromise<ExecuteFluxQueryResult> => {
  let isCancelled = false
  let cancelExecution

  const cancel = () => {
    isCancelled = true

    if (cancelExecution) {
      cancelExecution()
    }
  }

  const promise = getWindowVars(query, variables).then(windowVars => {
    if (isCancelled) {
      return Promise.reject(new CancellationError())
    }

    const extern = buildVarsOption([...variables, ...windowVars])
    const pendingResult = runQuery(orgID, query, extern)

    cancelExecution = pendingResult.cancel

    return pendingResult.promise
  })

  return {promise, cancel}
}
