// Utils
import Deferred from 'src/utils/Deferred'
import {getWindowVars} from 'src/variables/utils/getWindowVars'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'

// Types
import {File} from 'src/types/ast'
import {WrappedCancelablePromise, CancellationError} from 'src/types/promises'
import {VariableAssignment} from 'src/types/ast'

const CHECK_LIMIT_INTERVAL = 200
const MAX_ROWS = 50000

export interface ExecuteFluxQueryResult {
  csv: string
  didTruncate: boolean
  rowCount: number
}

interface XHRError extends Error {
  xhr?: XMLHttpRequest
}

export const executeQuery = (
  url: string,
  orgID: string,
  query: string,
  extern?: File
): WrappedCancelablePromise<ExecuteFluxQueryResult> => {
  // We're using `XMLHttpRequest` directly here rather than through `axios` so
  // that we can poll the response size as it comes back. If the response size
  // is greater than a predefined limit, we close the HTTP connection and
  // return the partial response. We could acheive this more elegantly using
  // `fetch` and the [Streams API][0], but the Streams API is currently behind
  // a feature flag in Firefox.
  //
  // [0]: https://developer.mozilla.org/en-US/docs/Web/API/Streams_API
  const xhr = new XMLHttpRequest()
  const deferred = new Deferred()

  let didTruncate = false
  let rowCount = 0
  let rowCountIndex = 0

  const countNewRows = (): number => {
    // Don't extract this to a non-closure helper, since passing
    // `xhr.responseText` as an argument will be expensive
    if (!xhr.responseText) {
      return 0
    }

    let count = 0

    for (let i = rowCountIndex; i < xhr.responseText.length; i++) {
      if (xhr.responseText[i] === '\n') {
        count++
      }
    }

    return count
  }

  const resolve = () => {
    let csv = xhr.responseText

    if (didTruncate) {
      // Discard the last line in the response since it may be partially read
      csv = csv.slice(0, csv.lastIndexOf('\n'))
    }

    rowCount += countNewRows()

    const result: ExecuteFluxQueryResult = {
      csv,
      didTruncate,
      rowCount,
    }

    deferred.resolve(result)
    clearTimeout(interval)
  }

  const reject = () => {
    let bodyError = null

    try {
      bodyError = JSON.parse(xhr.responseText).message
    } catch {
      if (xhr.responseText && xhr.responseText.trim() !== '') {
        bodyError = xhr.responseText
      }
    }

    const statusError = xhr.statusText
    const fallbackError = 'failed to execute Flux query'
    const error: XHRError = new Error(bodyError || statusError || fallbackError)
    error.xhr = xhr

    deferred.reject(error)
    clearTimeout(interval)
  }

  const interval = setInterval(() => {
    if (!xhr.responseText) {
      // Haven't received any data yet
      return
    }

    rowCount += countNewRows()
    rowCountIndex = xhr.responseText.length

    if (rowCount < MAX_ROWS) {
      return
    }

    didTruncate = true
    resolve()
    xhr.abort()
  }, CHECK_LIMIT_INTERVAL)

  xhr.onload = () => {
    if (xhr.status === 200) {
      resolve()
    } else {
      reject()
    }
  }

  xhr.onerror = reject

  const dialect = {annotations: ['group', 'datatype', 'default']}
  const body = extern ? {query, dialect, extern} : {query, dialect}

  xhr.open('POST', `${url}?orgID=${encodeURIComponent(orgID)}`)
  xhr.setRequestHeader('Content-Type', 'application/json')
  xhr.send(JSON.stringify(body))

  return {
    promise: deferred.promise,
    cancel: () => {
      clearTimeout(interval)
      xhr.abort()
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
  url: string,
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
    const pendingResult = executeQuery(url, orgID, query, extern)

    cancelExecution = pendingResult.cancel

    return pendingResult.promise
  })

  return {promise, cancel}
}
