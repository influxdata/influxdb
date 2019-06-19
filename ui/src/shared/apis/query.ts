import {getWindowVars} from 'src/variables/utils/getWindowVars'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'
import {client} from 'src/utils/api'

import {
  File,
  CancellationError as ClientCancellationError,
} from '@influxdata/influx'

// Types
import {WrappedCancelablePromise, CancellationError} from 'src/types/promises'
import {VariableAssignment} from 'src/types/ast'

const MAX_RESPONSE_CHARS = 50000 * 160

export const runQuery = (
  orgID: string,
  query: string,
  extern?: File
): WrappedCancelablePromise<string> => {
  const {promise, cancel} = client.queries.execute(orgID, query, {
    extern,
    limitChars: MAX_RESPONSE_CHARS,
  })

  // Convert the client `CancellationError` to a UI `CancellationError`
  const wrappedPromise = promise.catch(error =>
    error instanceof ClientCancellationError
      ? Promise.reject(CancellationError)
      : Promise.reject(error)
  )

  return {promise: wrappedPromise, cancel}
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
  orgID: string,
  query: string,
  variables?: VariableAssignment[]
): WrappedCancelablePromise<string> => {
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
