// Actions
import {setQueryResults} from 'src/timeMachine/actions/queries'
// Types
import {GetState, RemoteDataState} from 'src/types'

const hashCode = s =>
  s.split('').reduce((a, b) => ((a << 5) - a + b.charCodeAt(0)) | 0, 0)

export const getQueryResultsByQueryID = (queryID: string) => (
  dispatch,
  getState: GetState
): void => {
  try {
    const state = getState()
    const {files, timeInterval} = state.data.queryResultsByQueryID[
      hashCode(queryID)
    ]
    console.log('timeInterval: ', timeInterval)
    dispatch(setQueryResults(RemoteDataState.Done, files, null, null))
  } catch (error) {
    console.error('error: ', error)
    dispatch(setQueryResults(RemoteDataState.Error, null, null, error.message))
  }
}

export const setQueryResultsByQueryID = (queryID: string, files: string[]) =>
  ({
    type: 'SET_QUERY_RESULTS_BY_QUERY',
    queryID: hashCode(queryID),
    files,
  } as const)
