// Libraries
import {Dispatch} from 'react'
// import {fromFlux as parse} from '@influxdata/giraffe'

// API
import {runQuery} from 'src/shared/apis/query'

// Types
import {AppState, GetState, RemoteDataState, Schema} from 'src/types'

// Utils
import {getOrg} from 'src/organizations/selectors'
import {getSchemaByBucketName} from 'src/shared/selectors/schemaSelectors'
import {parseResponse} from 'src/shared/parsing/flux/schemaParser'

// Actions
import {
  resetSchema,
  setSchema,
  Action as SchemaAction,
} from 'src/shared/actions/schemaCreator'
import {notify, Action as NotifyAction} from 'src/shared/actions/notifications'

// Constants
import {getBucketsFailed} from 'src/shared/copy/notifications'
import {TEN_MINUTES} from 'src/shared/reducers/schema'

type Action = SchemaAction | NotifyAction

export const fetchSchemaForBucket = async (
  bucketName: string,
  orgID: string
): Promise<Schema> => {
  /*
  -4d here is an arbitrary time range that fulfills the need to overfetch a bucket's meta data
  rather than underfetching the data. At the time of writing this comment, a timerange is
  prerequisite for querying a bucket's metadata and is therefore required here.

  If overfetching provides too much overhead / comes at a performance cost down the line,
  we should reduce the range / come up with an alternative to allow for querying a bucket's metadata
  without having to provide a range
  */
  const text = `from(bucket: "${bucketName}")
  |> range(start: -4d)
  |> first()`

  const res = await runQuery(orgID, text)
    .promise.then(raw => {
      if (raw.type !== 'SUCCESS') {
        throw new Error(raw.message)
      }

      return raw
    })
    .then(raw => parseResponse(raw.csv))

  return res
}

const getUnexpiredSchema = (
  state: AppState,
  bucketName: string
): Schema | null => {
  const storedSchema = getSchemaByBucketName(state, bucketName)

  if (storedSchema?.schema && storedSchema?.exp > new Date().getTime()) {
    return storedSchema.schema
  } else {
    return null
  }
}

export const startWatchDog = () => (dispatch: Dispatch<Action>) => {
  setInterval(() => {
    dispatch(resetSchema())
  }, TEN_MINUTES / 2)

  dispatch(resetSchema())
}

export const getAndSetBucketSchema = (bucketName: string) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const state = getState()
    let validCachedResult = null
    if (bucketName) {
      validCachedResult = getUnexpiredSchema(state, bucketName)
    }
    if (validCachedResult !== null) {
      dispatch(setSchema(RemoteDataState.Done, bucketName, validCachedResult))
      return
    } else {
      dispatch(setSchema(RemoteDataState.Loading, bucketName, {}))
    }
    const orgID = getOrg(state).id
    const schema = await fetchSchemaForBucket(bucketName, orgID)
    dispatch(setSchema(RemoteDataState.Done, bucketName, schema))
  } catch (error) {
    console.error(error)
    dispatch(setSchema(RemoteDataState.Error))
    dispatch(notify(getBucketsFailed()))
  }
}
