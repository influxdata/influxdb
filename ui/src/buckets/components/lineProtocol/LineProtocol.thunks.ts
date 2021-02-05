// Libraries
import {Dispatch} from 'react'

// Action Creators
import {
  setWriteStatus,
  Action,
} from 'src/buckets/components/lineProtocol/LineProtocol.creators'

// APIs
import {postWrite as apiPostWrite} from 'src/client'

// Types
import {RemoteDataState, WritePrecision} from 'src/types'

export const writeLineProtocolAction = async (
  dispatch: Dispatch<Action>,
  org: string,
  bucket: string,
  body: string,
  precision: WritePrecision
) => {
  try {
    dispatch(setWriteStatus(RemoteDataState.Loading))

    const resp = await apiPostWrite({
      data: body,
      query: {org, bucket, precision},
    })

    if (resp.status === 204) {
      dispatch(setWriteStatus(RemoteDataState.Done))
    } else if (resp.status === 429) {
      dispatch(
        setWriteStatus(
          RemoteDataState.Error,
          'Failed due to plan limits: read cardinality reached'
        )
      )
    } else if (resp.status === 403) {
      dispatch(setWriteStatus(RemoteDataState.Error, resp.data.message))
    } else {
      const message = resp?.data?.message || 'Failed to write data'
      dispatch(setWriteStatus(RemoteDataState.Error, message))
      throw new Error(message)
    }
  } catch (error) {
    console.error(error)
  }
}
