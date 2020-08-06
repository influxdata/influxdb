// Libraries
import {Dispatch} from 'react'

// Action Creators
import {
  setWriteStatus,
  Action,
} from 'src/buckets/components/lineProtocol/LineProtocol.creators'
import {readWriteCardinalityLimitReached} from 'src/shared/copy/notifications'
import {notify} from 'src/shared/actions/notifications'

// APIs
import {postWrite as apiPostWrite} from 'src/client'

// Types
import {RemoteDataState, WritePrecision} from 'src/types'

type WriteAction = Action | ReturnType<typeof notify>

export const writeLineProtocolAction = (
  org: string,
  bucket: string,
  body: string,
  precision: WritePrecision
) => async (dispatch: Dispatch<WriteAction>) => {
  try {
    dispatch(setWriteStatus(RemoteDataState.Loading))

    const resp = await apiPostWrite({
      data: body,
      query: {org, bucket, precision},
    })

    if (resp.status === 204) {
      dispatch(setWriteStatus(RemoteDataState.Done))
    } else if (resp.status === 429) {
      dispatch(notify(readWriteCardinalityLimitReached(resp.data.message)))
      dispatch(setWriteStatus(RemoteDataState.Error))
    } else if (resp.status === 403) {
      dispatch(setWriteStatus(RemoteDataState.Error, resp.data.message))
    } else {
      dispatch(setWriteStatus(RemoteDataState.Error, 'failed to write data'))
      throw new Error(resp?.data?.message || 'Failed to write data')
    }
  } catch (error) {
    console.error(error)
  }
}
