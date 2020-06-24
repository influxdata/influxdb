import React, {FC} from 'react'
import {connect} from 'react-redux'

// Actions
import {getBuckets} from 'src/buckets/actions/thunks'

// Selectors
import {getSortedBuckets} from 'src/buckets/selectors'
import {getStatus} from 'src/resources/selectors'

// Types
import {AppState, Bucket, ResourceType, RemoteDataState} from 'src/types'

export interface StateProps {
  loading: RemoteDataState
  buckets: Bucket[]
}

export interface DispatchProps {
  getBuckets: typeof getBuckets
}

export type Props = StateProps & DispatchProps

export interface BucketContextType {
  loading: RemoteDataState
  buckets: Bucket[]
}

export const DEFAULT_CONTEXT: BucketContextType = {
  loading: RemoteDataState.NotStarted,
  buckets: [],
}

export const BucketContext = React.createContext<BucketContextType>(
  DEFAULT_CONTEXT
)

export const BucketProvider: FC<Props> = React.memo(
  ({loading, getBuckets, buckets, children}) => {
    if (loading === RemoteDataState.NotStarted) {
      getBuckets()
    }

    return (
      <BucketContext.Provider
        value={{
          loading,
          buckets,
        }}
      >
        {children}
      </BucketContext.Provider>
    )
  }
)

const mstp = (state: AppState): StateProps => {
  const buckets = getSortedBuckets(state)
  const loading = getStatus(state, ResourceType.Buckets)

  return {
    loading,
    buckets,
  }
}

const mdtp: DispatchProps = {
  getBuckets: getBuckets,
}

export default connect<StateProps, DispatchProps>(mstp, mdtp)(BucketProvider)
