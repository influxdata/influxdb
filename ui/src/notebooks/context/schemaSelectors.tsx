import React, {FC, useEffect} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Actions
import {getSchemaByBucket} from 'src/notebooks/selectors'

// Selectors
import {getStatus} from 'src/resources/selectors'

// Types
import {AppState, ResourceType, RemoteDataState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
export type Props = ReduxProps

export interface SelectorContextType {
  loading: RemoteDataState
  selectors: any[]
}

export const DEFAULT_CONTEXT: SelectorContextType = {
  loading: RemoteDataState.NotStarted,
  selectors: [],
}

export const SelectorContext = React.createContext<SelectorContextType>(
  DEFAULT_CONTEXT
)

let GLOBAL_LOADING = false

const lockAndLoad = async grabber => {
  GLOBAL_LOADING = true
  await grabber()
  GLOBAL_LOADING = false
}

export const SelectorProvider: FC<Props> = React.memo(
  ({loading, getSelectors, selectors, children}) => {
    const {data} = useContext(PipeContext)
    const localFetchSchema = (bucket: string) => {
      fetchSchema(bucket)
    }
    const fetchSchema = async () => {
      const text = `import "influxdata/influxdb/v1"
from(bucket: "${data.bucketName}")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> first()`
      // calls set results
    }
    useEffect(() => {
      if (loading !== RemoteDataState.NotStarted) {
        return
      }

      if (GLOBAL_LOADING) {
        return
      }

      lockAndLoad(fetchSchema)
    }, [loading, fetchSchema])

    return (
      <SelectorContext.Provider
        value={{
          loading,
          selectors,
        }}
      >
        {children}
      </SelectorContext.Provider>
    )
  }
)

const mstp = (state: AppState) => {
  const Selectors = getSchemaByBucket(state)
  const Selectors = getSchemaByBucket(state)
  const loading = getStatus(state, ResourceType.Selectors)

  return {
    loading,
    Selectors,
  }
}

const mdtp = {
  getSelectors: getSelectors,
}

const connector = connect(mstp, mdtp)

export default connector(SelectorProvider)
