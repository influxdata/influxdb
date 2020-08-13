import React, {FC, useCallback, useContext} from 'react'
import {useDispatch, useSelector} from 'react-redux'

// Contexts
import {PipeContext} from 'src/notebooks/context/pipe'

// Actions
import {getAndSetBucketSchema} from 'src/notebooks/actions/thunks'

// Types
import {AppState, RemoteDataState} from 'src/types'

export type Props = {
  children: JSX.Element
}

export interface SchemaContextType {
  localFetchSchema: (bucketName: string) => void
  loading: RemoteDataState
  schema: any
}

export const DEFAULT_CONTEXT: SchemaContextType = {
  localFetchSchema: (_: string): void => {},
  loading: RemoteDataState.NotStarted,
  schema: {},
}

export const SchemaContext = React.createContext<SchemaContextType>(
  DEFAULT_CONTEXT
)

export const SchemaProvider: FC<Props> = React.memo(({children}) => {
  const {data} = useContext(PipeContext)
  const dispatch = useDispatch()

  const loading = useSelector(
    (state: AppState) =>
      state.notebook.schema[data?.bucketName]?.status ||
      RemoteDataState.NotStarted
  )
  const localFetchSchema = useCallback(
    async (bucket: string) => {
      if (loading !== RemoteDataState.NotStarted) {
        return
      }
      if (bucket) {
        await dispatch(getAndSetBucketSchema(bucket))
      }
    },
    [data?.bucketName]
  )

  const schema = useSelector(
    (state: AppState) => state.notebook.schema[data?.bucketName] || {}
  )

  return (
    <SchemaContext.Provider
      value={{
        loading,
        localFetchSchema,
        schema,
      }}
    >
      {children}
    </SchemaContext.Provider>
  )
})

export default SchemaProvider
