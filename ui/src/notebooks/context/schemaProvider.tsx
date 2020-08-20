// Libraries
import React, {FC, useCallback, useContext, useState} from 'react'
import {useDispatch, useSelector} from 'react-redux'

// Contexts
import {PipeContext} from 'src/notebooks/context/pipe'

// Utils
import {normalizeSchema} from 'src/notebooks/utils'
import {getAndSetBucketSchema} from 'src/notebooks/actions/thunks'

// Types
import {AppState, RemoteDataState} from 'src/types'

export type Props = {
  children: JSX.Element
}

export interface SchemaContextType {
  data: any
  fields: string[]
  localFetchSchema: (bucketName: string) => void
  loading: RemoteDataState
  measurements: string[]
  searchTerm: string
  setSearchTerm: (value: string) => void
  tags: any[]
}

export const DEFAULT_CONTEXT: SchemaContextType = {
  data: {},
  fields: [],
  localFetchSchema: (_: string): void => {},
  loading: RemoteDataState.NotStarted,
  measurements: [],
  searchTerm: '',
  setSearchTerm: (_: string) => {},
  tags: [],
}

export const SchemaContext = React.createContext<SchemaContextType>(
  DEFAULT_CONTEXT
)

export const SchemaProvider: FC<Props> = React.memo(({children}) => {
  const {data} = useContext(PipeContext)
  const [searchTerm, setSearchTerm] = useState('')
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
    [data?.bucketName, loading, dispatch]
  )

  const schema = useSelector(
    (state: AppState) => state.notebook.schema[data?.bucketName]?.schema || {}
  )

  const schemaCopy = {...schema}

  const {fields, measurements, tags} = normalizeSchema(
    schemaCopy,
    data,
    searchTerm
  )

  return (
    <SchemaContext.Provider
      value={{
        data: schema,
        fields,
        loading,
        localFetchSchema,
        measurements,
        searchTerm,
        setSearchTerm,
        tags,
      }}
    >
      {children}
    </SchemaContext.Provider>
  )
})

export default SchemaProvider
