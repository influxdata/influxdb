// Types
import {DataSource, DataLoaderType} from 'src/types/v2/dataLoaders'

export type Action =
  | SetDataLoadersType
  | AddDataSource
  | RemoveDataSource
  | SetActiveDataSource

interface SetDataLoadersType {
  type: 'SET_DATA_LOADERS_TYPE'
  payload: {type: DataLoaderType}
}

export const setDataLoadersType = (
  type: DataLoaderType
): SetDataLoadersType => ({
  type: 'SET_DATA_LOADERS_TYPE',
  payload: {type},
})

interface AddDataSource {
  type: 'ADD_DATA_SOURCE'
  payload: {dataSource: DataSource}
}

export const addDataSource = (dataSource: DataSource): AddDataSource => ({
  type: 'ADD_DATA_SOURCE',
  payload: {dataSource},
})

interface RemoveDataSource {
  type: 'REMOVE_DATA_SOURCE'
  payload: {dataSource: string}
}

export const removeDataSource = (dataSource: string): RemoveDataSource => ({
  type: 'REMOVE_DATA_SOURCE',
  payload: {dataSource},
})

interface SetActiveDataSource {
  type: 'SET_ACTIVE_DATA_SOURCE'
  payload: {dataSource: string}
}

export const setActiveDataSource = (
  dataSource: string
): SetActiveDataSource => ({
  type: 'SET_ACTIVE_DATA_SOURCE',
  payload: {dataSource},
})
