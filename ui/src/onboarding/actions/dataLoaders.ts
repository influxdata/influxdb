// Types
import {DataSource, DataSourceType} from 'src/types/v2/dataSources'

export type Action = SetDataLoadersType | AddDataSource | RemoveDataSource

interface SetDataLoadersType {
  type: 'SET_DATA_LOADERS_TYPE'
  payload: {type: DataSourceType}
}

export const setDataLoadersType = (
  type: DataSourceType
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
