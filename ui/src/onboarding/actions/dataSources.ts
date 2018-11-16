// Types
import {DataSource} from 'src/types/v2/dataSources'

export type Action =
  | AddDataSource
  | RemoveDataSource
  | SetDataSources
  | SetActiveDataSource

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

interface SetDataSources {
  type: 'SET_DATA_SOURCES'
  payload: {dataSources: DataSource[]}
}

export const setDataSources = (dataSources: DataSource[]): SetDataSources => ({
  type: 'SET_DATA_SOURCES',
  payload: {dataSources},
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
