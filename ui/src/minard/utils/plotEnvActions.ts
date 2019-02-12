import {Table, AestheticDataMappings} from 'src/minard'

export type PlotAction =
  | RegisterLayerAction
  | UnregisterLayerAction
  | SetDimensionsAction
  | SetTableAction
  | SetColorsAction

interface RegisterLayerAction {
  type: 'REGISTER_LAYER'
  payload: {
    layerKey: string
    table: Table
    aesthetics: AestheticDataMappings
    colors?: string[]
  }
}

export const registerLayer = (
  layerKey: string,
  table: Table,
  aesthetics: AestheticDataMappings,
  colors?: string[]
): RegisterLayerAction => ({
  type: 'REGISTER_LAYER',
  payload: {layerKey, table, aesthetics, colors},
})

interface UnregisterLayerAction {
  type: 'UNREGISTER_LAYER'
  payload: {layerKey: string}
}

export const unregisterLayer = (layerKey: string): UnregisterLayerAction => ({
  type: 'UNREGISTER_LAYER',
  payload: {layerKey},
})

interface SetDimensionsAction {
  type: 'SET_DIMENSIONS'
  payload: {width: number; height: number}
}

export const setDimensions = (
  width: number,
  height: number
): SetDimensionsAction => ({
  type: 'SET_DIMENSIONS',
  payload: {width, height},
})

interface SetTableAction {
  type: 'SET_TABLE'
  payload: {table: Table}
}

export const setTable = (table: Table): SetTableAction => ({
  type: 'SET_TABLE',
  payload: {table},
})

interface SetColorsAction {
  type: 'SET_COLORS'
  payload: {colors: string[]; layerKey?: string}
}

export const setColors = (
  colors: string[],
  layerKey?: string
): SetColorsAction => ({
  type: 'SET_COLORS',
  payload: {colors, layerKey},
})
