import {Table, PlotEnv, Layer} from 'src/minard'

export type PlotAction =
  | RegisterLayerAction
  | UnregisterLayerAction
  | SetDimensionsAction
  | SetTableAction
  | ResetAction
  | SetControlledXDomainAction
  | SetControlledYDomainAction

interface RegisterLayerAction {
  type: 'REGISTER_LAYER'
  payload: {
    layerKey: string
    layer: Partial<Layer>
  }
}

export const registerLayer = (
  layerKey: string,
  layer: Partial<Layer>
): RegisterLayerAction => ({
  type: 'REGISTER_LAYER',
  payload: {layerKey, layer},
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

interface ResetAction {
  type: 'RESET'
  payload: Partial<PlotEnv>
}

export const reset = (initialState: Partial<PlotEnv>): ResetAction => ({
  type: 'RESET',
  payload: initialState,
})

interface SetControlledXDomainAction {
  type: 'SET_CONTROLLED_X_DOMAIN'
  payload: {xDomain: [number, number]}
}

export const setControlledXDomain = (
  xDomain: [number, number]
): SetControlledXDomainAction => ({
  type: 'SET_CONTROLLED_X_DOMAIN',
  payload: {xDomain},
})

interface SetControlledYDomainAction {
  type: 'SET_CONTROLLED_Y_DOMAIN'
  payload: {yDomain: [number, number]}
}

export const setControlledYDomain = (
  yDomain: [number, number]
): SetControlledYDomainAction => ({
  type: 'SET_CONTROLLED_Y_DOMAIN',
  payload: {yDomain},
})
