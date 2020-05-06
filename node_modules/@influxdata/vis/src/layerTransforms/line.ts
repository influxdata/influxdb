import {Table, LineLayerConfig, LineMappings, LineScales} from '../types'
import {getFillScale} from '../utils/getFillScale'
import {appendGroupingCol} from '../utils/appendGroupingCol'
import {FILL} from '../constants/columnKeys'

export const getLineTable = (table: Table, fill: string[]): Table =>
  appendGroupingCol(table, fill, FILL)

export const getLineMappings = (
  layerConfig: LineLayerConfig
): LineMappings => ({
  x: layerConfig.x,
  y: layerConfig.y,
  fill: layerConfig.fill,
})

export const getLineScales = (table: Table, colors: string[]): LineScales => ({
  fill: getFillScale(table, colors),
})
