import {Table, ScatterLayerConfig, ScatterMappings} from '../types'
import {getFillScale} from '../utils/getFillScale'
import {getSymbolScale} from '../utils/getSymbolScale'
import {appendGroupingCol} from '../utils/appendGroupingCol'
import {FILL, SYMBOL} from '../constants/columnKeys'

export const getScatterTable = (
  table: Table,
  fill: string[],
  symbol: string[]
): Table => {
  const withFillCol = appendGroupingCol(table, fill, FILL)
  const withSymbolCol = appendGroupingCol(withFillCol, symbol, SYMBOL)

  return withSymbolCol
}

export const getScatterMappings = (
  layerConfig: ScatterLayerConfig
): ScatterMappings => ({
  x: layerConfig.x,
  y: layerConfig.y,
  fill: layerConfig.fill,
  symbol: layerConfig.symbol,
})

export const getScatterScales = (table: Table, colors: string[]) => ({
  fill: getFillScale(table, colors),
  symbol: getSymbolScale(table),
})
