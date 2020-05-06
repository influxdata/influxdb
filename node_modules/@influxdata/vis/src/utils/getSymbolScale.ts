import {scaleOrdinal} from 'd3-scale'
import {Table, Scale} from '../types'
import {SYMBOL} from '../constants/columnKeys'

export type SymbolType =
  | 'circle'
  | 'triangle'
  | 'square'
  | 'plus'
  | 'tritip'
  | 'ex'

export const getSymbolScale = (table: Table): Scale<string, SymbolType> => {
  const symbolCol = table.getColumn(SYMBOL, 'string')
  const uniqueValues = Array.from(new Set(symbolCol))

  const symbolTypes: SymbolType[] = [
    'circle',
    'plus',
    'triangle',
    'square',
    'tritip',
    'ex',
  ]

  const scale = scaleOrdinal<string, SymbolType>()
    .domain(uniqueValues)
    .range(symbolTypes)

  return scale
}
