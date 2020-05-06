import {scaleUtc} from 'd3-scale'
import {ticks} from 'd3-array'

import {ColumnType} from '../types'
import {getTimeFormatter} from './getTimeFormatter'
import {TextMetrics} from './getTextMetrics'

const TICK_DENSITY = 0.5

export const getTicks = (
  domain: number[],
  length: number,
  orientation: 'vertical' | 'horizontal',
  columnType: ColumnType,
  charMetrics: TextMetrics
): number[] => {
  let sampleTick

  if (columnType === 'time') {
    sampleTick = getTimeFormatter(domain)(domain[1])
  } else {
    sampleTick = String(domain[1])
  }

  const charLength =
    orientation === 'vertical' ? charMetrics.height : charMetrics.width

  const numTicks = getNumTicks(sampleTick, length, charLength)

  if (columnType === 'time') {
    return getTimeTicks(domain, length, numTicks)
  }

  return ticks(domain[0], domain[1], numTicks)
}

const getNumTicks = (
  sampleTick: string,
  length: number,
  charLength: number
): number => {
  const sampleTickWidth = sampleTick.length * charLength
  const numTicks = Math.round((length / sampleTickWidth) * TICK_DENSITY)

  return Math.max(numTicks, 2)
}

const getTimeTicks = (
  [d0, d1]: number[],
  length: number,
  numTicks: number
): number[] => {
  return scaleUtc()
    .domain([d0, d1])
    .range([0, length])
    .ticks(numTicks)
    .map(d => d.getTime())
}
