import {scaleLinear} from 'd3-scale'

import {Scale} from '../types'

export const getLinearScale = (
  domain: number[],
  range: number[]
): Scale<number, number> => {
  return scaleLinear()
    .domain(domain)
    .range(range)
}
