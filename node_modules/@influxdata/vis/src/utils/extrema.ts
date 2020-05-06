import {extent} from 'd3-array'

import {flatMap} from './flatMap'

export const minBy = <T>(f: (x: T) => number, xs: T[]): T => {
  let minX = null
  let minDistance = Infinity

  for (const x of xs) {
    const distance = f(x)

    if (distance < minDistance) {
      minX = x
      minDistance = distance
    }
  }

  return minX
}

export const maxBy = <T>(f: (x: T) => number, xs: T[]): T => {
  let maxX = null
  let maxDistance = -Infinity

  for (const x of xs) {
    const distance = f(x)

    if (distance > maxDistance) {
      maxX = x
      maxDistance = distance
    }
  }

  return maxX
}

/*
  Given a list of lists, return the minimum and maximum value across all items
  in all lists as a tuple.

  Returns `null` if unsuccessful (e.g. if passed empty lists).
*/
export const extentOfExtents = (
  ...data: number[][]
): [number, number] | null => {
  const result = extent(flatMap(d => extent(d), data))

  if (result.some(x => x === undefined)) {
    return null
  }

  return result
}
