import {Duration} from 'src/types/ast'

export const parseDuration = (input: string): Duration[] => {
  const r = /([0-9]+)(y|mo|w|d|h|ms|s|m|us|µs|ns)/g
  const result = []

  let match = r.exec(input)

  if (!match) {
    throw new Error(`could not parse "${input}" as duration`)
  }

  while (match) {
    result.push({
      magnitude: +match[1],
      unit: match[2],
    })

    match = r.exec(input)
  }

  return result
}

const UNIT_TO_APPROX_MS = {
  ns: 1 / 1000000,
  µs: 1 / 1000,
  us: 1 / 1000,
  ms: 1,
  s: 1000,
  m: 1000 * 60,
  h: 1000 * 60 * 60,
  d: 1000 * 60 * 60 * 24,
  w: 1000 * 60 * 60 * 24 * 7,
  mo: 1000 * 60 * 60 * 24 * 30,
  y: 1000 * 60 * 60 * 24 * 365,
}

export const durationToMilliseconds = (duration: Duration[]): number =>
  duration.reduce(
    (sum, {magnitude, unit}) => sum + magnitude * UNIT_TO_APPROX_MS[unit],
    0
  )

export const areDurationsEqual = (a: string, b: string): boolean => {
  try {
    return (
      durationToMilliseconds(parseDuration(a)) ===
      durationToMilliseconds(parseDuration(b))
    )
  } catch {
    return false
  }
}
