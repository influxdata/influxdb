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
