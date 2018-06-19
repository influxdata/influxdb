import parseShowDatabases from 'src/shared/parsing/showDatabases'
import parseShowFieldKeys from 'src/shared/parsing/showFieldKeys'
import parseShowTagKeys from 'src/shared/parsing/showTagKeys'
import parseShowTagValues from 'src/shared/parsing/showTagValues'
import parseShowMeasurements from 'src/shared/parsing/showMeasurements'
import parseShowSeries from 'src/shared/parsing/showSeries'

export const parseMetaQuery = (metaQuery: string, response): string[] => {
  const metaQueryStart = getMetaQueryPrefix(metaQuery)

  if (!metaQueryStart) {
    throw new Error('Could not find parser for meta query')
  }

  const parser = PARSERS[metaQueryStart]
  const extractor = EXTRACTORS[metaQueryStart]
  const parsed = parser(response)

  if (parsed.errors.length) {
    throw new Error(parsed.errors)
  }

  return extractor(parsed)
}

export const isInvalidMetaQuery = (metaQuery: string): boolean =>
  !getMetaQueryPrefix(metaQuery)

const getMetaQueryPrefix = (metaQuery: string): string | null => {
  const words = metaQuery
    .trim()
    .toUpperCase()
    .split(' ')
  const firstTwoWords = words.slice(0, 2).join(' ')
  const firstThreeWords = words.slice(0, 3).join(' ')

  return VALID_META_QUERY_PREFIXES.find(
    q => q === firstTwoWords || q === firstThreeWords
  )
}

const VALID_META_QUERY_PREFIXES = [
  'SHOW DATABASES',
  'SHOW MEASUREMENTS',
  'SHOW SERIES',
  'SHOW TAG VALUES',
  'SHOW FIELD KEYS',
  'SHOW TAG KEYS',
]

const PARSERS = {
  'SHOW DATABASES': parseShowDatabases,
  'SHOW FIELD KEYS': parseShowFieldKeys,
  'SHOW MEASUREMENTS': parseShowMeasurements,
  'SHOW SERIES': parseShowSeries,
  'SHOW TAG VALUES': parseShowTagValues,
  'SHOW TAG KEYS': parseShowTagKeys,
}

const EXTRACTORS = {
  'SHOW DATABASES': parsed => parsed.databases,
  'SHOW FIELD KEYS': parsed => {
    const {fieldSets} = parsed
    const fieldSetsValues = Object.values(fieldSets) as string[]

    return fieldSetsValues.reduce((acc, current) => [...acc, ...current], [])
  },
  'SHOW MEASUREMENTS': parsed => {
    const {measurementSets} = parsed

    return measurementSets.reduce(
      (acc, current) => [...acc, ...current.measurements],
      []
    )
  },
  'SHOW TAG KEYS': parsed => parsed.tagKeys,
  'SHOW TAG VALUES': parsed => {
    const {tags} = parsed
    const tagsValues = Object.values(tags) as string[]

    return tagsValues.reduce((acc, current) => [...acc, ...current], [])
  },
  'SHOW SERIES': parsed => parsed.series,
}
