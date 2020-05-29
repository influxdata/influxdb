import {get} from 'lodash'

// Types
import {AlertBuilderState} from 'src/alerting/reducers/alertBuilder'
import {BuilderConfig} from 'src/types'

export function createCheckQueryFromAlertBuilder(
  builderConfig: BuilderConfig,
  {
    statusMessageTemplate,
    tags,
    id,
    name,
    every,
    offset,
    type,
    staleTime,
    level,
    timeSince,
    thresholds,
  }: AlertBuilderState
): string {
  const dataFrom = `data = from(bucket: "${builderConfig.buckets[0]}")`

  const filterStatements = builderConfig.tags
    .filter(tag => !!tag.values[0])
    .map(tag => `  |> filter(fn: (r) => r.${tag.key} == "${tag.values[0]}")`)

  const messageFn = `messageFn = (r) =>("${statusMessageTemplate}")`

  const checkTags = tags
    ? tags
        .filter(t => t.key && t.value)
        .map(t => `${t.key}: "${t.value}"`)
        .join(',')
    : ''

  const checkStatement = [
    'check = {',
    `  _check_id: "${id || ''}",`,
    `  _check_name: "${name}",`,
    `  _type: "custom",`,
    `  tags: {${checkTags}},`,
    `  every: ${every}`,
    '}',
  ]

  const optionTask = [
    'option task = {',
    `  name: "${name}",`,
    `  every: ${every}, // expected to match check.every`,
    `  offset: ${offset}`,
    '}',
  ]

  if (type === 'deadman') {
    const imports = [
      'package main',
      'import "influxdata/influxdb/monitor"',
      'import "experimental"',
      'import "influxdata/influxdb/v1"',
    ]

    const dataRange = `  |> range(start: -${staleTime})`

    const dataDefinition = [dataFrom, dataRange, ...filterStatements]

    const levelFunction = `${level.toLowerCase()} = (r) => (r.dead)`

    const checkLevel = `${level.toLowerCase()}:${level.toLowerCase()}`

    const queryStatement = [
      'data',
      '  |> v1.fieldsAsCols()',
      `  |> monitor.deadman(t: experimental.subDuration(from: now(), d: ${timeSince}))`,
      `  |> monitor.check(data: check, messageFn: messageFn,${checkLevel})`,
    ]

    const script: string[] = [
      imports.join('\n'),
      checkStatement.join('\n'),
      optionTask.join('\n'),
      levelFunction,
      messageFn,
      dataDefinition.join('\n'),
      queryStatement.join('\n'),
    ]
    return script.join('\n\n')
  }

  if (type === 'threshold') {
    const imports = [
      'package main',
      'import "influxdata/influxdb/monitor"',
      'import "influxdata/influxdb/v1"',
    ]

    const dataRange = `  |> range(start: -check.every)`

    const aggregateFunction = `  |> aggregateWindow(every: check.every, fn: ${
      builderConfig.functions[0].name
    }, createEmpty: false)`

    const dataDefinition = [
      dataFrom,
      dataRange,
      ...filterStatements,
      aggregateFunction,
    ]

    const thresholdExpressions = thresholds.map(t => {
      const fieldTag = builderConfig.tags.find(t => t.key === '_field')
      const fieldSelection = get(fieldTag, 'values[0]')

      const beginning = `${t.level.toLowerCase()} = (r) =>(r.${fieldSelection}`

      if (t.type === 'range') {
        if (t.within) {
          return `${beginning} > ${t.min}) and r.${fieldSelection} < ${t.max})`
        }
        return `${beginning} < ${t.min} and r.${fieldSelection} > ${t.max})`
      }

      const operator = t.type === 'greater' ? '>' : '<'

      return `${beginning} ${operator} ${t.value})`
    })

    const thresholdsDefined = thresholds.map(
      t => ` ${t.level.toLowerCase()}:${t.level.toLowerCase()}`
    )

    const queryStatement = [
      'data',
      '  |> v1.fieldsAsCols()',
      `  |> monitor.check(data: check, messageFn: messageFn,${thresholdsDefined})`,
    ]

    const script: string[] = [
      imports.join('\n'),
      checkStatement.join('\n'),
      optionTask.join('\n'),
      thresholdExpressions.join('\n'),
      messageFn,
      dataDefinition.join('\n'),
      queryStatement.join('\n'),
    ]

    return script.join('\n\n')
  }
}
