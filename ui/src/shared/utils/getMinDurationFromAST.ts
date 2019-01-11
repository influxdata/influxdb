import {get, isObject, isArray} from 'lodash'

export function getMinDurationFromAST(ast: any): number {
  // We can't take the minimum of durations of each range individually, since
  // seperate ranges are potentially combined via an inner `join` call. So the
  // approach is to:
  //
  // 1. Find every possible `[start, stop]` combination for all start and stop
  //    times across every `range` call
  // 2. Map each combination to a duration via `stop - start`
  // 3. Filter out the non-positive durations
  // 4. Take the minimum duration
  //
  const times = allRangeTimes(ast)
  const starts = times.map(t => t[0])
  const stops = times.map(t => t[1])
  const cartesianProduct = starts.map(start => stops.map(stop => [start, stop]))

  const durations = []
    .concat(...cartesianProduct)
    .map(([start, stop]) => stop - start)
    .filter(d => d > 0)

  const result = Math.min(...durations)

  return result
}

// The following interfaces only represent AST structs as they appear
// in the context of a `range` call

interface RangeCallExpression {
  type: 'CallExpression'
  callee: {
    type: 'Identifier'
    name: 'range'
  }
  arguments: [{properties: RangeCallProperty[]}]
}

interface RangeCallProperty {
  type: 'Property'
  key: {
    name: 'start' | 'stop'
  }
  value: RangeCallPropertyValue
}

type RangeCallPropertyValue =
  | MinusUnaryExpression<DurationLiteral>
  | DurationLiteral
  | DateTimeLiteral
  | Identifier
  | DurationBinaryExpression

interface MinusUnaryExpression<T> {
  type: 'UnaryExpression'
  operator: '-'
  argument: T
}

interface DurationLiteral {
  type: 'DurationLiteral'
  values: Array<{
    magnitude: number
    unit: DurationUnit
  }>
}

type DurationUnit =
  | 'y'
  | 'mo'
  | 'w'
  | 'd'
  | 'h'
  | 'm'
  | 's'
  | 'ms'
  | 'us'
  | 'µs'
  | 'ns'

interface DateTimeLiteral {
  type: 'DateTimeLiteral'
  value: string
}

interface Identifier {
  type: 'Identifier'
  name: string
}

interface DurationBinaryExpression {
  type: 'BinaryExpression'
  left: DateTimeLiteral
  right: DurationLiteral
  operator: '+' | '-'
}

function allRangeTimes(ast: any): Array<[number, number]> {
  return findNodes(isRangeNode, ast).map(node => rangeTimes(ast, node))
}

/*
  Given a `range` call in an AST, reports the `start` and `stop` arguments the
  the call as absolute instants in time. If the `start` or `stop` argument is a
  relative duration literal, it is interpreted as relative to the current
  instant (`Date.now()`).
*/
function rangeTimes(
  ast: any,
  rangeNode: RangeCallExpression
): [number, number] {
  const properties = rangeNode.arguments[0].properties
  const now = Date.now()

  // The `start` argument is required
  const startProperty = properties.find(p => p.key.name === 'start')
  const start = propertyTime(ast, startProperty.value, now)

  // The `end` argument to a `range` call is optional, and defaults to now
  const endProperty = properties.find(p => p.key.name === 'stop')
  const end = endProperty ? propertyTime(ast, endProperty.value, now) : now

  if (isNaN(start) || isNaN(end)) {
    throw new Error('failed to analyze query')
  }

  return [start, end]
}

function propertyTime(
  ast: any,
  value: RangeCallPropertyValue,
  now: number
): number {
  switch (value.type) {
    case 'UnaryExpression':
      return now - durationDuration(value.argument)
    case 'DurationLiteral':
      return now + durationDuration(value)
    case 'DateTimeLiteral':
      return Date.parse(value.value)
    case 'Identifier':
      return propertyTime(ast, resolveDeclaration(ast, value.name), now)
    case 'BinaryExpression':
      const leftTime = Date.parse(value.left.value)
      const rightDuration = durationDuration(value.right)

      switch (value.operator) {
        case '+':
          return leftTime + rightDuration
        case '-':
          return leftTime - rightDuration
      }
  }
}

const UNIT_TO_APPROX_DURATION = {
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

function durationDuration(durationLiteral: DurationLiteral): number {
  const duration = durationLiteral.values.reduce(
    (sum, {magnitude, unit}) => sum + magnitude * UNIT_TO_APPROX_DURATION[unit],
    0
  )

  return duration
}

/*
  Find the node in the `ast` that defines the value of the variable with the
  given `name`.
*/
function resolveDeclaration(ast: any, name: string): RangeCallPropertyValue {
  const isDeclarator = node => {
    return (
      get(node, 'type') === 'VariableAssignment' &&
      get(node, 'id.name') === name
    )
  }

  const declarator = findNodes(isDeclarator, ast)

  if (!declarator.length) {
    throw new Error(`unable to resolve identifier "${name}"`)
  }

  if (declarator.length > 1) {
    throw new Error('cannot resolve identifier with duplicate declarations')
  }

  const init = declarator[0].init

  return init
}

function isRangeNode(node: any) {
  return (
    get(node, 'type') === 'CallExpression' &&
    get(node, 'callee.type') === 'Identifier' &&
    get(node, 'callee.name') === 'range'
  )
}

/*
  Find all nodes in a tree matching the `predicate` function. Each node in the
  tree is an object, which may contain objects or arrays of objects as children
  under any key.
*/
function findNodes(
  predicate: (node: any[]) => boolean,
  node: any,
  acc: any[] = []
) {
  if (predicate(node)) {
    acc.push(node)
  }

  for (const value of Object.values(node)) {
    if (isObject(value)) {
      findNodes(predicate, value, acc)
    } else if (isArray(value)) {
      for (const innerValue of value) {
        findNodes(predicate, innerValue, acc)
      }
    }
  }

  return acc
}
