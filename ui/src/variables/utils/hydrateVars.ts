// Utils
import {valueFetcher, ValueFetcher} from 'src/variables/utils/ValueFetcher'
import Deferred from 'src/utils/Deferred'
import {getVarAssignment} from 'src/variables/utils/getVarAssignment'
import {resolveSelectedValue} from 'src/variables/utils/resolveSelectedValue'

// Constants
import {OPTION_NAME} from 'src/variables/constants/index'

// Types
import {RemoteDataState} from 'src/types'
import {Variable} from '@influxdata/influx'
import {WrappedCancelablePromise, CancellationError} from 'src/types/promises'
import {
  VariableValues,
  VariableValuesByID,
  ValueSelections,
} from 'src/variables/types'

interface VariableNode {
  variable: Variable
  values: VariableValues
  parents: VariableNode[]
  children: VariableNode[]
  status: RemoteDataState
  cancel: () => void
}

interface HydrateVarsOptions {
  url: string
  orgID: string
  selections: ValueSelections
  fetcher?: ValueFetcher
}

const createVariableGraph = (
  variables: Variable[],
  allVariables: Variable[]
): VariableNode[] => {
  const nodesByID: {[variableID: string]: VariableNode} = {}

  // First initialize all the nodes
  for (const variable of allVariables) {
    nodesByID[variable.id] = {
      variable,
      values: null,
      parents: [],
      children: [],
      status: RemoteDataState.NotStarted,
      cancel: () => {},
    }
  }

  // Then initialize all the edges (the `parents` and `children` references)
  for (const variable of allVariables) {
    if (variable.arguments.type !== 'query') {
      continue
    }

    const query: string = variable.arguments.values.query
    const childIDs = allVariables
      .filter(maybeChild => query.includes(`${OPTION_NAME}.${maybeChild.name}`))
      .map(maybeChild => maybeChild.id)

    for (const childID of childIDs) {
      nodesByID[variable.id].children.push(nodesByID[childID])
      nodesByID[childID].parents.push(nodesByID[variable.id])
    }
  }

  // Only return a graph containing nodes for the variables we care about and
  // their dependencies
  const relevantSubGraph = Object.values(nodesByID).filter(
    node =>
      variables.includes(node.variable) ||
      node.parents.some(parent => variables.includes(parent.variable))
  )

  return relevantSubGraph
}

/*
  Get the `VariableValues` for a variable that cannot be successfully hydrated.
*/
const errorVariableValues = (
  message = 'Failed to load values for variable'
): VariableValues => ({
  values: null,
  selectedValue: null,
  valueType: null,
  error: message,
})

/*
  Get the `VariableValues` for a map variable.
*/
const mapVariableValues = (
  variable: Variable,
  prevSelection: string,
  defaultSelection: string
): VariableValues => {
  const values: string[] = Object.values(variable.arguments.values)

  return {
    valueType: 'string',
    values,
    selectedValue: resolveSelectedValue(
      values,
      prevSelection,
      defaultSelection
    ),
  }
}

/*
  Get the `VariableValues` for a constant variable.
*/
const constVariableValues = (
  variable: Variable,
  prevSelection: string,
  defaultSelection: string
): VariableValues => {
  const {values} = variable.arguments

  return {
    valueType: 'string',
    values,
    selectedValue: resolveSelectedValue(
      values,
      prevSelection,
      defaultSelection
    ),
  }
}

/*
  Find all the descendants of a node.

  A node `b` is a descendant of `a` if there exists a path from `a` to `b`.

  This will loop forever if it is run on a node within a cycle or leading to a
  cycle.
*/
const collectDescendants = (node: VariableNode, acc = []): VariableNode[] => {
  for (const child of node.children) {
    acc.push(child)
    collectDescendants(child)
  }

  return acc
}

/*
  Hydrate the values of a single node in the graph.

  This assumes that every descendant of this node has already been hydrated. 
*/
const hydrateVarsHelper = async (
  node: VariableNode,
  options: HydrateVarsOptions
): Promise<VariableValues> => {
  const variableType = node.variable.arguments.type
  const prevSelection = options.selections[node.variable.id]
  const defaultSelection = node.variable.selected
    ? node.variable.selected[0]
    : null

  if (variableType === 'map') {
    return mapVariableValues(node.variable, prevSelection, defaultSelection)
  }

  if (variableType === 'constant') {
    return constVariableValues(node.variable, prevSelection, defaultSelection)
  }

  const descendants = collectDescendants(node)
  const assignments = descendants.map(node =>
    getVarAssignment(node.variable.name, node.values)
  )

  const {url, orgID} = options
  const {query} = node.variable.arguments.values
  const fetcher = options.fetcher || valueFetcher

  const request = fetcher.fetch(
    url,
    orgID,
    query,
    assignments,
    prevSelection,
    defaultSelection
  )

  node.cancel = request.cancel

  const values = await request.promise

  return values
}

/*
  Check if a node is `NotStarted` and if every child of the node has been
  resolved (successfully or not).
*/
const readyToResolve = (parent: VariableNode): boolean =>
  parent.status === RemoteDataState.NotStarted &&
  parent.children.every(child => child.status === RemoteDataState.Done)

/*
  Find all `NotStarted` nodes in the graph that have no children.
*/
const findLeaves = (graph: VariableNode[]): VariableNode[] =>
  graph.filter(
    node =>
      node.children.length === 0 && node.status === RemoteDataState.NotStarted
  )

/*
  Given a node, attempt to find a cycle that the node is a part of. If no cycle
  is found, return `null`.
*/
const findCyclicPath = (node: VariableNode): VariableNode[] => {
  try {
    findCyclicPathHelper(node, [])
  } catch (cyclicPath) {
    return cyclicPath
  }

  return null
}

const findCyclicPathHelper = (
  node: VariableNode,
  seen: VariableNode[]
): void => {
  if (seen.includes(node)) {
    throw seen
  }

  for (const child of node.children) {
    findCyclicPathHelper(child, [...seen, node])
  }
}

/*
  Find all cycles within the variable graph and mark every node within a cycle
  as errored (we cannot resolve cycles).
*/
const invalidateCycles = (graph: VariableNode[]): void => {
  for (const node of graph) {
    const cyclicPath = findCyclicPath(node)

    if (cyclicPath) {
      for (const invalidNode of cyclicPath) {
        invalidNode.status = RemoteDataState.Error
      }
    }
  }
}

/*
  Given a node, mark all ancestors of that node as `Error`.

  A node `a` is an ancestor of `b` if there exists a path from `a` to `b`.

  Traversing the ancestors of a node stops at `Error` nodes. In this case it is
  assumed that every ancestor of the `Error` node has also been marked as
  `Error`; this is to avoid getting caught in cycles. Nodes within cycles and
  nodes leading to cycles should have already been invalidated at this point by
  `invalidateCycles`.
*/
const invalidateAncestors = (node: VariableNode): void => {
  for (const parent of node.parents) {
    if (parent.status !== RemoteDataState.Error) {
      parent.status = RemoteDataState.Error
      invalidateAncestors(parent)
    }
  }
}

const extractResult = (graph: VariableNode[]): VariableValuesByID => {
  const result = {}

  for (const node of graph) {
    if (node.status === RemoteDataState.Error) {
      node.values = errorVariableValues()
    }

    result[node.variable.id] = node.values
  }

  return result
}

/*
  Given a list of `variables`, execute their queries to retrieve the possible
  values for each variable.

  Since variables can make use of other variables, this process is more
  complicated then simply executing every query found in a variable. Instead,
  we:

  1. Construct a graph that represents the dependency structure between
     variables. A variable `a` depends on variable `b` if the query for `a`
     uses the variable `b`.
     
     Each node in the graph has a hydration status: it it either `NotStarted`,
     `Loading` (values are being fetched), `Error` (values failed to fetch or
     cannot be fetched), or `Done` (values have been fetched successfully).
     When the graph is constructed, all nodes a marked as `NotStarted`.

  2. Find all cycles in the graph and mark nodes within a cycle and nodes that
     lead to a cycle as `Error`. We cannot resolve variables with cyclical
     dependencies.

  3. Find the leaves of the graph (the nodes with no dependencies) and begin to
     load their values.

     a. If loading the values succeeds, we mark that node as `Done` and check
        if every child of the parent of this node is `Done`. If this is the
        case, we start loading the parent.

     b. If loading the values fails, we mark that node and every one of its
        ancestors as as `Error`.

     c. If the parent node is `Error`, we stop resolving along this path. Steps
        2 and 3b guarantee that every ancestor of this `Error` node is also
        `Error`.

  4. By the time all node resolution has succeeded or failed, every node in the
     graph has now either been resolved with values or marked with `Error`.

*/
export const hydrateVars = (
  variables: Variable[],
  allVariables: Variable[],
  options: HydrateVarsOptions
): WrappedCancelablePromise<VariableValuesByID> => {
  const graph = createVariableGraph(variables, allVariables)

  invalidateCycles(graph)

  let isCancelled = false

  const resolve = async (node: VariableNode) => {
    if (isCancelled) {
      return
    }

    node.status === RemoteDataState.Loading

    try {
      node.values = await hydrateVarsHelper(node, options)
      node.status = RemoteDataState.Done

      return Promise.all(node.parents.filter(readyToResolve).map(resolve))
    } catch (e) {
      if (e instanceof CancellationError) {
        return
      }

      node.status = RemoteDataState.Error

      invalidateAncestors(node)
    }
  }

  const deferred = new Deferred()

  const cancel = () => {
    isCancelled = true
    graph.forEach(node => node.cancel())
    deferred.reject(new CancellationError())
  }

  Promise.all(findLeaves(graph).map(resolve)).then(() => {
    deferred.resolve(extractResult(graph))
  })

  return {promise: deferred.promise, cancel}
}
