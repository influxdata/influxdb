// Utils
import {valueFetcher, ValueFetcher} from 'src/variables/utils/ValueFetcher'
import Deferred from 'src/utils/Deferred'
import {asAssignment} from 'src/variables/selectors'

// Constants
import {OPTION_NAME, BOUNDARY_GROUP} from 'src/variables/constants/index'

// Types
import {
  RemoteDataState,
  Variable,
  VariableValues,
  ValueSelections,
} from 'src/types'
import {CancelBox, CancellationError} from 'src/types/promises'

export interface VariableNode {
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
  selections?: ValueSelections
  fetcher?: ValueFetcher
  skipCache?: boolean
}

export interface EventedCancelBox<T> extends CancelBox<T> {
  on?: any
}

export const createVariableGraph = (
  allVariables: Variable[]
): VariableNode[] => {
  const nodesByID: {[variableID: string]: VariableNode} = allVariables.reduce(
    (prev, curr) => {
      let status = RemoteDataState.Done
      if (curr.arguments.type === 'query') {
        status = RemoteDataState.NotStarted
      }
      prev[curr.id] = {
        variable: curr,
        values: null,
        parents: [],
        children: [],
        status,
        cancel: () => {},
      }
      return prev
    },
    {}
  )

  // Then initialize all the edges (the `parents` and `children` references)
  Object.keys(nodesByID)
    .filter(k => nodesByID[k].variable.arguments.type === 'query')
    .forEach(k => {
      getVarChildren(nodesByID[k].variable, allVariables)
        .map(child => child.id)
        .forEach(c => {
          nodesByID[k].children.push(nodesByID[c])
          nodesByID[c].parents.push(nodesByID[k])
        })
    })

  return Object.values(nodesByID)
}

export const isInQuery = (query: string, v: Variable) => {
  const regexp = new RegExp(
    `${BOUNDARY_GROUP}${OPTION_NAME}.${v.name}${BOUNDARY_GROUP}`
  )

  return regexp.test(query)
}

const getVarChildren = (
  {
    arguments: {
      values: {query},
    },
  }: Variable,
  allVariables: Variable[]
) => allVariables.filter(maybeChild => isInQuery(query, maybeChild))

/*
  Collect all ancestors of a node.

  A node `a` is an ancestor of `b` if there exists a path from `a` to `b`.

  This function is safe to call on a node within a graph with cycles.
*/
const collectAncestors = (
  node: VariableNode,
  acc: Set<VariableNode> = new Set()
): VariableNode[] => {
  for (const parent of node.parents) {
    if (!acc.has(parent)) {
      acc.add(parent)
      collectAncestors(parent, acc)
    }
  }

  return [...acc]
}

/*
  Collect youngest child of a node (or the tail of the LinkedList within our graph).

  A node `a` is a child of `b` if there exists a path from `b` to `a`.

  This function determines the root child in order to easily hydrate parent variables and
  resolve and prevent rehydrating variables that have already been passed in.

  In the example below, we have 3 variables `a`, `b`, `c` where a is a parent to b, which is a parent to c:

  a --> b --> c

  If `a`, `b`, or `c` are passed into this function, we should expect that the output for each should be:

  `c` with a reference to its parent `b`, which should have a reference to its parent `a`
*/
const getRootChildNode = (
  node: VariableNode,
  acc: Set<VariableNode> = new Set(),
  cache: {[key: string]: boolean | undefined} = {}
): VariableNode => {
  if (node.children.length === 0) {
    return node
  }
  for (const child of node.children) {
    // by checking the cache for existing variables, we ensure that the graph stops
    // when an existing node has already been encountered, invalidating a cycle if one exists
    if (child.children.length > 0 && !cache[child.variable.id]) {
      cache[child.variable.id] = true
      return getRootChildNode(child, acc, cache)
    }
    return child
  }
}

/*
  Filters out any parents that have already been referenced in a previous node

  A node `a` & `c` are children of `b` if there exists a path:

            b
          /   \
         a     c

  This function safely filters out a duplicate reference to the parent `b`
  When both `a` and `c` are passed as variables to be hydrated, since `b` should
  already be `hydrated` by the first variable that is passed in.
*/
export interface DeduplicatedRoot {
  node: VariableNode
  subsetIDs: {[key: string]: boolean}
}

const getDeduplicatedRootChild = (
  node: VariableNode,
  subsetIDs: {[key: string]: boolean | undefined}
): DeduplicatedRoot => {
  for (const n of node.parents) {
    if (!subsetIDs[n.variable.id]) {
      subsetIDs[n.variable.id] = true
      getDeduplicatedRootChild(n, subsetIDs)
    } else {
      node.parents = []
    }
  }
  return {node, subsetIDs}
}

/*
  Given a variable graph, return the minimal subgraph containing only the nodes
  needed to hydrate the values for variables in the passed `variables` argument.

  We discard all nodes in the graph unless:

  - It is the node for one of the passed variables
  - The node for one of the passed variables depends on this node

*/
export const findSubgraph = (
  graph: VariableNode[],
  variables: Variable[]
): VariableNode[] => {
  const subgraph: Set<VariableNode> = new Set()
  // use an ID array to reduce the chance of reference errors
  const varIDs = variables.map(v => v.id)
  // create an ID reference object to identify relevant root variables to hydrate
  let subgraphIDs = {}
  for (const node of graph) {
    const shouldKeep = varIDs.includes(node.variable.id)

    // get youngest child of the node in order to find the lowest common denominator amongst the nodes
    const rootChild = getRootChildNode(node)
    // by checking whether the subgraphIDs[rootChild.variable.id] !== true, we are ensuring that the root
    // node has not yet been added to the subgraph. Therefore, if a rootChild has already been added to the
    // subgraph, we prevent that rootChild from being added & hydrated again
    if (shouldKeep && subgraphIDs[rootChild.variable.id] !== true) {
      if (rootChild.parents.length > 0) {
        /*
          Once a node exists within the subgraph (whether nested as a parent or as the rootNode)
          we want to remove any further to that node within the subgraph.
          This can be particularly challenging when a parent node has multiple child nodes that have been passed in.
          For example, if variables `a` and `b` are both children to `c`,
          and `a` & `b` are both variables that should be hydrated, we would need to reset a parent
          reference for one of the variables so that `c` is not hydrated twice.
          This can be achieved by storing a reference to all the existing nodeIDs within the subgraph to the `graphIDs`
          and checking for any collisions before adding the node to the subgraph.
          If a parent collision is detected, that node's parent is simply set to [] since we know that
          the parent was already hydrated based on a previous input
        */
        const {node: filteredNodes, subsetIDs} = getDeduplicatedRootChild(
          rootChild,
          subgraphIDs
        )
        subgraph.add(filteredNodes)
        subgraphIDs[rootChild.variable.id] = true
        subgraphIDs = {
          [rootChild.variable.id]: true,
          ...subgraphIDs,
          ...subsetIDs,
        }
      } else {
        subgraph.add(rootChild)
        subgraphIDs[rootChild.variable.id] = true
      }
    }
  }

  return [...subgraph]
}

/*
  Get the `VariableValues` for a variable that cannot be successfully hydrated.
*/
const errorVariableValues = (
  message = 'Failed to load values for variable'
): VariableValues => ({
  values: null,
  selected: null,
  valueType: null,
  error: message,
})

/*
  Find all the descendants of a node.

  A node `b` is a descendant of `a` if there exists a path from `a` to `b`.

  Checks visited to prevent looping forever
*/
export const collectDescendants = (
  node: VariableNode,
  acc: Set<VariableNode> = new Set()
): VariableNode[] => {
  for (const child of node.children) {
    if (!acc.has(child)) {
      acc.add(child)
      collectDescendants(child, acc)
    }
  }

  return [...acc]
}

/*
  Hydrate the values of a single node in the graph.

  This assumes that every descendant of this node has already been hydrated.
*/
// TODO: figure out how to type the `on` function
const hydrateVarsHelper = async (
  node: VariableNode,
  options: HydrateVarsOptions,
  on?: any
): Promise<VariableValues> => {
  const variableType = node.variable.arguments.type

  // this assumes that the variable hydration is done in the selector 'getVariable'
  if (variableType === 'map') {
    return {
      valueType: 'string',
      values: node.variable.arguments.values,
      selected: node.variable.selected,
    }
  }

  if (variableType === 'constant') {
    return {
      valueType: 'string',
      values: node.variable.arguments.values,
      selected: node.variable.selected,
    }
  }

  if (variableType === 'system') {
    return {
      valueType: 'string',
      values: node.variable.arguments.values,
      selected: node.variable.selected,
    }
  }

  if (node.status !== RemoteDataState.Loading) {
    node.status = RemoteDataState.Loading
    on.fire('status', node.variable, node.status)
    collectAncestors(node)
      .filter(parent => parent.variable.arguments.type === 'query')
      .forEach(parent => {
        if (parent.status !== RemoteDataState.Loading) {
          parent.status = RemoteDataState.Loading
          on.fire('status', parent.variable, parent.status)
        }
      })
  }

  const descendants = collectDescendants(node)
  const assignments = descendants
    .map(node => asAssignment(node.variable))
    .filter(v => !!v)

  const {url, orgID} = options
  const {query} = node.variable.arguments.values
  const fetcher = options.fetcher || valueFetcher

  const request = fetcher.fetch(
    url,
    orgID,
    query,
    assignments,
    null,
    '',
    options.skipCache
  )

  node.cancel = request.cancel

  const values = await request.promise

  // NOTE: do not fire `done` event here, as the value
  // has not been properly hydrated yet
  node.status = RemoteDataState.Done
  return values
}

/*
  Check if a node is `NotStarted` and if every child of the node has been
  resolved (successfully or not).
*/
const readyToResolve = (parent: VariableNode): boolean =>
  parent.children.every(child => child.status === RemoteDataState.Done)

/*
  Find all `NotStarted` nodes in the graph that have no children.
*/
const findLeaves = (graph: VariableNode[]): VariableNode[] =>
  graph.filter(node => node.children.length === 0)

/*
  Given a node, attempt to find a cycle that the node is a part of. If no cycle
  is found, return `null`.
*/
const findCyclicPath = (node: VariableNode): VariableNode[] => {
  try {
    findCyclicPathHelper(node, [], {})
  } catch (cyclicPath) {
    return cyclicPath
  }

  return null
}

const findCyclicPathHelper = (
  node: VariableNode,
  seen: VariableNode[],
  cache: {[key: string]: undefined | boolean} = {}
): void => {
  if (cache[node.variable.id]) {
    throw seen
  }

  for (const child of node.children) {
    findCyclicPathHelper(child, [...seen, node], {
      ...cache,
      [node.variable.id]: true,
    })
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
*/
const invalidateAncestors = (node: VariableNode): void => {
  const ancestors = collectAncestors(node)

  for (const ancestor of ancestors) {
    ancestor.status = RemoteDataState.Error
    if (ancestor.variable.arguments.type === 'query') {
      ancestor.variable.arguments.values.results = []
    }
  }
}

const extractResult = (graph: VariableNode[]): Variable[] => {
  const result = {}

  for (const node of graph) {
    if (node.status === RemoteDataState.Error) {
      node.values = errorVariableValues()
    }

    result[node.variable.id] = node.variable
  }

  return Object.values(result)
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
): EventedCancelBox<Variable[]> => {
  const graph = findSubgraph(
    createVariableGraph(allVariables),
    variables
  ).filter(n => n.variable.arguments.type !== 'system')
  invalidateCycles(graph)

  let isCancelled = false

  const resolve = async (node: VariableNode) => {
    if (isCancelled) {
      return
    }

    try {
      // TODO: terminate the concept of node.values at the fetcher and just use variables
      node.values = await hydrateVarsHelper(node, options, on)

      if (node.variable.arguments.type === 'query') {
        node.variable.arguments.values.results = node.values.values as string[]
      } else {
        node.variable.arguments.values = node.values.values
      }

      node.variable.selected = node.variable.selected || []

      // ensure that the selected value defaults propegate for
      // nested queryies.
      if (
        node.variable.arguments.type === 'query' ||
        node.variable.arguments.type === 'constant'
      ) {
        if (
          !(node.values.values as string[]).includes(node.variable.selected[0])
        ) {
          node.variable.selected = []
        }
      } else if (node.variable.arguments.type === 'map') {
        if (
          !Object.keys(node.values.values).includes(node.variable.selected[0])
        ) {
          node.variable.selected = []
        }
      }

      if (!node.variable.selected || !node.variable.selected[0]) {
        node.variable.selected = node.values.selected
      }

      on.fire('status', node.variable, node.status)

      return Promise.all(node.parents.filter(readyToResolve).map(resolve))
    } catch (e) {
      if (e.name === 'CancellationError') {
        return
      }

      node.status = RemoteDataState.Error
      node.variable.arguments.values.results = []

      invalidateAncestors(node)
    }
  }

  const deferred = new Deferred()

  const cancel = () => {
    isCancelled = true
    graph.forEach(node => node.cancel())
    deferred.reject(new CancellationError())
  }

  const on = (function() {
    const callbacks = {}
    const ret = (evt, cb) => {
      if (!callbacks.hasOwnProperty(evt)) {
        callbacks[evt] = []
      }

      callbacks[evt].push(cb)
    }

    ret.fire = (evt, ...args) => {
      if (!callbacks.hasOwnProperty(evt)) {
        return
      }

      callbacks[evt].forEach(cb => cb.apply(cb, args))
    }

    return ret
  })()

  // NOTE: wrapping in a resolve disconnects the following findLeaves
  // from the main execution thread, allowing external services to
  // register listeners for the loading state changes
  Promise.resolve()
    .then(() => Promise.all(findLeaves(graph).map(resolve)))
    .then(() => {
      deferred.resolve(extractResult(graph))
    })

  return {promise: deferred.promise, cancel, on}
}
