// Utils
import {
  collectDescendants,
  createVariableGraph,
  VariableNode,
} from 'src/variables/utils/hydrateVars'

// Types
import {Variable} from 'src/types'

const getDescendantsFromGraph = (
  variable: Variable,
  varGraph: VariableNode[]
): Variable[] => {
  const node = varGraph.find(n => n.variable.id === variable.id)
  return collectDescendants(node).map(n => n.variable)
}

export const findDependentVariables = (
  variable: Variable,
  allVariables: Variable[]
) => {
  const varGraph = createVariableGraph(allVariables)
  return getDescendantsFromGraph(variable, varGraph)
}

export const exportVariables = (
  variables: Variable[],
  allVariables: Variable[]
): Variable[] => {
  const varSet = new Set<Variable>()
  const varGraph = createVariableGraph(allVariables)

  for (const v of variables) {
    if (varSet.has(v)) {
      continue
    }

    varSet.add(v)
    for (const d of getDescendantsFromGraph(v, varGraph)) {
      varSet.add(d)
    }
  }

  return [...varSet]
}
