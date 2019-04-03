// Utils
import {
  collectDescendants,
  createVariableGraph,
  VariableNode,
} from 'src/variables/utils/hydrateVars'

// Types
import {Variable} from '@influxdata/influx'

export const findDependentVariables = (
  variable: Variable,
  varGraph: VariableNode[]
): Variable[] => {
  const node = varGraph.find(n => n.variable.id === variable.id)
  return collectDescendants(node).map(n => n.variable)
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
    for (const d of findDependentVariables(v, varGraph)) {
      varSet.add(d)
    }
  }

  return [...varSet]
}
