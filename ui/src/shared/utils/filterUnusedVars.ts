// Utils
import {isInQuery} from 'src/variables/utils/hydrateVars'

// Types
import {QueryViewProperties, View, ViewProperties, Variable} from 'src/types'

function isQueryViewProperties(vp: ViewProperties): vp is QueryViewProperties {
  return (vp as QueryViewProperties).queries !== undefined
}

/*
  Given a collection variables and a collection of views, return only the
  variables that are used in at least one of the view queries.
*/
export const filterUnusedVars = (variables: Variable[], views: View[]) => {
  const viewProperties = views.map(v => v.properties).filter(vp => !!vp)
  const queryViewProperties = viewProperties.filter(isQueryViewProperties)

  const queryTexts = queryViewProperties.reduce(
    (acc, vp) => [...acc, ...vp.queries.map(query => query.text)],
    [] as Array<string>
  )

  const varsInUse = variables.filter(variable =>
    queryTexts.some(text => isInQuery(text, variable))
  )

  return varsInUse
}
