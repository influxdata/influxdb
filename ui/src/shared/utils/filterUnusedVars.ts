// Utils
import {isInQuery} from 'src/variables/utils/hydrateVars'

// Types
import {QueryView} from 'src/types/dashboards'
import {IVariable as Variable, View} from '@influxdata/influx'

/*
  Given a collection variables and a collection of views, return only the
  variables that are used in at least one of the view queries.
*/
export const filterUnusedVars = (variables: Variable[], views: View[]) => {
  const queryViews: QueryView[] = views.filter(
    view => !!view.properties.queries
  )

  const queryTexts = queryViews.reduce(
    (acc, view) => [
      ...acc,
      ...view.properties.queries.map(query => query.text),
    ],
    []
  )

  const varsInUse = variables.filter(variable =>
    queryTexts.some(text => isInQuery(text, variable))
  )

  return varsInUse
}
