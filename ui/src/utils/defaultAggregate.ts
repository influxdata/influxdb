// Constants
import {defaultBuilderConfig} from 'src/views/helpers'

// Types
import {QueryView} from 'src/types'

export const applyQueryBuilderRequirements = (view: QueryView): QueryView => {
  const correctedQueries = view.properties.queries.map(q => {
    if (
      q.editMode === 'builder' &&
      q.builderConfig.functions.length === 0 &&
      !!q.text.trim()
    ) {
      return {
        ...q,
        editMode: 'advanced' as 'advanced',
        builderConfig: defaultBuilderConfig(),
      }
    }
    return q
  })
  return {...view, properties: {...view.properties, queries: correctedQueries}}
}
