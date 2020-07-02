// Constants
import {defaultBuilderConfig} from 'src/views/helpers'

interface ViewWithQuery {
  properties: {
    queries: Array<{
      editMode?: 'advanced' | 'builder'
      builderConfig?: {functions?: Array<{name?: string}>}
      text?: string
    }>
  }
}

function applyQueryBuilderRequirements<T extends ViewWithQuery>(view: T): T {
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

export default applyQueryBuilderRequirements
