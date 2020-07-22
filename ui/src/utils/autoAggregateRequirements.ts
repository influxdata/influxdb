//Utils
import {buildQuery} from 'src/timeMachine/utils/queryBuilder'
import {get, isNull} from 'lodash'

// Constants
import {defaultBuilderConfig} from 'src/views/helpers'
import {
  AGG_WINDOW_AUTO,
  DEFAULT_FILLVALUES,
} from 'src/timeMachine/constants/queryBuilder'

interface ViewWithQuery {
  properties: {
    queries: Array<{
      editMode?: 'advanced' | 'builder'
      builderConfig?: {
        functions?: Array<{name?: string}>
        aggregateWindow?: {period?: string; fillValues?: boolean}
      }
      text?: string
    }>
  }
}

function applyAutoAggregateRequirements<T extends ViewWithQuery>(view: T): T {
  const autoAggregateableQueries = view.properties.queries.map(q => {
    if (q.editMode === 'advanced') {
      return q
    }

    const functions = get(q, 'builderConfig.functions', [])
    const queryText = get(q, 'text', '')
    const queryIsNotEmpty = !!queryText.trim()

    const originalQueryInScriptEditorMode = {
      ...q,
      editMode: 'advanced' as 'advanced',
      builderConfig: defaultBuilderConfig(),
    }

    if (functions.length === 0 && queryIsNotEmpty) {
      return originalQueryInScriptEditorMode
    }

    let alteredQuery = false

    let period = get(q, 'builderConfig.aggregateWindow.period', null)
    if (isNull(period)) {
      alteredQuery = true
      period = AGG_WINDOW_AUTO
    }

    let fillValues = get(q, 'builderConfig.aggregateWindow.fillValues', null)
    if (isNull(fillValues)) {
      alteredQuery = true
      fillValues = DEFAULT_FILLVALUES
    }

    let builderConfig = get(q, 'builderConfig', {})
    builderConfig = {...builderConfig, aggregateWindow: {period, fillValues}}

    const originalQueryText = get(q, 'text', '')

    try {
      const text = alteredQuery ? buildQuery(builderConfig) : originalQueryText

      return {
        ...q,
        text,
        builderConfig,
      }
    } catch (e) {
      return originalQueryInScriptEditorMode
    }
  })

  return {
    ...view,
    properties: {...view.properties, queries: autoAggregateableQueries},
  }
}

export default applyAutoAggregateRequirements
