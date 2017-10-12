import _ from 'lodash'

import defaultQueryConfig from 'src/utils/defaultQueryConfig'
import {
  fill,
  chooseTag,
  groupByTag,
  removeFuncs,
  groupByTime,
  toggleField,
  editRawText,
  updateRawQuery,
  chooseNamespace,
  chooseMeasurement,
  addInitialField,
  applyFuncsToField,
  toggleTagAcceptance,
} from 'src/utils/queryTransitions'
import {INITIAL_GROUP_BY_TIME} from 'src/data_explorer/constants'

const queryConfigs = (state = {}, action) => {
  switch (action.type) {
    case 'DE_CHOOSE_NAMESPACE': {
      const {queryId, database, retentionPolicy} = action.payload
      const nextQueryConfig = chooseNamespace(state[queryId], {
        database,
        retentionPolicy,
      })

      return Object.assign({}, state, {
        [queryId]: Object.assign(nextQueryConfig, {rawText: null}),
      })
    }

    case 'DE_CHOOSE_MEASUREMENT': {
      const {queryId, measurement} = action.payload
      const nextQueryConfig = chooseMeasurement(state[queryId], measurement)

      return Object.assign({}, state, {
        [queryId]: Object.assign(nextQueryConfig, {
          rawText: state[queryId].rawText,
        }),
      })
    }

    // there is an additional reducer for this same action in the ui reducer
    case 'DE_ADD_QUERY': {
      const {queryID} = action.payload

      return {
        ...state,
        [queryID]: defaultQueryConfig({id: queryID}),
      }
    }

    // there is an additional reducer for this same action in the ui reducer
    case 'DE_DELETE_QUERY': {
      const {queryID} = action.payload
      return _.omit(state, queryID)
    }

    case 'DE_UPDATE_QUERY_CONFIG': {
      const {config} = action.payload
      return {...state, [config.id]: config}
    }

    case 'DE_EDIT_RAW_TEXT': {
      const {queryId, rawText} = action.payload
      const nextQueryConfig = editRawText(state[queryId], rawText)

      return Object.assign({}, state, {
        [queryId]: nextQueryConfig,
      })
    }

    case 'DE_GROUP_BY_TIME': {
      const {queryId, time} = action.payload
      const nextQueryConfig = groupByTime(state[queryId], time)

      return Object.assign({}, state, {
        [queryId]: nextQueryConfig,
      })
    }

    case 'DE_TOGGLE_TAG_ACCEPTANCE': {
      const {queryId} = action.payload
      const nextQueryConfig = toggleTagAcceptance(state[queryId])

      return Object.assign({}, state, {
        [queryId]: nextQueryConfig,
      })
    }

    case 'DE_TOGGLE_FIELD': {
      const {queryId, fieldFunc} = action.payload
      const nextQueryConfig = toggleField(state[queryId], fieldFunc)

      return Object.assign({}, state, {
        [queryId]: {...nextQueryConfig, rawText: null},
      })
    }

    case 'DE_APPLY_FUNCS_TO_FIELD': {
      const {queryId, fieldFunc} = action.payload
      const nextQueryConfig = applyFuncsToField(
        state[queryId],
        fieldFunc,
        INITIAL_GROUP_BY_TIME
      )

      return Object.assign({}, state, {
        [queryId]: nextQueryConfig,
      })
    }

    case 'DE_CHOOSE_TAG': {
      const {queryId, tag} = action.payload
      const nextQueryConfig = chooseTag(state[queryId], tag)

      return Object.assign({}, state, {
        [queryId]: nextQueryConfig,
      })
    }

    case 'DE_GROUP_BY_TAG': {
      const {queryId, tagKey} = action.payload
      const nextQueryConfig = groupByTag(state[queryId], tagKey)
      return Object.assign({}, state, {
        [queryId]: nextQueryConfig,
      })
    }

    case 'DE_FILL': {
      const {queryId, value} = action.payload
      const nextQueryConfig = fill(state[queryId], value)

      return {
        ...state,
        [queryId]: nextQueryConfig,
      }
    }

    case 'DE_UPDATE_RAW_QUERY': {
      const {queryID, text} = action.payload
      const nextQueryConfig = updateRawQuery(state[queryID], text)
      return Object.assign({}, state, {
        [queryID]: nextQueryConfig,
      })
    }

    case 'DE_EDIT_QUERY_STATUS': {
      const {queryID, status} = action.payload
      const nextState = {
        [queryID]: {...state[queryID], status},
      }

      return {...state, ...nextState}
    }

    case 'DE_REMOVE_FUNCS': {
      const {queryID, fields, groupBy} = action.payload

      // fields with no functions cannot have a group by time
      const nextState = {
        [queryID]: {
          ...state[queryID],
          fields: removeFuncs(fields),
          groupBy: {...groupBy, time: null},
        },
      }

      return {...state, ...nextState}
    }

    // Adding the first feild applies a groupBy time
    case 'DE_ADD_INITIAL_FIELD': {
      const {queryID, field, groupBy} = action.payload
      const nextQuery = addInitialField(state[queryID], field, groupBy)

      return {...state, [queryID]: nextQuery}
    }
  }
  return state
}

export default queryConfigs
