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

const queryConfigs = (state = {}, action) => {
  switch (action.type) {
    case 'DE_CHOOSE_NAMESPACE': {
      const {queryID, database, retentionPolicy} = action.payload
      const nextQueryConfig = chooseNamespace(state[queryID], {
        database,
        retentionPolicy,
      })

      return Object.assign({}, state, {
        [queryID]: Object.assign(nextQueryConfig, {rawText: null}),
      })
    }

    case 'DE_CHOOSE_MEASUREMENT': {
      const {queryID, measurement} = action.payload
      const nextQueryConfig = chooseMeasurement(state[queryID], measurement)

      return Object.assign({}, state, {
        [queryID]: Object.assign(nextQueryConfig, {
          rawText: state[queryID].rawText,
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
      const {queryID, rawText} = action.payload
      const nextQueryConfig = editRawText(state[queryID], rawText)

      return Object.assign({}, state, {
        [queryID]: nextQueryConfig,
      })
    }

    case 'DE_GROUP_BY_TIME': {
      const {queryID, time} = action.payload
      const nextQueryConfig = groupByTime(state[queryID], time)

      return Object.assign({}, state, {
        [queryID]: nextQueryConfig,
      })
    }

    case 'DE_TOGGLE_TAG_ACCEPTANCE': {
      const {queryID} = action.payload
      const nextQueryConfig = toggleTagAcceptance(state[queryID])

      return Object.assign({}, state, {
        [queryID]: nextQueryConfig,
      })
    }

    case 'DE_TOGGLE_FIELD': {
      const {queryID, fieldFunc} = action.payload
      const nextQueryConfig = toggleField(state[queryID], fieldFunc)

      return Object.assign({}, state, {
        [queryID]: {...nextQueryConfig, rawText: null},
      })
    }

    case 'DE_APPLY_FUNCS_TO_FIELD': {
      const {queryID, fieldFunc, groupBy} = action.payload
      const nextQueryConfig = applyFuncsToField(
        state[queryID],
        fieldFunc,
        groupBy
      )

      return Object.assign({}, state, {
        [queryID]: nextQueryConfig,
      })
    }

    case 'DE_CHOOSE_TAG': {
      const {queryID, tag} = action.payload
      const nextQueryConfig = chooseTag(state[queryID], tag)

      return Object.assign({}, state, {
        [queryID]: nextQueryConfig,
      })
    }

    case 'DE_GROUP_BY_TAG': {
      const {queryID, tagKey} = action.payload
      const nextQueryConfig = groupByTag(state[queryID], tagKey)
      return Object.assign({}, state, {
        [queryID]: nextQueryConfig,
      })
    }

    case 'DE_FILL': {
      const {queryID, value} = action.payload
      const nextQueryConfig = fill(state[queryID], value)

      return {
        ...state,
        [queryID]: nextQueryConfig,
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
      const {queryID, fields} = action.payload
      const nextQuery = removeFuncs(state[queryID], fields)

      // fields with no functions cannot have a group by time
      return {...state, [queryID]: nextQuery}
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
