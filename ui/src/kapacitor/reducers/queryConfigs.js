import defaultQueryConfig from 'src/utils/defaultQueryConfig'
import {
  timeShift,
  chooseTag,
  groupByTag,
  groupByTime,
  removeFuncs,
  chooseNamespace,
  toggleKapaField,
  applyFuncsToField,
  chooseMeasurement,
  toggleTagAcceptance,
} from 'src/utils/queryTransitions'

const IS_KAPACITOR_RULE = true

const queryConfigs = (state = {}, action) => {
  switch (action.type) {
    case 'KAPA_LOAD_QUERY': {
      const {query} = action.payload
      const nextState = Object.assign({}, state, {
        [query.id]: query,
      })

      return nextState
    }

    case 'KAPA_ADD_QUERY': {
      const {queryID} = action.payload

      return {
        ...state,
        [queryID]: defaultQueryConfig({id: queryID, isKapacitorRule: true}),
      }
    }

    case 'KAPA_CHOOSE_NAMESPACE': {
      const {queryID, database, retentionPolicy} = action.payload
      const nextQueryConfig = chooseNamespace(
        state[queryID],
        {
          database,
          retentionPolicy,
        },
        IS_KAPACITOR_RULE
      )

      return Object.assign({}, state, {
        [queryID]: Object.assign(nextQueryConfig, {rawText: null}),
      })
    }

    case 'KAPA_CHOOSE_MEASUREMENT': {
      const {queryID, measurement} = action.payload
      const nextQueryConfig = chooseMeasurement(
        state[queryID],
        measurement,
        IS_KAPACITOR_RULE
      )

      return Object.assign({}, state, {
        [queryID]: Object.assign(nextQueryConfig, {
          rawText: state[queryID].rawText,
        }),
      })
    }

    case 'KAPA_CHOOSE_TAG': {
      const {queryID, tag} = action.payload
      const nextQueryConfig = chooseTag(state[queryID], tag)

      return Object.assign({}, state, {
        [queryID]: nextQueryConfig,
      })
    }

    case 'KAPA_GROUP_BY_TAG': {
      const {queryID, tagKey} = action.payload
      const nextQueryConfig = groupByTag(state[queryID], tagKey)
      return Object.assign({}, state, {
        [queryID]: nextQueryConfig,
      })
    }

    case 'KAPA_TOGGLE_TAG_ACCEPTANCE': {
      const {queryID} = action.payload
      const nextQueryConfig = toggleTagAcceptance(state[queryID])

      return Object.assign({}, state, {
        [queryID]: nextQueryConfig,
      })
    }

    case 'KAPA_TOGGLE_FIELD': {
      const {queryID, fieldFunc} = action.payload
      const nextQueryConfig = toggleKapaField(state[queryID], fieldFunc)

      return {...state, [queryID]: {...nextQueryConfig, rawText: null}}
    }

    case 'KAPA_APPLY_FUNCS_TO_FIELD': {
      const {queryID, fieldFunc} = action.payload
      const {groupBy} = state[queryID]
      const nextQueryConfig = applyFuncsToField(state[queryID], fieldFunc, {
        ...groupBy,
        time: groupBy.time ? groupBy.time : '10s',
      })

      return {...state, [queryID]: nextQueryConfig}
    }

    case 'KAPA_GROUP_BY_TIME': {
      const {queryID, time} = action.payload
      const nextQueryConfig = groupByTime(state[queryID], time)

      return Object.assign({}, state, {
        [queryID]: nextQueryConfig,
      })
    }

    case 'KAPA_REMOVE_FUNCS': {
      const {queryID, fields} = action.payload
      const nextQuery = removeFuncs(state[queryID], fields)

      // fields with no functions cannot have a group by time
      return {...state, [queryID]: nextQuery}
    }
  }
  return state
}

export default queryConfigs
