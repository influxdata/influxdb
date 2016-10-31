import defaultQueryConfig from 'src/utils/defaultQueryConfig';
import {
  applyFuncsToField,
  chooseMeasurement,
  chooseNamespace,
  chooseTag,
  groupByTag,
  groupByTime,
  toggleField,
  toggleTagAcceptance,
  updateRawQuery,
} from 'src/utils/queryTransitions';
import update from 'react-addons-update';

export default function queryConfigs(state = {}, action) {
  switch (action.type) {
    case 'LOAD_EXPLORER': {
      return action.payload.explorer.data.queryConfigs;
    }

    case 'CHOOSE_NAMESPACE': {
      const {queryId, database, retentionPolicy} = action.payload;
      const nextQueryConfig = chooseNamespace(defaultQueryConfig(queryId), {database, retentionPolicy});

      return Object.assign({}, state, {
        [queryId]: nextQueryConfig,
      });
    }

    case 'CHOOSE_MEASUREMENT': {
      const {queryId, measurement} = action.payload;
      const nextQueryConfig = chooseMeasurement(state[queryId], measurement);

      return Object.assign({}, state, {
        [queryId]: nextQueryConfig,
      });
    }

    case 'CREATE_PANEL':
    case 'ADD_QUERY': {
      const {queryId} = action.payload;
      const nextState = Object.assign({}, state, {
        [queryId]: defaultQueryConfig(queryId),
      });

      return nextState;
    }

    case 'UPDATE_QUERY': {
      const {queryId, updates} = action.payload;
      const nextState = update(state, {
        [queryId]: {$merge: updates},
      });

      return nextState;
    }

    case 'GROUP_BY_TIME': {
      const {queryId, time} = action.payload;
      const nextQueryConfig = groupByTime(state[queryId], time);

      return Object.assign({}, state, {
        [queryId]: nextQueryConfig,
      });
    }

    case 'TOGGLE_TAG_ACCEPTANCE': {
      const {queryId} = action.payload;
      const nextQueryConfig = toggleTagAcceptance(state[queryId]);

      return Object.assign({}, state, {
        [queryId]: nextQueryConfig,
      });
    }

    case 'DELETE_QUERY': {
      const {queryId} = action.payload;
      const nextState = update(state, {$apply: (configs) => {
        delete configs[queryId];
        return configs;
      }});

      return nextState;
    }

    case 'TOGGLE_FIELD': {
      const {queryId, fieldFunc} = action.payload;
      const nextQueryConfig = toggleField(state[queryId], fieldFunc);

      return Object.assign({}, state, {
        [queryId]: nextQueryConfig,
      });
    }

    case 'APPLY_FUNCS_TO_FIELD': {
      const {queryId, fieldFunc} = action.payload;
      const nextQueryConfig = applyFuncsToField(state[queryId], fieldFunc);

      return Object.assign({}, state, {
        [queryId]: nextQueryConfig,
      });
    }

    case 'CHOOSE_TAG': {
      const {queryId, tag} = action.payload;
      const nextQueryConfig = chooseTag(state[queryId], tag);

      return Object.assign({}, state, {
        [queryId]: nextQueryConfig,
      });
    }

    case 'GROUP_BY_TAG': {
      const {queryId, tagKey} = action.payload;
      const nextQueryConfig = groupByTag(state[queryId], tagKey);
      return Object.assign({}, state, {
        [queryId]: nextQueryConfig,
      });
    }

    case 'UPDATE_RAW_QUERY': {
      const {queryID, text} = action.payload;
      const nextQueryConfig = updateRawQuery(state[queryID], text);
      return Object.assign({}, state, {
        [queryID]: nextQueryConfig,
      });
    }
  }
  return state;
}
