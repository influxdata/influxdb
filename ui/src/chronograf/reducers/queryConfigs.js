import defaultQueryConfig from 'src/utils/defaultQueryConfig';
import {chooseNamespace, chooseMeasurement, toggleField} from 'src/utils/queryTransitions';
import update from 'react-addons-update';
import u from 'updeep';

export default function queryConfigs(state = {}, action) {
  switch (action.type) {
    case 'LOAD_EXPLORER': {
      return action.payload.explorer.data.queryConfigs;
    }

    case 'CHOOSE_NAMESPACE': {
      const {queryId, database, retentionPolicy} = action.payload;
      const nextQueryConfig = chooseNamespace(defaultQueryConfig(queryId), {database, retentionPolicy});
      const nextState = update(state, {
        [queryId]: {$set: nextQueryConfig},
      });
      return nextState;
    }

    case 'CHOOSE_MEASUREMENT': {
      const {queryId, measurement} = action.payload;
      const nextQueryConfig = chooseMeasurement(state[queryId], measurement);
      const nextState = update(state, {
        [queryId]: {$set: nextQueryConfig},
      });
      return nextState;
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
      const nextState = update(state, {
        [queryId]: {
          groupBy: {
            time: {$set: time},
          },
        },
      });

      return nextState;
    }

    case 'TOGGLE_TAG_ACCEPTANCE': {
      const {queryId} = action.payload;
      const nextState = update(state, {
        [queryId]: {
          areTagsAccepted: {$set: !state[queryId].areTagsAccepted},
        },
      });

      return nextState;
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
      const {field, funcs} = fieldFunc;
      const shouldRemoveFuncs = funcs.length === 0;

      const nextState = update(state, {
        [queryId]: {$apply: (queryConfig) => {
          const nextFields = queryConfig.fields.map((f) => {
            // If one field has no funcs, all fields must have no funcs
            if (shouldRemoveFuncs) {
              return update(f, {funcs: {$set: []}});
            }

            if (f.field === field || !f.funcs || !f.funcs.length) {
              return update(f, {funcs: {$set: funcs}});
            }

            return f;
          });

          // If there are no functions, then there should be no GROUP BY time
          if (shouldRemoveFuncs) {
            const nextGroupBy = update(state[queryId].groupBy, {time: {$set: null}});
            return update(queryConfig, {fields: {$set: nextFields}, groupBy: {$set: nextGroupBy}});
          }

          return update(queryConfig, {fields: {$set: nextFields}});
        }},
      });

      return nextState;
    }

    case 'CHOOSE_TAG': {
      const {queryId, tag} = action.payload;

      const tagValues = state[queryId].tags[tag.key];
      const shouldRemoveTag = tagValues && tagValues.length === 1 && tagValues[0] === tag.value;
      if (shouldRemoveTag) {
        const nextState = update(state, {
          [queryId]: {
            tags: {$apply: (tags) => {
              const tagsCopy = Object.assign({}, tags);
              delete tagsCopy[tag.key];
              return tagsCopy;
            }},
          },
        });

        return nextState;
      }

      const nextState = update(state, {
        [queryId]: {
          tags: {
            [tag.key]: {$apply: (vals) => {
              if (!vals) {
                return [tag.value];
              }

              // If the tag value is already selected, deselect it by removing it from the list
              const valsCopy = vals.slice();
              const i = valsCopy.indexOf(tag.value);
              if (i > -1) {
                valsCopy.splice(i, 1);
                return valsCopy;
              }

              return update(valsCopy, {$push: [tag.value]});
            }},
          },
        },
      });

      return nextState;
    }

    case 'GROUP_BY_TAG': {
      const {queryId, tagKey} = action.payload;

      const nextState = update(state, {
        [queryId]: {
          groupBy: {
            tags: {$apply: (groupByTags) => {
              // If the tag value is already selected, deselect it by removing it from the list
              const groupByTagsCopy = groupByTags.slice();
              const i = groupByTagsCopy.indexOf(tagKey);
              if (i > -1) {
                groupByTagsCopy.splice(i, 1);
                return groupByTagsCopy;
              }

              return update(groupByTagsCopy, {$push: [tagKey]});
            }},
          },
        },
      });

      return nextState;
    }

    case 'UPDATE_RAW_QUERY': {
      const {queryID, text} = action.payload;

      const updateQuery = {
        [queryID]: {
          rawText: u.constant(text),
        },
      };

      return u(updateQuery, state);
    }
  }
  return state;
}
