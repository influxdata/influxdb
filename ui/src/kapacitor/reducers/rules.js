import {defaultRuleConfigs, DEFAULT_RULE_ID} from 'src/kapacitor/constants'
import _ from 'lodash'
import {parseAlerta} from 'shared/parsing/parseAlerta'

export default function rules(state = {}, action) {
  switch (action.type) {
    case 'LOAD_DEFAULT_RULE': {
      const {queryID} = action.payload
      return Object.assign({}, state, {
        [DEFAULT_RULE_ID]: {
          id: DEFAULT_RULE_ID,
          queryID,
          trigger: 'threshold',
          values: defaultRuleConfigs.threshold,
          message: '',
          alerts: [],
          alertNodes: [],
          every: null,
          name: 'Untitled Rule',
        },
      })
    }

    case 'LOAD_RULES': {
      const theRules = action.payload.rules
      const ruleIDs = theRules.map(r => r.id)
      return _.zipObject(ruleIDs, theRules)
    }

    case 'LOAD_RULE': {
      const {rule} = action.payload
      return Object.assign({}, state, {
        [rule.id]: rule,
      })
    }

    case 'CHOOSE_TRIGGER': {
      const trigger = action.payload.trigger
      const ruleID = action.payload.ruleID
      return Object.assign({}, state, {
        [ruleID]: Object.assign({}, state[ruleID], {
          trigger: trigger.toLowerCase(),
          values: defaultRuleConfigs[trigger.toLowerCase()],
        }),
      })
    }

    case 'ADD_EVERY': {
      const {ruleID, frequency} = action.payload
      return {...state, [ruleID]: {...state[ruleID], every: frequency}}
    }

    case 'REMOVE_EVERY': {
      const {ruleID} = action.payload
      return {...state, [ruleID]: {...state[ruleID], every: null}}
    }

    case 'UPDATE_RULE_VALUES': {
      const {ruleID, trigger, values} = action.payload
      return Object.assign({}, state, {
        [ruleID]: Object.assign({}, state[ruleID], {
          trigger: trigger.toLowerCase(),
          values,
        }),
      })
    }

    case 'UPDATE_RULE_MESSAGE': {
      const {ruleID, message} = action.payload
      return Object.assign({}, state, {
        [ruleID]: Object.assign({}, state[ruleID], {
          message,
        }),
      })
    }

    case 'UPDATE_RULE_ALERTS': {
      const {ruleID, alerts} = action.payload
      return Object.assign({}, state, {
        [ruleID]: Object.assign({}, state[ruleID], {
          alerts,
        }),
      })
    }

    // TODO: refactor to allow multiple alert nodes, and change name + refactor
    // functionality to clearly disambiguate creating an alert node, changing its
    // type, adding other alert nodes to a single rule, and updating an alert node's
    // properties vs args vs details vs message.
    case 'UPDATE_RULE_ALERT_NODES': {
      const {ruleID, alertType, alertNodesText} = action.payload

      let alertNodesByType

      switch (alertType) {
        case 'http':
        case 'tcp':
        case 'log':
          alertNodesByType = [
            {
              name: alertType,
              args: [alertNodesText],
              properties: [],
            },
          ]
          break
        case 'exec':
        case 'smtp':
          alertNodesByType = [
            {
              name: alertType,
              args: alertNodesText.split(' '),
              properties: [],
            },
          ]
          break
        case 'slack':
          alertNodesByType = [
            {
              name: alertType,
              properties: [
                {
                  name: 'channel',
                  args: [alertNodesText],
                },
              ],
            },
          ]
          break
        case 'alerta':
          alertNodesByType = [
            {
              name: alertType,
              args: [],
              properties: parseAlerta(alertNodesText),
            },
          ]
          break
        case 'pushover':
        default:
          alertNodesByType = [
            {
              name: alertType,
              args: [],
              properties: [],
            },
          ]
      }

      return Object.assign({}, state, {
        [ruleID]: Object.assign({}, state[ruleID], {
          alertNodes: alertNodesByType,
        }),
      })
    }

    case 'UPDATE_RULE_ALERT_PROPERTY': {
      const {ruleID, alertType, alertProperty} = action.payload

      const newAlertNodes = state[ruleID].alertNodes.map(alertNode => {
        if (alertNode.name !== alertType) {
          return alertNode
        }
        let matched = false

        if (!alertNode.properties) {
          alertNode.properties = []
        }
        alertNode.properties = alertNode.properties.map(property => {
          if (property.name === alertProperty.name) {
            matched = true
            return alertProperty
          }
          return property
        })
        if (!matched) {
          alertNode.properties.push(alertProperty)
        }
        return alertNode
      })

      return {
        ...state,
        [ruleID]: {...state[ruleID]},
        alertNodes: newAlertNodes,
      }
    }

    case 'UPDATE_RULE_NAME': {
      const {ruleID, name} = action.payload
      return Object.assign({}, state, {
        [ruleID]: Object.assign({}, state[ruleID], {
          name,
        }),
      })
    }

    case 'DELETE_RULE_SUCCESS': {
      const {ruleID} = action.payload
      delete state[ruleID]
      return Object.assign({}, state)
    }

    case 'UPDATE_RULE_DETAILS': {
      const {ruleID, details} = action.payload

      return {
        ...state,
        ...{
          [ruleID]: {...state[ruleID], details},
        },
      }
    }

    case 'UPDATE_RULE_STATUS_SUCCESS': {
      const {ruleID, status} = action.payload

      return {
        ...state,
        ...{
          [ruleID]: {...state[ruleID], status},
        },
      }
    }
  }
  return state
}
