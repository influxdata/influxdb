import reducer from 'src/kapacitor/reducers/rules'
import {defaultRuleConfigs} from 'src/kapacitor/constants'

import {
  chooseTrigger,
  addEvery,
  removeEvery,
  updateRuleValues,
  updateDetails,
  updateMessage,
  updateAlertNodes,
  updateAlertProperty,
  updateRuleName,
  deleteRuleSuccess,
  updateRuleStatusSuccess,
} from 'src/kapacitor/actions/view'

describe('Kapacitor.Reducers.rules', () => {
  it('can choose a trigger', () => {
    const ruleID = 1
    const initialState = {
      [ruleID]: {
        id: ruleID,
        queryID: 988,
        trigger: '',
      },
    }

    let newState = reducer(initialState, chooseTrigger(ruleID, 'deadman'))
    expect(newState[ruleID].trigger).to.equal('deadman')
    expect(newState[ruleID].values).to.equal(defaultRuleConfigs.deadman)

    newState = reducer(initialState, chooseTrigger(ruleID, 'relative'))
    expect(newState[ruleID].trigger).to.equal('relative')
    expect(newState[ruleID].values).to.equal(defaultRuleConfigs.relative)

    newState = reducer(initialState, chooseTrigger(ruleID, 'threshold'))
    expect(newState[ruleID].trigger).to.equal('threshold')
    expect(newState[ruleID].values).to.equal(defaultRuleConfigs.threshold)
  })

  it('can update the every', () => {
    const ruleID = 1
    const initialState = {
      [ruleID]: {
        id: ruleID,
        queryID: 988,
        every: null,
      },
    }

    let newState = reducer(initialState, addEvery(ruleID, '30s'))
    expect(newState[ruleID].every).to.equal('30s')

    newState = reducer(newState, removeEvery(ruleID))
    expect(newState[ruleID].every).to.equal(null)
  })

  it('can update the values', () => {
    const ruleID = 1
    const initialState = {
      [ruleID]: {
        id: ruleID,
        queryID: 988,
        trigger: 'deadman',
        values: defaultRuleConfigs.deadman,
      },
    }

    const newDeadmanValues = {duration: '5m'}
    const newState = reducer(
      initialState,
      updateRuleValues(ruleID, 'deadman', newDeadmanValues)
    )
    expect(newState[ruleID].values).to.equal(newDeadmanValues)

    const newRelativeValues = {func: 'max', change: 'change'}
    const finalState = reducer(
      newState,
      updateRuleValues(ruleID, 'relative', newRelativeValues)
    )
    expect(finalState[ruleID].trigger).to.equal('relative')
    expect(finalState[ruleID].values).to.equal(newRelativeValues)
  })

  it('can update the message', () => {
    const ruleID = 1
    const initialState = {
      [ruleID]: {
        id: ruleID,
        queryID: 988,
        message: '',
      },
    }

    const message = 'im a kapacitor rule message'
    const newState = reducer(initialState, updateMessage(ruleID, message))
    expect(newState[ruleID].message).to.equal(message)
  })

  it('can update a slack alert', () => {
    const ruleID = 1
    const initialState = {
      [ruleID]: {
        id: ruleID,
        queryID: 988,
        alertNodes: {},
      },
    }
    const updatedSlack = {
      alias: 'slack-1',
      username: 'testname',
      iconEmoji: 'testemoji',
      enabled: true,
      text: 'slack',
      type: 'slack',
      url: true,
    }
    const expectedSlack = {
      username: 'testname',
      iconEmoji: 'testemoji',
    }
    const newState = reducer(
      initialState,
      updateAlertNodes(ruleID, [updatedSlack])
    )
    expect(newState[ruleID].alertNodes.slack[0]).to.deep.equal(expectedSlack)
  })

  it('can update the name', () => {
    const ruleID = 1
    const name = 'New name'
    const initialState = {
      [ruleID]: {
        id: ruleID,
        queryID: 988,
        name: 'Random album title',
      },
    }

    const newState = reducer(initialState, updateRuleName(ruleID, name))
    expect(newState[ruleID].name).to.equal(name)
  })

  it('it can delete a rule', () => {
    const rule1 = 1
    const rule2 = 2
    const initialState = {
      [rule1]: {
        id: rule1,
      },
      [rule2]: {
        id: rule2,
      },
    }

    expect(Object.keys(initialState).length).to.equal(2)
    const newState = reducer(initialState, deleteRuleSuccess(rule2))
    expect(Object.keys(newState).length).to.equal(1)
    expect(newState[rule1]).to.equal(initialState[rule1])
  })

  it('can update details', () => {
    const ruleID = 1
    const details = 'im some rule details'

    const initialState = {
      [ruleID]: {
        id: ruleID,
        queryID: 988,
        details: '',
      },
    }

    const newState = reducer(initialState, updateDetails(ruleID, details))
    expect(newState[ruleID].details).to.equal(details)
  })

  it('can update properties', () => {
    const ruleID = 1

    const alertNodeName = 'pushover'

    const alertProperty1_Name = 'device'
    const alertProperty1_ArgsOrig =
      'pineapple_kingdom_control_room,bob_cOreos_watch'
    const alertProperty1_ArgsDiff = 'pineapple_kingdom_control_tower'

    const alertProperty2_Name = 'URLTitle'
    const alertProperty2_ArgsOrig = 'Cubeapple Rising'
    const alertProperty2_ArgsDiff = 'Cubeapple Falling'

    const alertProperty1_Orig = {
      name: alertProperty1_Name,
      args: [alertProperty1_ArgsOrig],
    }
    const alertProperty1_Diff = {
      name: alertProperty1_Name,
      args: [alertProperty1_ArgsDiff],
    }
    const alertProperty2_Orig = {
      name: alertProperty2_Name,
      args: [alertProperty2_ArgsOrig],
    }
    const alertProperty2_Diff = {
      name: alertProperty2_Name,
      args: [alertProperty2_ArgsDiff],
    }

    const initialState = {
      [ruleID]: {
        id: ruleID,
        alertNodes: [
          {
            name: 'pushover',
            args: null,
            properties: null,
          },
        ],
      },
    }

    const getAlertPropertyArgs = (matchState, propertyName) =>
      matchState[ruleID].alertNodes
        .find(node => node.name === alertNodeName)
        .properties.find(property => property.name === propertyName).args[0]

    // add first property
    let newState = reducer(
      initialState,
      updateAlertProperty(ruleID, alertNodeName, alertProperty1_Orig)
    )
    expect(getAlertPropertyArgs(newState, alertProperty1_Name)).to.equal(
      alertProperty1_ArgsOrig
    )

    // change first property
    newState = reducer(
      initialState,
      updateAlertProperty(ruleID, alertNodeName, alertProperty1_Diff)
    )
    expect(getAlertPropertyArgs(newState, alertProperty1_Name)).to.equal(
      alertProperty1_ArgsDiff
    )

    // add second property
    newState = reducer(
      initialState,
      updateAlertProperty(ruleID, alertNodeName, alertProperty2_Orig)
    )
    expect(getAlertPropertyArgs(newState, alertProperty1_Name)).to.equal(
      alertProperty1_ArgsDiff
    )
    expect(getAlertPropertyArgs(newState, alertProperty2_Name)).to.equal(
      alertProperty2_ArgsOrig
    )
    expect(
      newState[ruleID].alertNodes.find(node => node.name === alertNodeName)
        .properties.length
    ).to.equal(2)

    // change second property
    newState = reducer(
      initialState,
      updateAlertProperty(ruleID, alertNodeName, alertProperty2_Diff)
    )
    expect(getAlertPropertyArgs(newState, alertProperty1_Name)).to.equal(
      alertProperty1_ArgsDiff
    )
    expect(getAlertPropertyArgs(newState, alertProperty2_Name)).to.equal(
      alertProperty2_ArgsDiff
    )
    expect(
      newState[ruleID].alertNodes.find(node => node.name === alertNodeName)
        .properties.length
    ).to.equal(2)
  })

  it('can update status', () => {
    const ruleID = 1
    const status = 'enabled'

    const initialState = {
      [ruleID]: {
        id: ruleID,
        queryID: 988,
        status: 'disabled',
      },
    }

    const newState = reducer(
      initialState,
      updateRuleStatusSuccess(ruleID, status)
    )
    expect(newState[ruleID].status).to.equal(status)
  })
})
