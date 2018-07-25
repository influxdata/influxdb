import React from 'react'
import {DataExplorer} from 'src/data_explorer/containers/DataExplorer'
import {shallow} from 'enzyme'
import {source, query, timeRange} from 'test/resources'

const queryConfigActions = {
  chooseNamespace: () => {},
  chooseMeasurement: () => {},
  chooseTag: () => {},
  groupByTag: () => {},
  addQuery: () => {},
  toggleField: () => {},
  groupByTime: () => {},
  toggleTagAcceptance: () => {},
  applyFuncsToField: () => {},
  editRawTextAsync: () => {},
  addInitialField: () => {},
  editQueryStatus: () => {},
  deleteQuery: () => {},
  fill: () => {},
  removeFuncs: () => {},
  editRawText: () => {},
  setTimeRange: () => {},
  updateRawQuery: () => {},
  updateQueryConfig: () => {},
  timeShift: () => {},
}

const setup = () => {
  const props = {
    source,
    queryConfigs: [query],
    queryConfigActions,
    autoRefresh: 1000,
    handleChooseAutoRefresh: () => {},
    timeRange,
    setTimeRange: () => {},
    dataExplorer: {
      queryIDs: [query.id],
    },
    writeLineProtocol: () => {},
    errorThrownAction: () => {},
    onManualRefresh: () => {},
    manualRefresh: 0,
  }

  const wrapper = shallow(<DataExplorer {...props} />)
  return {
    wrapper,
  }
}

describe('DataExplorer.Containers.DataExplorer', () => {
  describe('rendering', () => {
    it('renders without errors', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })
})
