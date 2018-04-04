import React from 'react'
import QueryMaker from 'src/data_explorer/components/QueryMaker'
import {shallow} from 'enzyme'
import {source, query, timeRange} from 'test/resources'

const setup = () => {
  const props = {
    source,
    timeRange,
    actions: {
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
      fill: () => {},
      removeFuncs: () => {},
    },
    activeQuery: query,
    initialGroupByTime: '',
  }

  const wrapper = shallow(<QueryMaker {...props} />)

  return {
    wrapper,
  }
}

describe('DataExplorer.Components.QueryMaker', () => {
  describe('rendering', () => {
    it('renders', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })
})
