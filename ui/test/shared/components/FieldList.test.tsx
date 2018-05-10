import {mount} from 'enzyme'
import React from 'react'
import FieldList from 'src/shared/components/FieldList'
import FieldListItem from 'src/data_explorer/components/FieldListItem'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

import {query, source} from 'test/resources'

jest.mock('src/shared/apis/metaQuery', () =>
  require('mocks/shared/apis/metaQuery')
)

const setup = (override = {}) => {
  const props = {
    query,
    onTimeShift: () => {},
    onToggleField: () => {},
    onGroupByTime: () => {},
    onFill: () => {},
    applyFuncsToField: () => {},
    isKapacitorRule: false,
    // querySource,
    removeFuncs: () => {},
    addInitialField: () => {},
    initialGroupByTime: '5m',
    isQuerySupportedByExplorer: true,
    source,
    ...override,
  }

  const wrapper = mount(<FieldList {...props} />, {
    context: {source},
  })

  // const instance = wrapper.instance() as FieldList

  return {
    // instance,
    props,
    wrapper,
  }
}

describe('Shared.Components.FieldList', () => {
  describe('rendering', () => {
    it('renders to the page', () => {
      const {wrapper} = setup()

      expect(wrapper.exists()).toBe(true)
    })

    describe('<FieldListItem/>', () => {
      it('renders <FieldListItem/>`s to the page', () => {
        const {wrapper} = setup()
        const items = wrapper.find(FieldListItem)
        // const first = items.first()
        // const last = items.last()
        console.debug('jimmy', wrapper.debug())
        expect(items.length).toBe(9)
        // expect(first.dive().text()).toContain('foo')
        // expect(last.dive().text()).toContain('bar')
      })
    })
  })
})
