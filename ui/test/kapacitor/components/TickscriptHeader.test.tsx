import React from 'react'
import {shallow} from 'enzyme'

import TickscriptHeader from 'src/kapacitor/components/TickscriptHeader'
import TickscriptSave from 'src/kapacitor/components/TickscriptSave'

const setup = (override?) => {
  const props = {
    isNewTickscript: false,
    onToggleLogsVisibility: () => {},
    onSave: () => {},
    onExit: () => {},
    areLogsVisible: false,
    areLogsEnabled: false,
    task: {
      id: '1',
      dbrps: [],
    },
    unsavedChanges: false,
    ...override,
  }

  const wrapper = shallow(<TickscriptHeader {...props} />)

  return {
    wrapper,
  }
}

describe('Kapacitor.Components.TickscriptHeader', () => {
  describe('rendering', () => {
    it('renders without error', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })

  describe('user interreaction', () => {
    describe('saving an existing tickscript', () => {
      it('is disabled if there are no changes', () => {
        const {wrapper} = setup({unsavedChanges: false})

        const save = wrapper.find(TickscriptSave).dive()
        const saveButton = save.find('button')

        expect(saveButton.props().disabled).toBe(true)
      })

      it('is disabled if there are no dbrps', () => {
        const {wrapper} = setup({unsavedChanges: true})

        const save = wrapper.find(TickscriptSave).dive()
        const saveButton = save.find('button')

        expect(saveButton.props().disabled).toBe(true)
      })
    })

    describe('saving a new tickscript', () => {
      describe('when there are no dbrps', () => {
        it('disables saving', () => {
          const task = {id: '1', dbrps: []}
          const {wrapper} = setup({isNewTickscript: true, task})

          const save = wrapper.find(TickscriptSave).dive()
          const saveButton = save.find('button')

          expect(saveButton.props().disabled).toBe(true)
        })
      })

      describe('when there is not an id', () => {
        it('disables saving', () => {
          const task = {id: '', dbrps: [{db: 'db1', rp: 'rp1'}]}
          const {wrapper} = setup({isNewTickscript: true, task})

          const save = wrapper.find(TickscriptSave).dive()
          const saveButton = save.find('button')

          expect(saveButton.props().disabled).toBe(true)
        })
      })
    })
  })
})
