import React from 'react'
import {shallow} from 'enzyme'
import {TickscriptPage} from 'src/kapacitor/containers/TickscriptPage'
import TickscriptHeader from 'src/kapacitor/components/TickscriptHeader'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'
import TickscriptSave from 'src/kapacitor/components/TickscriptSave'
import {source, kapacitorRules} from 'test/resources'

jest.mock('src/shared/apis', () => require('mocks/shared/apis'))
jest.mock('src/kapacitor/apis', () => require('mocks/kapacitor/apis'))

const kapacitorActions = {
  updateTask: () => {},
  createTask: () => {},
  getRule: () => {},
}

const setup = (override?) => {
  const props = {
    source,
    errorActions: {
      errorThrown: () => {},
    },
    kapacitorActions,
    router: {
      push: () => {},
    },
    params: {
      ruleID: kapacitorRules[0].id,
    },
    rules: kapacitorRules,
    notify: () => {},
    ...override,
  }

  const wrapper = shallow(<TickscriptPage {...props} />)

  return {
    wrapper,
  }
}

describe('Kapacitor.Containers.TickscriptPage', () => {
  afterEach(() => {
    jest.clearAllMocks()
  })

  describe('rendering', () => {
    it('renders without errors', () => {
      const {wrapper} = setup()
      expect(wrapper.exists()).toBe(true)
    })
  })

  describe('user interractions', () => {
    describe('saving a new Tickscript', () => {
      it('routes the user Tickscript edit page if save succeeds', done => {
        const id = 'newly-create-tickscript'
        const push = jest.fn()
        const createTask = jest.fn(() =>
          Promise.resolve({code: 200, message: 'nice tickscript', id})
        )

        const actions = {
          ...kapacitorActions,
          createTask,
        }

        const {wrapper} = setup({
          kapacitorActions: actions,
          router: {push},
          params: {ruleID: 'new'},
        })

        const header = wrapper.dive().find(TickscriptHeader)
        const pageHeader = header.dive().find(PageHeader)
        const save = pageHeader.dive().find(TickscriptSave)

        save.dive().simulate('click')

        process.nextTick(() => {
          expect(push).toHaveBeenCalledTimes(1)
          expect(push.mock.calls[0][0]).toMatch(id)
          done()
        })
      })

      it('keeps the user on /new if save fails', done => {
        const push = jest.fn()
        const createTask = jest.fn(() =>
          Promise.resolve({code: 422, message: 'invalid tickscript'})
        )

        const actions = {
          ...kapacitorActions,
          createTask,
        }

        const {wrapper} = setup({
          kapacitorActions: actions,
          router: {push},
          params: {ruleID: 'new'},
        })

        const header = wrapper.dive().find(TickscriptHeader)
        const pageHeader = header.dive().find(PageHeader)
        const save = pageHeader.dive().find(TickscriptSave)

        save.dive().simulate('click')

        process.nextTick(() => {
          expect(push).not.toHaveBeenCalled()
          done()
        })
      })
    })
  })
})
