import React from 'react'
import {KapacitorPage} from 'src/kapacitor/containers/KapacitorPage'
import KapacitorForm from 'src/kapacitor/components/KapacitorForm'
import {shallow} from 'enzyme'
import {getKapacitor} from 'src/shared/apis'

import {source, kapacitor} from 'test/resources'
import * as dummy from 'mocks/dummy'

jest.mock('src/shared/apis', () => require('mocks/shared/apis'))

const setup = async (override = {}, returnWrapper = true) => {
  const props = {
    source: source,
    addFlashMessage: () => {},
    kapacitor,
    router: {
      push: () => {},
      replace: () => {},
    },
    location: {pathname: '', hash: ''},
    params: {id: '', hash: ''},
    ...override,
  }

  if (!returnWrapper) {
    return {
      props,
    }
  }

  const wrapper = await shallow(<KapacitorPage {...props} />)

  return {
    wrapper,
    props,
  }
}

describe('Kapacitor.Containers.KapacitorPage', () => {
  describe('rendering', () => {
    it('renders the KapacitorPage', async () => {
      const {wrapper} = await setup()
      expect(wrapper.exists()).toBe(true)
    })

    it('renders the <KapacitorForm/>', async () => {
      const {wrapper} = await setup()
      const form = wrapper.find(KapacitorForm)

      expect(form.exists()).toBe(true)
    })
  })

  describe('instance methods', () => {
    describe('componentDidMount', () => {
      describe('if there is no id in the params', () => {
        it('does not get the kapacitor', () => {
          expect(getKapacitor).not.toHaveBeenCalled()
        })
      })

      describe('if there is an id in the params', () => {
        it('gets the kapacitor', async () => {
          const params = {id: '1', hash: ''}
          const {wrapper} = await setup({params})

          expect(getKapacitor).toHaveBeenCalledWith(source, params.id)
          expect(wrapper.state().kapacitor).toEqual(dummy.kapacitor)
        })
      })
    })
  })
})
