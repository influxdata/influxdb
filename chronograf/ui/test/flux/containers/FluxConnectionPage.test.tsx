import React from 'react'
import {shallow} from 'enzyme'

import {FluxConnectionPage} from 'src/flux/containers/FluxConnectionPage'
import FluxNew from 'src/flux/components/FluxNew'
import FluxEdit from 'src/flux/components/FluxEdit'

import {source, service} from 'test/fixtures'

jest.mock('src/shared/apis', () => require('mocks/shared/apis'))

const setup = (override = {}) => {
  const props = {
    source,
    services: [],
    params: {id: '', sourceID: ''},
    router: {push: () => {}},
    notify: () => {},
    createService: jest.fn(),
    updateService: jest.fn(),
    setActiveFlux: jest.fn(),
    fetchServicesForSource: jest.fn(),
    ...override,
  }

  const wrapper = shallow(<FluxConnectionPage {...props} />)

  return {
    props,
    wrapper,
  }
}

describe('Flux.Containers.FluxConnectionPage', () => {
  describe('when no ID is present on params', () => {
    it('renders the FluxNew component', () => {
      const {wrapper} = setup()
      const fluxNew = wrapper.find(FluxNew)
      const fluxEdit = wrapper.find(FluxEdit)

      expect(fluxNew.length).toBe(1)
      expect(fluxEdit.length).toBe(0)
    })
  })

  describe('when ID is present on params', () => {
    it('renders a FluxEdit component', () => {
      const services = [service]
      const id = service.id
      const params = {id, sourceID: service.sourceID}
      const {wrapper} = setup({services, id, params})

      const fluxNew = wrapper.find(FluxNew)
      const fluxEdit = wrapper.find(FluxEdit)

      expect(fluxNew.length).toBe(0)
      expect(fluxEdit.length).toBe(1)
    })
  })
})
