import reducer, {initialState} from 'src/shared/reducers/services'

import {
  addService,
  loadServices,
  deleteService,
  updateService,
} from 'src/shared/actions/services'

import {Service} from 'src/types'
import {service} from 'test/resources'

const services = (): Service[] => {
  return reducer(initialState, addService(service))
}

describe('Shared.Reducers.services', () => {
  describe('LOAD_SERVICES', () => {
    it('correctly loads services', () => {
      const s1 = {...service, id: '1'}
      const s2 = {...service, id: '2'}

      const expected = [s1, s2]
      const actual = reducer(initialState, loadServices(expected))

      expect(actual).toEqual(expected)
    })
  })

  describe('ADD_SERVICE', () => {
    it('can add a service', () => {
      const expected = service
      const [actual] = reducer(initialState, addService(service))

      expect(actual).toEqual(expected)
    })
  })

  describe('DELETE_SERVICE', () => {
    it('can delete a service', () => {
      const state = services()
      const actual = reducer(state, deleteService(service))

      expect(actual).toEqual([])
    })
  })

  describe('UPDATE_SERVICE', () => {
    it('can update a service', () => {
      const name = 'updated name'
      const type = 'updated type'
      const expected = {...service, name, type}

      const state = services()
      const [actual] = reducer(state, updateService(expected))

      expect(actual).toEqual(expected)
    })
  })
})
