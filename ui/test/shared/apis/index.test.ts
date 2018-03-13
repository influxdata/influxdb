import {createKapacitor, updateKapacitor} from 'src/shared/apis'
import {source, kapacitor, createKapacitorBody} from 'mocks/dummy'
import AJAX from 'src/utils/ajax'

jest.mock('src/utils/ajax', () => require('mocks/utils/ajax'))

describe('Shared.Apis', () => {
  describe('createKapacitor', () => {
    it('is called with the expected body', () => {
      createKapacitor(source, createKapacitorBody)

      expect(AJAX).toHaveBeenCalledWith({
        url: source.links.kapacitors,
        method: 'POST',
        data: createKapacitorBody
      })
    })
  })

  describe('updateKapacitor', () => {
    it('is called with the expected body', () => {
      const username = 'user'
      const password = 'password'
      const data = {username, password, ...kapacitor}

      updateKapacitor(data)

      expect(AJAX).toHaveBeenCalledWith({
        url: kapacitor.links.self,
        method: 'PATCH',
        data,
      })
    })
  })
})
