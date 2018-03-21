import {
  createKapacitorBody,
  kapacitor,
  source,
  updateKapacitorBody,
} from 'mocks/dummy'
import {createKapacitor, updateKapacitor} from 'src/shared/apis'
import AJAX from 'src/utils/ajax'

jest.mock('src/utils/ajax', () => require('mocks/utils/ajax'))

describe('Shared.Apis', () => {
  afterEach(() => {
    jest.clearAllMocks()
  })

  describe('createKapacitor', () => {
    it('is called with the expected body', () => {
      createKapacitor(source, createKapacitorBody)

      expect(AJAX).toHaveBeenCalledWith({
        url: source.links.kapacitors,
        method: 'POST',
        data: createKapacitorBody,
      })
    })
  })

  describe('updateKapacitor', () => {
    it('is called with the expected body', () => {
      updateKapacitor(updateKapacitorBody)
      const data = {...updateKapacitorBody}
      delete data.links

      expect(AJAX).toHaveBeenCalledWith({
        url: kapacitor.links.self,
        method: 'PATCH',
        data,
      })
    })
  })
})
