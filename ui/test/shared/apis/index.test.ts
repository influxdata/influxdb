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
        data: createKapacitorBody,
        method: 'POST',
        url: source.links.kapacitors,
      })
    })
  })

  describe('updateKapacitor', () => {
    it('is called with the expected body', () => {
      updateKapacitor(updateKapacitorBody)
      const data = {...updateKapacitorBody}
      delete data.links

      expect(AJAX).toHaveBeenCalledWith({
        data,
        method: 'PATCH',
        url: kapacitor.links.self,
      })
    })
  })
})
