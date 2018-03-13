import {createKapacitor} from 'src/shared/apis'
import {source, createKapacitorBody} from 'mocks/dummy'
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
})
