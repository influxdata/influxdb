import {getSuggestions} from 'src/ifql/apis'
import AJAX from 'src/utils/ajax'

jest.mock('src/utils/ajax', () => require('mocks/utils/ajax'))

describe('IFQL.Apis', () => {
  afterEach(() => {
    jest.clearAllMocks()
  })

  describe('getSuggestions', () => {
    it('is called with the expected body', () => {
      const url = '/chronograf/v1/suggestions'
      getSuggestions(url)
      expect(AJAX).toHaveBeenCalledWith({
        url,
      })
    })
  })
})
