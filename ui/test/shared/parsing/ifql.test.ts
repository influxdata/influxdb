import {parseTables} from 'src/shared/parsing/ifql'
import {FROM_LAST_RESPONSE} from 'test/shared/parsing/constants'

describe('IFQL response parser', () => {
  it('parseTables', () => {
    const result = parseTables(FROM_LAST_RESPONSE)

    expect(result).toHaveLength(47)
  })
})
