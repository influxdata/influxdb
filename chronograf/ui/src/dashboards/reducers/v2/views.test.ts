import reducer from 'src/dashboards/reducers/v2/views'

import {setActiveCell} from 'src/dashboards/actions/v2/views'

describe('Dashboards.Reducers.Ranges', () => {
  it('can set the active cell', () => {
    const state = 'cell-one'
    const expected = 'cell-tw0'

    const actual = reducer(state, setActiveCell(expected))
    expect(actual).toEqual(expected)
  })
})
