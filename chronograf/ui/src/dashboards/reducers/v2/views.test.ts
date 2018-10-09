import viewsReducer from 'src/dashboards/reducers/v2/views'

import {setActiveCell} from 'src/dashboards/actions/v2/views'

describe('viewsReducer', () => {
  it('can set the active cell', () => {
    const state = {
      activeViewID: 'cell-one',
      views: {},
    }

    const actual = viewsReducer(state, setActiveCell('cell-two')).activeViewID
    const expected = 'cell-two'

    expect(actual).toEqual(expected)
  })
})
