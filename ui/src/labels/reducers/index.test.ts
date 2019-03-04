// Reducer
import {labelsReducer} from 'src/labels/reducers'

// Actions
import {setLabels} from 'src/labels/actions'
import {RemoteDataState} from 'src/types'

describe('labels reducer', () => {
  it('can set the labels', () => {
    const status = RemoteDataState.Done
    const properties = {color: '#4286f4', description: 'the best of labels'}
    const list = [{id: '1', properties}]

    const expected = {status, list}
    const actual = labelsReducer(undefined, setLabels(status, list))

    expect(actual).toEqual(expected)
  })
})
