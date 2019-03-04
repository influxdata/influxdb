// Reducer
import {labelsReducer} from 'src/labels/reducers'

// Actions
import {setLabels, addLabel} from 'src/labels/actions'
import {RemoteDataState} from 'src/types'

// Mock Label
const status = RemoteDataState.Done
const properties = {color: '#4286f4', description: 'the best of labels'}
const dummyLabel = {id: '1', properties}

describe('labels reducer', () => {
  it('can set the labels', () => {
    const list = [dummyLabel]

    const expected = {status, list}
    const actual = labelsReducer(undefined, setLabels(status, list))

    expect(actual).toEqual(expected)
  })

  it('can add a label', () => {
    const list = [dummyLabel]
    const state = {status, list}
    const newLabel = {id: '2', properties}

    const expected = {status, list: [...list, newLabel]}
    const actual = labelsReducer(state, addLabel(newLabel))

    expect(actual).toEqual(expected)
  })
})
