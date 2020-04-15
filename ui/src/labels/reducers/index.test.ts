// Libraries
import {normalize} from 'normalizr'

// Schemas
import {labelSchema, arrayOfLabels} from 'src/schemas/labels'

// Reducer
import {labelsReducer, initialState} from 'src/labels/reducers'

// Actions
import {setLabels, setLabel, removeLabel} from 'src/labels/actions/creators'

// Types
import {RemoteDataState, Label, LabelEntities} from 'src/types'

// Mock Label
const status = RemoteDataState.Done
const properties = {color: '#4286f4', description: 'the best of labels'}
const dummyLabel = {id: '1', properties}

describe('labels reducer', () => {
  it('can set the labels', () => {
    const labels = normalize<Label, LabelEntities, string[]>(
      [dummyLabel],
      arrayOfLabels
    )

    const actual = labelsReducer(initialState(), setLabels(status, labels))

    expect(actual.allIDs).toEqual([dummyLabel.id])
    expect(actual.byID[dummyLabel.id]).toEqual({...dummyLabel, status})
  })

  it('can set a label', () => {
    const state = initialState()
    const newLabel = {id: '2', properties}

    const label = normalize<Label, LabelEntities, string>(newLabel, labelSchema)

    const actual = labelsReducer(state, setLabel(newLabel.id, status, label))

    expect(actual.byID[newLabel.id]).toEqual({
      ...newLabel,
      status,
    })
    expect(actual.allIDs).toEqual([newLabel.id])
  })

  it('can remove a label', () => {
    const {id} = dummyLabel
    const state = {
      byID: {
        [id]: {...dummyLabel, status},
      },
      allIDs: [id],
      status: RemoteDataState.Done,
    }

    const expected = {status, byID: {}, allIDs: []}
    const actual = labelsReducer(state, removeLabel(dummyLabel.id))

    expect(actual).toEqual(expected)
  })
})
