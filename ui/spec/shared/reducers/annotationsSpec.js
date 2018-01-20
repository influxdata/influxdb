import reducer from 'shared/reducers/annotations'

import {
  addAnnotation,
  deleteAnnotation,
  loadAnnotations,
  updateAnnotation,
} from 'shared/actions/annotations'

import {DEFAULT_ANNOTATION_ID} from 'src/shared/constants/annotations'

const a1 = {
  id: '1',
  group: '',
  name: 'anno1',
  time: '1515716169000',
  duration: '',
  text: 'you have no swoggels',
}

const a2 = {
  id: '2',
  group: '',
  name: 'anno1',
  time: '1515716169000',
  duration: '',
  text: 'you have no swoggels',
}

const state = {
  mode: null,
  annotations: [],
}

describe.only('Shared.Reducers.annotations', () => {
  it('can load the annotations', () => {
    const expected = [{time: '0', duration: ''}]
    const actual = reducer(state, loadAnnotations(expected))

    expect(actual.annotations).to.deep.equal(expected)
  })

  it('can update an annotation', () => {
    const expected = [{...a1, time: ''}]
    const actual = reducer(
      {...state, annotations: [a1]},
      updateAnnotation(expected[0])
    )

    expect(actual.annotations).to.deep.equal(expected)
  })

  it('can delete an annotation', () => {
    const expected = [a2]
    const actual = reducer(
      {...state, annotations: [a1, a2]},
      deleteAnnotation(a1)
    )

    expect(actual.annotations).to.deep.equal(expected)
  })

  it('can add an annotation', () => {
    const expected = [{...a1, id: DEFAULT_ANNOTATION_ID}]
    const actual = reducer(state, addAnnotation(a1))

    expect(actual.annotations).to.deep.equal(expected)
  })
})
