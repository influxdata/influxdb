import reducer from 'src/shared/reducers/annotations'
import {AnnotationInterface} from 'src/types'
import {AnnotationState} from 'src/shared/reducers/annotations'

import {
  addAnnotation,
  deleteAnnotation,
  loadAnnotations,
  updateAnnotation,
} from 'src/shared/actions/annotations'

const a1: AnnotationInterface = {
  id: '1',
  startTime: 1515716169000,
  endTime: 1515716169000,
  type: '',
  text: 'you have no swoggels',
  links: {self: 'to/thine/own/self/be/true'},
}

const a2: AnnotationInterface = {
  id: '2',
  startTime: 1515716169000,
  endTime: 1515716169002,
  type: '',
  text: 'you have so many swoggels',
  links: {self: 'self/in/eye/of/beholder'},
}

const state: AnnotationState = {
  isTempHovering: false,
  mode: null,
  annotations: [],
}

describe('Shared.Reducers.annotations', () => {
  it('can load the annotations', () => {
    const expected = [a1]
    const actual = reducer(state, loadAnnotations(expected))

    expect(actual.annotations).toEqual(expected)
  })
  it('can update an annotation', () => {
    const expected = [{...a1, startTime: 6666666666666}]
    const actual = reducer(
      {...state, annotations: [a1]},
      updateAnnotation(expected[0])
    )

    expect(actual.annotations).toEqual(expected)
  })

  it('can delete an annotation', () => {
    const expected = [a2]
    const actual = reducer(
      {...state, annotations: [a1, a2]},
      deleteAnnotation(a1)
    )

    expect(actual.annotations).toEqual(expected)
  })

  it('can add an annotation', () => {
    const expected = [a1]
    const actual = reducer(state, addAnnotation(a1))

    expect(actual.annotations).toEqual(expected)
  })
})
