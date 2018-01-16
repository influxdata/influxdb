import {getAnnotations} from 'shared/annotations/helpers'
import Dygraph from 'src/external/dygraph'
const timeSeries = [
  [1515628800000, 25],
  [1515715200000, 13],
  [1515801600000, 10],
  [1515888000000, 5],
  [1515974400000, null],
  [1516060800000, 14],
]

const labels = ['time', 'test.label']

const div = document.createElement('div')
const graph = new Dygraph(div, timeSeries, {labels})

const a1 = {
  group: '',
  name: 'anno1',
  time: '1515716169000',
  duration: '', // 1 hour
  text: 'you have no swoggels',
}

const a2 = {
  group: '',
  name: 'anno1',
  time: '1515716169000',
  duration: '3600000', // 1 hour
  text: 'you have no swoggels',
}

const annotations = [a1]

describe.only('Shared.Annotations.Helpers', () => {
  describe('getAnnotations', () => {
    it('returns an empty array with no graph or annotations are provided', () => {
      const actual = getAnnotations(undefined, annotations)
      const expected = []

      expect(actual).to.deep.equal(expected)
    })

    it('returns an annotation if it is in the time range', () => {
      const actual = getAnnotations(graph, annotations)
      const expected = annotations

      expect(actual).to.deep.equal(expected)
    })
  })
})
