import {getAnnotations} from 'shared/annotations/helpers'
import Dygraph from 'src/external/dygraph'

const start = 1515628800000
const end = 1516060800000
const timeSeries = [
  [start, 25],
  [1515715200000, 13],
  [1515801600000, 10],
  [1515888000000, 5],
  [1515974400000, null],
  [end, 14],
]

const labels = ['time', 'test.label']

const div = document.createElement('div')
const graph = new Dygraph(div, timeSeries, {labels})

const oneHourMs = '3600000'

const a1 = {
  group: '',
  name: 'a1',
  time: '1515716160000',
  duration: '',
  text: 'you have no swoggels',
}

const a2 = {
  group: '',
  name: 'a2',
  time: '1515716169000',
  duration: '3600000', // 1 hour
  text: 'you have no swoggels',
}

const annotations = [a1]

describe('Shared.Annotations.Helpers', () => {
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

    it('removes an annotation if it is out of the time range', () => {
      const outOfBounds = {
        group: '',
        name: 'not in time range',
        time: '2515716169000',
        duration: '',
      }

      const newAnnos = [...annotations, outOfBounds]
      const actual = getAnnotations(graph, newAnnos)
      const expected = annotations

      expect(actual).to.deep.equal(expected)
    })

    describe('with a duration', () => {
      it('it adds an annotation', () => {
        const withDurations = [...annotations, a2]
        const actual = getAnnotations(graph, withDurations)
        const expectedAnnotation = {
          ...a2,
          time: `${Number(a2.time) + Number(a2.duration)}`,
          duration: '',
        }

        const expected = [...withDurations, expectedAnnotation]
        expect(actual).to.deep.equal(expected)
      })

      it('does not add a duration annotation if it is out of bounds', () => {
        const annotationWithOutOfBoundsDuration = {
          ...a2,
          duration: a2.time,
        }

        const withDurations = [
          ...annotations,
          annotationWithOutOfBoundsDuration,
        ]

        const actual = getAnnotations(graph, withDurations)
        const expected = withDurations

        expect(actual).to.deep.equal(expected)
      })
    })
  })
})
