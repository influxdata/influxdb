import {
  getLocalStateRanges,
  setLocalStateRanges,
} from 'src/normalizers/localStorage/dashboardTime'

const getFunction: (a: any) => {} = getLocalStateRanges // This is to be able to test that these functions correctly filter badly formed inputs
const setFunction: (a: any) => {} = setLocalStateRanges

const dashboardID = '1'
const lowerDuration = 'now()-15m'
const arrayFormatRange = [{dashboardID, lower: lowerDuration, upper: null}]
const objFormatRange = {
  [dashboardID]: {
    lower: lowerDuration,
    upper: null,
    type: 'duration' as 'duration',
  },
}

const badArrayFormats = [
  {lower: lowerDuration, upper: null}, // no dashID
  {dashboardID: '2', upper: null}, // no lower
  {dashboardID: '3', lower: lowerDuration}, // no upper
  {dashboardID: '4', lower: 3}, // lower is not string or null
  {dashboardID: '5', lower: null, upper: null}, // lower is not string or null
]

const badObjFormats = {
  '2': {
    // no lower
    upper: null,
    type: 'custom' as 'custom',
  },
  ['3']: {
    // no upper
    lower: lowerDuration,
    type: 'custom' as 'custom',
  },
  ['5']: {
    // upper is not string or null
    upper: 5,
    lower: lowerDuration,
    type: 'custom' as 'custom',
  },
}

describe('Time Range interactions with LocalStorage', () => {
  describe('can read timeRanges from localState', () => {
    describe('can read timeRanges from localstate if they are in an array format', () => {
      it('can read a timeRange and assign it to dashboard ID', () => {
        expect(getLocalStateRanges(arrayFormatRange)).toEqual(objFormatRange)
      })

      it('rejects timeRanges that are not well formed', () => {
        expect(getFunction([...arrayFormatRange, ...badArrayFormats])).toEqual(
          objFormatRange
        )
      })
    })
    describe('can read timeRanges from localState if they are in object format', () => {
      it('returns the object if timeRange well formed', () => {
        expect(getFunction(objFormatRange)).toEqual(
          getLocalStateRanges(objFormatRange)
        )
      })

      it('rejects timeRanges that are not well formed', () => {
        expect(getFunction({...objFormatRange, ...badObjFormats})).toEqual(
          getLocalStateRanges(objFormatRange)
        )
      })
    })
  })
  describe('can write timeRanges to localState', () => {
    it('returns the object if timeRange well formed', () => {
      expect(setFunction(objFormatRange)).toEqual(
        getLocalStateRanges(objFormatRange)
      )
    })

    it('rejects timeRanges that are not well formed', () => {
      expect(setFunction({...objFormatRange, ...badObjFormats})).toEqual(
        getLocalStateRanges(objFormatRange)
      )
    })
  })
})
