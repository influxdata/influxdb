import alertBuilderReducer, {
  AlertBuilderState,
  initialState,
} from 'src/alerting/reducers/alertBuilder'
import {
  resetAlertBuilder,
  setAlertBuilderCheck,
  setAlertBuilderCheckStatus,
  setEvery,
  setStatusMessageTemplate,
  setOffset,
  editTagSetByIndex,
  removeTagSet,
  updateThreshold,
  removeThreshold,
  setStaleTime,
  setTimeSince,
  setLevel,
  updateThresholds,
} from 'src/alerting/actions/alertBuilder'
import {CHECK_FIXTURE_1, CHECK_FIXTURE_3} from 'src/checks/reducers/checks.test'
import {
  RemoteDataState,
  Threshold,
  TaskStatusType,
  ThresholdCheck,
  DeadmanCheck,
} from 'src/types'

const check_1 = {
  ...(CHECK_FIXTURE_1 as ThresholdCheck),
  activeStatus: 'inactive' as TaskStatusType,
  status: RemoteDataState.Done,
}

const check_3 = {
  ...(CHECK_FIXTURE_3 as DeadmanCheck),
  activeStatus: 'active' as TaskStatusType,
  status: RemoteDataState.Done,
}

const mockState = (): AlertBuilderState => ({
  id: '3',
  type: 'deadman',
  activeStatus: 'active',
  name: 'just',
  every: '1m',
  offset: '2m',
  tags: [],
  statusMessageTemplate: 'laksdjflj',
  timeSince: '10000s',
  reportZero: true,
  staleTime: '2m',
  level: 'OK',
  thresholds: [],
  status: RemoteDataState.Done,
})

describe('alertBuilderReducer', () => {
  describe('resetAlertBuilder', () => {
    it('resets Alert Builder State to defaults', () => {
      const actual = alertBuilderReducer(mockState(), resetAlertBuilder())

      const expected = initialState()

      expect(actual).toEqual(expected)
    })
  })
  describe('loadCheck', () => {
    it('Loads threshold check properties in to alert builder', () => {
      const actual = alertBuilderReducer(
        initialState(),
        setAlertBuilderCheck(check_1)
      )

      const expected = check_1

      expect(actual.type).toEqual(expected.type)
      expect(actual.name).toEqual(expected.name)
      expect(actual.every).toEqual(expected.every)
      expect(actual.offset).toEqual(expected.offset)
      expect(actual.statusMessageTemplate).toEqual(
        expected.statusMessageTemplate
      )
      expect(actual.tags).toEqual(expected.tags)
      expect(actual.thresholds).toEqual(expected.thresholds)
      expect(actual.status).toEqual(RemoteDataState.Done)
    })

    it('Loads deadman check properties in to alert builder', () => {
      const actual = alertBuilderReducer(
        initialState(),
        setAlertBuilderCheck(check_3)
      )

      const expected = check_3

      expect(actual.type).toEqual(expected.type)
      expect(actual.name).toEqual(expected.name)
      expect(actual.every).toEqual(expected.every)
      expect(actual.offset).toEqual(expected.offset)
      expect(actual.statusMessageTemplate).toEqual(
        expected.statusMessageTemplate
      )
      expect(actual.tags).toEqual(expected.tags)

      expect(actual.timeSince).toEqual(expected.timeSince)
      expect(actual.staleTime).toEqual(expected.staleTime)
      expect(actual.reportZero).toEqual(expected.reportZero)
      expect(actual.level).toEqual(expected.level)
      expect(actual.status).toEqual(RemoteDataState.Done)
    })
  })

  describe('setAlertBuilderCheckStatus', () => {
    it('check status is initialized to Not Started', () => {
      expect(initialState().status).toEqual(RemoteDataState.NotStarted)
    })
    it('sets check status', () => {
      const newStatus = RemoteDataState.Error
      const actual = alertBuilderReducer(
        initialState(),
        setAlertBuilderCheckStatus(newStatus)
      )
      expect(actual.status).toEqual(newStatus)
    })
  })

  describe('setEvery', () => {
    it('sets Every', () => {
      const newEvery = '20000m'
      const actual = alertBuilderReducer(initialState(), setEvery(newEvery))
      expect(actual.every).toEqual(newEvery)
    })
  })

  describe('setOffset', () => {
    it('sets Offset', () => {
      const newOffset = '20000m'
      const actual = alertBuilderReducer(initialState(), setOffset(newOffset))
      expect(actual.offset).toEqual(newOffset)
    })
  })

  describe('setStaleTime', () => {
    it('sets Stale Time', () => {
      const newStaleTime = '20000m'
      const actual = alertBuilderReducer(
        initialState(),
        setStaleTime(newStaleTime)
      )

      expect(actual.staleTime).toEqual(newStaleTime)
    })
  })

  describe('setTimeSince', () => {
    it('sets Time Sinces', () => {
      const newTimeSince = '20000m'
      const actual = alertBuilderReducer(
        initialState(),
        setTimeSince(newTimeSince)
      )
      expect(actual.timeSince).toEqual(newTimeSince)
    })
  })

  describe('setLevel', () => {
    it('sets Level', () => {
      const newLevel = 'INFO' as 'INFO'
      const actual = alertBuilderReducer(initialState(), setLevel(newLevel))
      expect(actual.level).toEqual(newLevel)
    })
  })

  describe('setStatusMessageTemplate', () => {
    it('sets statusMessageTemplate', () => {
      const newMessage = 'Good news!!'
      const actual = alertBuilderReducer(
        initialState(),
        setStatusMessageTemplate(newMessage)
      )
      expect(actual.statusMessageTemplate).toEqual(newMessage)
    })
  })

  describe('editTagSetByIndex', () => {
    it('edits Tag Set by index', () => {
      const tagSet1 = {key: 'key1', value: 'value1'}
      const tagSet2 = {key: 'key2', value: 'value2'}
      const tagSet3 = {key: 'key3', value: 'value3'}

      const actual = alertBuilderReducer(
        {...initialState(), tags: [tagSet1, tagSet2]},
        editTagSetByIndex(1, tagSet3)
      )
      expect(actual.tags).toEqual([tagSet1, tagSet3])
    })
  })

  describe('removeTagSet', () => {
    it('removes indexed tag set', () => {
      const newTagSet1 = {key: 'key1', value: 'value1'}
      const newTagSet2 = {key: 'key2', value: 'value2'}

      const tags = [newTagSet1, newTagSet2]

      const actual = alertBuilderReducer(
        {...initialState(), tags},
        removeTagSet(0)
      )
      expect(actual.tags).toEqual([newTagSet2])
    })
  })

  describe('updateThreshold ', () => {
    it('updates Threshold', () => {
      const existingThreshold: Threshold = {
        level: 'INFO',
        type: 'greater',
        value: 10,
      }
      const newThreshold: Threshold = {
        level: 'INFO',
        type: 'greater',
        value: 50,
      }

      const actual = alertBuilderReducer(
        {...initialState(), thresholds: [existingThreshold]},
        updateThreshold(newThreshold)
      )
      expect(actual.thresholds).toEqual([newThreshold])
    })
  })

  describe('updateThresholds ', () => {
    it('updates Thresholds', () => {
      const existingThreshold: Threshold = {
        level: 'INFO',
        type: 'greater',
        value: 10,
      }
      const newThreshold: Threshold = {
        level: 'CRIT',
        type: 'greater',
        value: 50,
      }

      const actual = alertBuilderReducer(
        {...initialState(), thresholds: [existingThreshold]},
        updateThresholds([newThreshold])
      )
      expect(actual.thresholds).toEqual([newThreshold])
    })
  })

  describe('removeThreshold ', () => {
    it('removes Threshold by level', () => {
      const infoThresh: Threshold = {
        level: 'INFO',
        type: 'greater',
        value: 10,
      }
      const critThresh: Threshold = {
        level: 'CRIT',
        type: 'greater',
        value: 10,
      }
      const existingThresholds = [infoThresh, critThresh]

      const actual = alertBuilderReducer(
        {...initialState(), thresholds: existingThresholds},
        removeThreshold('INFO')
      )
      expect(actual.thresholds).toEqual([critThresh])
    })
  })
})
