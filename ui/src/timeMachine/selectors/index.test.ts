// Funcs
import {isFlagEnabled} from 'src/shared/utils/featureFlag'
import {mocked} from 'ts-jest/utils'
jest.mock('src/shared/utils/featureFlag')

import {
  getActiveTagValues,
  getStartTime,
  getEndTime,
} from 'src/timeMachine/selectors/index'
import moment from 'moment'

import {
  pastThirtyDaysTimeRange,
  pastHourTimeRange,
  pastFifteenMinTimeRange,
} from 'src/shared/constants/timeRanges'

const custom = 'custom' as 'custom'

describe('TimeMachine.Selectors.Index', () => {
  const thirty = moment()
    .subtract(30, 'days')
    .valueOf()
  it(`getStartTime should return ${thirty} when lower is now() - 30d`, () => {
    expect(getStartTime(pastThirtyDaysTimeRange)).toBeGreaterThanOrEqual(thirty)
  })

  const hour = moment()
    .subtract(1, 'hours')
    .valueOf()
  it(`getStartTime should return ${hour} when lower is now() - 1h`, () => {
    expect(getStartTime(pastHourTimeRange)).toBeGreaterThanOrEqual(hour)
  })

  const fifteen = moment()
    .subtract(15, 'minutes')
    .valueOf()
  it(`getStartTime should return ${hour} when lower is now() - 1h`, () => {
    expect(getStartTime(pastFifteenMinTimeRange)).toBeGreaterThanOrEqual(
      fifteen
    )
  })

  const date = 'January 1, 2019'
  const newYears = moment(date).valueOf()
  it(`getStartTime should return ${newYears} when lower is ${date}`, () => {
    const timeRange = {
      type: custom,
      lower: date,
      upper: date,
    }
    expect(getStartTime(timeRange)).toEqual(newYears)
  })

  it(`getEndTime should return ${newYears} when lower is ${date}`, () => {
    const timeRange = {
      type: custom,
      lower: date,
      upper: date,
    }
    expect(getEndTime(timeRange)).toEqual(newYears)
  })

  const now = moment().valueOf()
  it(`getEndTime should return ${now} when upper is null and lower includes now()`, () => {
    expect(getEndTime(pastThirtyDaysTimeRange)).toBeGreaterThanOrEqual(now)
  })
})

describe('getting active tag values', () => {
  const activeQueryTags = [
    {
      keys: [
        '_field',
        '_measurement',
        'cpu',
        'device',
        'fstype',
        'host',
        'interface',
        'mode',
        'name',
        'path',
      ],
      values: [
        'cpu',
        'disk',
        'diskio',
        'mem',
        'net',
        'processes',
        'swap',
        'system',
      ],
    },
    {
      keys: ['_field', 'host'],
      values: [
        'active',
        'available',
        'available_percent',
        'buffered',
        'cached',
        'commit_limit',
        'committed_as',
        'dirty',
        'free',
        'high_free',
        'high_total',
        'huge_page_size',
        'huge_pages_free',
        'huge_pages_total',
        'inactive',
        'low_free',
        'low_total',
        'mapped',
        'page_tables',
        'shared',
        'slab',
        'swap_cached',
        'swap_free',
        'swap_total',
        'total',
        'used',
        'used_percent',
        'vmalloc_chunk',
        'vmalloc_total',
        'vmalloc_used',
        'wired',
        'write_back',
        'write_back_tmp',
      ],
    },
    {
      keys: ['host'],
      values: ['foo_computer'],
    },
  ]
  beforeEach(() => {
    mocked(isFlagEnabled).mockReset()
  })

  it("returns the active query tag values when the isFlagEnabled('queryBuilderGrouping') is toggled off", () => {
    mocked(isFlagEnabled).mockImplementation(() => {
      return false
    })

    const actualTags = getActiveTagValues(activeQueryTags, 'filter', 2)
    expect(actualTags).toEqual(activeQueryTags[2].values)
  })

  it("returns the active query tag values when the isFlagEnabled('queryBuilderGrouping') is toggled on, but the function is filter", () => {
    mocked(isFlagEnabled).mockImplementation(() => {
      return true
    })

    const actualTags = getActiveTagValues(activeQueryTags, 'filter', 2)
    expect(actualTags).toEqual(activeQueryTags[2].values)
  })

  it("returns all previous tag values when the isFlagEnabled('queryBuilderGrouping') is toggled on and the function is group", () => {
    mocked(isFlagEnabled).mockImplementation(() => {
      return true
    })

    const actualTags = getActiveTagValues(activeQueryTags, 'group', 2)
    expect(actualTags).toEqual([
      ...activeQueryTags[0].values,
      ...activeQueryTags[1].values,
    ])
  })
})
