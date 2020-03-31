import {mocked} from 'ts-jest/utils'

window.fetch = jest.fn()
import {LineWriter, Precision} from 'src/utils/lineWriter'

describe('creating a line from a model', () => {
  const lineWriter = new LineWriter(
    'http://example.com',
    'orgid',
    'bucket',
    'token=='
  )

  it('creates a line without tags', () => {
    const measurement = 'performance'
    const tags = {}
    const fields = {fps: 55}
    const timestamp = 1584990314

    const line = lineWriter.createLineFromModel(
      measurement,
      fields,
      tags,
      timestamp
    )
    expect(line).toBe('performance fps=55 1584990314')
  })

  it('creates a line when no tags are passed in', () => {
    const measurement = 'performance'
    const fields = {fps: 55}

    const line = lineWriter.createLineFromModel(measurement, fields)
    expect(line).toEqual(expect.stringContaining('performance fps=55'))
  })

  it('creates a line without tags with multiple fields', () => {
    const measurement = 'performance'
    const tags = {}
    const fields = {fps: 49.33333, heap: 48577273}
    const timestamp = 1584990314

    const line = lineWriter.createLineFromModel(
      measurement,
      fields,
      tags,
      timestamp
    )
    expect(line).toBe('performance fps=49.33333,heap=48577273 1584990314')
  })

  it('creates a line with a tag', () => {
    const measurement = 'performance'
    const tags = {region: 'us-west'}
    const fields = {fps: 49.33333, heap: 48577273}
    const timestamp = 1584990314

    const line = lineWriter.createLineFromModel(
      measurement,
      fields,
      tags,
      timestamp
    )
    expect(line).toBe(
      'performance,region=us-west fps=49.33333,heap=48577273 1584990314'
    )
  })

  it('creates a line with multiple tags', () => {
    const measurement = 'performance'
    const tags = {region: 'us-west', status: 'good'}
    const fields = {fps: 49.33333, heap: 48577273}
    const timestamp = 1584990314

    const line = lineWriter.createLineFromModel(
      measurement,
      fields,
      tags,
      timestamp
    )
    expect(line).toBe(
      'performance,region=us-west,status=good fps=49.33333,heap=48577273 1584990314'
    )
  })

  it('alphabetizes tags by key, for write optimization', () => {
    const measurement = 'performance'
    const tags = {region: 'us-west', environment: 'dev'}
    const fields = {fps: 49.33333, heap: 48577273}
    const timestamp = 1584990314

    const line = lineWriter.createLineFromModel(
      measurement,
      fields,
      tags,
      timestamp
    )
    expect(line).toBe(
      'performance,environment=dev,region=us-west fps=49.33333,heap=48577273 1584990314'
    )
  })

  describe('replacing characters which could make the parser barf', () => {
    describe('measurement', () => {
      it('replaces many spaces with a single escaped space', () => {
        const measurement = 'performance                  of things'
        const tags = {region: 'us-west'}
        const fields = {fps: 49.33333, heap: 48577273}
        const timestamp = 1584990314

        const line = lineWriter.createLineFromModel(
          measurement,
          fields,
          tags,
          timestamp
        )
        expect(line).toBe(
          'performance\\ of\\ things,region=us-west fps=49.33333,heap=48577273 1584990314'
        )
      })

      it('replaces commas with escaped commas', () => {
        const measurement = 'performance,art'
        const tags = {region: 'us-west'}
        const fields = {fps: 49.33333, heap: 48577273}
        const timestamp = 1584990314

        const line = lineWriter.createLineFromModel(
          measurement,
          fields,
          tags,
          timestamp
        )
        expect(line).toBe(
          'performance\\,art,region=us-west fps=49.33333,heap=48577273 1584990314'
        )
      })
    })
  })
  describe('tag keys and values', () => {
    it('replaces many spaces with a single escaped space', () => {
      const measurement = 'performance'
      const tags = {'region          of the world': 'us              west'}
      const fields = {fps: 49.33333, heap: 48577273}
      const timestamp = 1584990314

      const line = lineWriter.createLineFromModel(
        measurement,
        fields,
        tags,
        timestamp
      )
      expect(line).toBe(
        'performance,region\\ of\\ the\\ world=us\\ west fps=49.33333,heap=48577273 1584990314'
      )
    })

    it('replaces commas with an escaped comma', () => {
      const measurement = 'performance'
      const tags = {'region,of,the,world': 'us,west'}
      const fields = {fps: 49.33333, heap: 48577273}
      const timestamp = 1584990314

      const line = lineWriter.createLineFromModel(
        measurement,
        fields,
        tags,
        timestamp
      )
      expect(line).toBe(
        'performance,region\\,of\\,the\\,world=us\\,west fps=49.33333,heap=48577273 1584990314'
      )
    })

    it('replaces equals signs with an escaped equal sign', () => {
      const measurement = 'performance'
      const tags = {'region=thewo=rld': 'us=west'}
      const fields = {fps: 49.33333, heap: 48577273}
      const timestamp = 1584990314

      const line = lineWriter.createLineFromModel(
        measurement,
        fields,
        tags,
        timestamp
      )
      expect(line).toBe(
        'performance,region\\=thewo\\=rld=us\\=west fps=49.33333,heap=48577273 1584990314'
      )
    })

    it('replaces newlines with an escaped newline in keys, and with nothing in values', () => {
      const measurement = 'performance'
      const tags = {'region\nworld': 'us\nwest'}
      const fields = {fps: 49.33333, heap: 48577273}
      const timestamp = 1584990314

      const line = lineWriter.createLineFromModel(
        measurement,
        fields,
        tags,
        timestamp
      )
      expect(line).toBe(
        'performance,region\\ world=uswest fps=49.33333,heap=48577273 1584990314'
      )
    })
  })

  describe('field keys and values', () => {
    it('replaces many spaces with a single escaped space only in keys', () => {
      const measurement = 'performance'
      const tags = {}
      const fields = {'fp          s': 49.33333}
      const timestamp = 1584990314

      const line = lineWriter.createLineFromModel(
        measurement,
        fields,
        tags,
        timestamp
      )
      expect(line).toBe('performance fp\\ s=49.33333 1584990314')
    })

    it('replaces commas with an escaped comma only in keys', () => {
      const measurement = 'performance'
      const tags = {}
      const fields = {'fp,s': 49.33333}
      const timestamp = 1584990314

      const line = lineWriter.createLineFromModel(
        measurement,
        fields,
        tags,
        timestamp
      )
      expect(line).toBe('performance fp\\,s=49.33333 1584990314')
    })

    it('replaces equal signs with an escaped equal sign only in keys', () => {
      const measurement = 'performance'
      const tags = {}
      const fields = {'fp=s': 49.33333}
      const timestamp = 1584990314

      const line = lineWriter.createLineFromModel(
        measurement,
        fields,
        tags,
        timestamp
      )
      expect(line).toBe('performance fp\\=s=49.33333 1584990314')
    })

    it('replaces newlines with empty strings only in values', () => {
      const measurement = 'performance'
      const tags = {}
      const fields = {fps: '49.\n33333'}
      const timestamp = 1584990314

      const line = lineWriter.createLineFromModel(
        measurement,
        fields,
        tags,
        timestamp
      )
      expect(line).toBe('performance fps=49.33333 1584990314')
    })

    it('replaces single backslashes with double backslashes only in values', () => {
      const fpsString = String.raw`turk\182`

      const measurement = 'performance'
      const tags = {}
      const fields = {fps: fpsString}
      const timestamp = 1584990314

      const line = lineWriter.createLineFromModel(
        measurement,
        fields,
        tags,
        timestamp
      )
      expect(line).toBe('performance fps=turk\\182 1584990314')
    })

    it('replaces double quotes with escaped double quotes only in values', () => {
      const measurement = 'performance'
      const tags = {}
      const fields = {fps: '49"."33333'}
      const timestamp = 1584990314

      const line = lineWriter.createLineFromModel(
        measurement,
        fields,
        tags,
        timestamp
      )
      expect(line).toBe('performance fps=49\\".\\"33333 1584990314')
    })
  })
})

describe('batched writes', () => {
  jest.useFakeTimers()

  afterEach(() => {
    mocked(window.fetch).mockReset()
  })

  const lineWriter = new LineWriter(
    'http://example.com',
    'orgid',
    'bucket',
    'token=='
  )

  it('throttles writes to 100 lines (by default) per http POST', () => {
    const batchedLines = []

    const measurement = 'performance'
    const tags = {}
    const timestamp = 1585163446
    for (let i = 0; i < 100; i++) {
      const line = lineWriter.createLineFromModel(
        measurement,
        {i: i},
        tags,
        timestamp + i
      )
      lineWriter.batchedWrite(line, Precision.s)
      batchedLines.push(line)
    }
    expect(mocked(window.fetch).mock.calls.length).toBe(0)
    const finalLine = lineWriter.createLineFromModel(
      measurement,
      {i: 100},
      tags,
      timestamp + 100
    )
    lineWriter.batchedWrite(finalLine, Precision.s)
    batchedLines.push(finalLine)

    expect(mocked(window.fetch).mock.calls.length).toBe(1)
    const [url, requestParams] = mocked(window.fetch).mock.calls[0]

    expect(url).toBe(
      `http://example.com/api/v2/write?org=orgid&bucket=bucket&precision=${
        Precision.s
      }`
    )
    expect(requestParams).toEqual({
      method: 'POST',
      body: batchedLines.join('\n'),
      headers: {
        Authorization: 'Token token==',
      },
    })
  })

  it('waits 10 seconds (by default) to send an HTTP reqeust', () => {
    const measurement = 'performance'
    const timestamp = 1585163446

    const line = lineWriter.createLineFromModel(
      measurement,
      {foo: 1},
      {},
      timestamp
    )

    lineWriter.batchedWrite(line)

    jest.runAllTimers()

    expect(mocked(window.fetch).mock.calls.length).toBe(1)
    expect(window.setTimeout).toHaveBeenLastCalledWith(
      expect.any(Function),
      10000
    )

    const [url, requestParams] = mocked(window.fetch).mock.calls[0]

    expect(url).toBe(
      `http://example.com/api/v2/write?org=orgid&bucket=bucket&precision=${
        Precision.s
      }`
    )
    expect(requestParams).toEqual({
      method: 'POST',
      body: line,
      headers: {
        Authorization: 'Token token==',
      },
    })
  })
})
