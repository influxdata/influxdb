export interface Tags {
  [key: string]: string
}

export interface Fields {
  [key: string]: number | string
}

export enum Precision {
  ns = 'ns',
  u = 'u',
  ms = 'ms',
  s = 's',
  m = 'm',
  h = 'h',
}

export interface BatchOptions {
  maxIntervalInSeconds: number
  maxBatchedLines: number
}

const defaultBatchOptions: BatchOptions = {
  maxIntervalInSeconds: 10,
  maxBatchedLines: 100,
}

const nowInSeconds = function nowInSeconds() {
  return Math.floor(Date.now() / 1000)
}

export class LineWriter {
  protected url: string
  protected orgID: number | string
  protected bucketName: string
  protected authToken: string
  protected batchOptions: BatchOptions

  protected timerID: number | null
  protected batchedLines: number
  protected lines: string[]

  constructor(
    url: string,
    orgID: number | string,
    bucketName: string,
    authToken: string,
    batchOptions: BatchOptions = defaultBatchOptions
  ) {
    this.url = url
    this.orgID = orgID
    this.bucketName = bucketName
    this.authToken = authToken
    this.batchOptions = batchOptions

    this.timerID = null
    this.batchedLines = 0
    this.lines = []
  }

  public createLineFromModel(
    measurement: string,
    fields: Fields,
    tags: Tags = {},
    timestamp: number = nowInSeconds()
  ): string {
    let tagString = ''
    Object.keys(tags)
      // Sort keys for a little extra perf
      // https://v2.docs.influxdata.com/v2.0/write-data/best-practices/optimize-writes/#sort-tags-by-key
      .sort((a, b) => a.localeCompare(b))
      .forEach((tagKey, i, tagKeys) => {
        const tagValue = tags[tagKey]
        const printableTagKey = tagKey
          .replace(/\s+/g, '\\ ') // replace any number of spaces with an escaped space
          .replace(/,/g, '\\,') // replace commas with escaped commas
          .replace(/=/g, '\\=') // replace equal signs with escaped equal signs

        const printableTagValue = tagValue
          .replace(/\n/g, '') // remove newlines
          .replace(/\s+/g, '\\ ') // replace any number of spaces with an escaped space
          .replace(/,/g, '\\,') // replace commas with escaped commas
          .replace(/=/g, '\\=') // replace equal signs with escaped equal signs

        tagString = `${tagString}${printableTagKey}=${printableTagValue}`

        // if this isn't the end of the string, append a comma
        if (i < tagKeys.length - 1) {
          tagString = `${tagString},`
        }
      })

    let fieldString = ''
    Object.keys(fields).forEach((fieldKey, i, fieldKeys) => {
      const fieldValue = fields[fieldKey]

      const printableFieldKey = fieldKey
        .replace(/\s+/g, '\\ ') // replace any number of spaces with an escaped space
        .replace(/,/g, '\\,') // replace commas with escaped commas
        .replace(/=/g, '\\=') // replace equal signs with escaped equal signs

      let printableFieldValue = fieldValue
      if (typeof fieldValue === 'string') {
        printableFieldValue = fieldValue
          .replace(/\n/g, '') // remove newlines
          .replace(/\\/g, '\\') // replace single backslach with an escaped backslash
          .replace(/"/g, '\\"') // replace double quotes with escaped double quotes
      }

      fieldString = `${fieldString}${printableFieldKey}=${printableFieldValue}`

      // if this isn't the end of the string, append a comma
      if (i < fieldKeys.length - 1) {
        fieldString = `${fieldString},`
      }
    })

    let lineStart = measurement
      .replace(/\s+/g, '\\ ') // replace any number of spaces with an escaped space
      .replace(/,/g, '\\,') // replace commas with escaped commas

    if (tagString !== '') {
      lineStart = `${lineStart},${tagString}`
    }

    return `${lineStart} ${fieldString} ${timestamp}`
  }

  public batchedWrite = (line: string, precision: Precision = Precision.s) => {
    this.lines.push(line)
    this.batchedLines++

    if (this.batchedLines > this.batchOptions.maxBatchedLines) {
      this.writeLine(this.lines.join('\n'), precision)
      window.clearTimeout(this.timerID)
      this.timerID = null
      this.batchedLines = 0
      this.lines = []
      return
    }

    if (this.timerID) {
      return
    }

    this.timerID = window.setTimeout(() => {
      this.writeLine(this.lines.join('\n'), precision)
      window.clearTimeout(this.timerID)
      this.timerID = null
      this.batchedLines = 0
      this.lines = []
    }, this.batchOptions.maxIntervalInSeconds * 1000)
  }

  protected writeLine = async (
    line: string,
    precision: Precision = Precision.s
  ) => {
    const url = `${this.url}/api/v2/write?org=${this.orgID}&bucket=${this.bucketName}&precision=${precision}`
    try {
      return await fetch(url, {
        method: 'POST',
        headers: {
          Authorization: `Token ${this.authToken}`,
        },
        body: line,
      })
    } catch (error) {
      console.error(error)
    }
  }
}
