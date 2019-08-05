// This file provides a fake API and should be deleted soon

import {range} from 'lodash'
import {Row, LoadRows} from 'src/eventViewer/types'
import {CheckStatusLevel, CancellationError} from 'src/types'

const ROW_COUNT = 222

const notFizzBuzz = (i: number): CheckStatusLevel => {
  if (i % 3 === 0 && i % 5 === 0) {
    return 'CRIT'
  } else if (i % 5 === 0 || i % 3 === 0) {
    return 'WARN'
  } else {
    return 'OK'
  }
}

const rowsRequest = (rows, delay) => {
  let reject

  const promise = new Promise<Row[]>((res, rej) => {
    reject = rej

    setTimeout(() => res(rows), delay)
  })

  const cancel = () => reject(new CancellationError())

  return {promise, cancel}
}

export const fakeLoadRows: LoadRows = ({offset, limit, since, filter}) => {
  if (offset >= ROW_COUNT) {
    return rowsRequest([], 500)
  }

  const allRows = range(ROW_COUNT).map(i => ({
    time: since - 1000 * 30 * i,
    checkID: '123',
    checkName: i % 5 === 0 ? 'high mem' : 'low CPU',
    status: notFizzBuzz(i),
    message: `hello from row ${i}`,
    tags: {host: 'pt2ph8', environment: 'dev'},
  }))

  let currentRows = allRows

  if (filter) {
    currentRows = currentRows.filter(
      row =>
        row.message.includes((filter as any).right.right) ||
        row.checkName.includes((filter as any).left.right)
    )
  }

  currentRows = currentRows.slice(offset, Math.min(ROW_COUNT, offset + limit))

  return rowsRequest(currentRows, 1000)
}
