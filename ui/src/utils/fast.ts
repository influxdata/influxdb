export function fastReduce<T = any, R = any>(
  subject: T[],
  iterator: (r: R, t: T, i: number, s: T[]) => R,
  initialValue: R
): R {
  const length = subject.length
  let i: number
  let result: any

  if (initialValue === undefined) {
    i = 1
    result = subject[0]
  } else {
    i = 0
    result = initialValue
  }

  for (; i < length; i++) {
    result = iterator(result, subject[i], i, subject)
  }

  return result
}

export function fastMap<T = any, R = any>(
  subject: T[],
  iterator: (t: T, i: number, s: T[]) => R
): R[] {
  const length = subject.length
  const result: R[] = new Array(length)
  for (let i = 0; i < length; i++) {
    result[i] = iterator(subject[i], i, subject)
  }
  return result
}

export function fastFilter<T = any>(
  subject: T[],
  iterator: (t: T, i: number, s: T[]) => boolean
): T[] {
  const length = subject.length
  const result: T[] = []
  for (let i = 0; i < length; i++) {
    if (iterator(subject[i], i, subject)) {
      result.push(subject[i])
    }
  }
  return result
}
