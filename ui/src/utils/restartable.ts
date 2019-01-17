import Deferred from 'src/utils/Deferred'

import {CancellationError} from 'src/types/promises'

// `restartable` is a utility that wraps promise-returning functions so that
// concurrent calls resolve successfully exactly once, and with the most
// up-to-date data.
//
// As an example, let `f` be a function returning a `Promise<V>`, and suppose
// that `f` is executed three times in immediate succession. The order in which
// each promise returned by `f` resolves is undefined, which often creates
// problems for impure callbacks chained off of the promises returned by `f`.
//
// Instead, we can let `g = restartable(f)` and call `g` three times instead.
// This will return three promises; the first two promises are guaranteed to
// reject with a `CancellationError`, while the third promise (the most recent
// call) will either resolve or reject according to the original behavior of
// `f`.
//
export function restartable<T extends any[], V>(
  f: (...args: T) => Promise<V>
): ((...args: T) => Promise<V>) {
  let id: number = 0

  const checkResult = async (
    promise: Promise<V>,
    deferred: Deferred,
    promiseID: number
  ) => {
    let result
    let isOk = true

    try {
      result = await promise
    } catch (error) {
      result = error
      isOk = false
    }

    if (promiseID !== id) {
      deferred.reject(new CancellationError())
    } else if (!isOk) {
      deferred.reject(result)
    } else {
      deferred.resolve(result)
    }
  }

  return (...args: T): Promise<V> => {
    const deferred = new Deferred()
    const promise = f(...args)

    id += 1
    checkResult(promise, deferred, id)

    return deferred.promise
  }
}
