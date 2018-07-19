import {WrappedCancelablePromise} from 'src/types/promises'

export const makeCancelable = <T>(
  promise: Promise<T>
): WrappedCancelablePromise<T> => {
  let isCanceled = false

  const wrappedPromise = new Promise<T>(async (resolve, reject) => {
    try {
      const value = await promise

      if (isCanceled) {
        reject({isCanceled})
      } else {
        resolve(value)
      }
    } catch (error) {
      if (isCanceled) {
        reject({isCanceled})
      } else {
        reject(error)
      }
    }
  })

  return {
    promise: wrappedPromise,
    cancel() {
      isCanceled = true
    },
  }
}
