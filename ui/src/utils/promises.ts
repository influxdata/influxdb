import {CancelBox} from 'src/types/promises'

export const makeCancelable = <T>(promise: Promise<T>): CancelBox<T> => {
  let isCanceled = false

  const wrappedPromise = new Promise<T>((resolve, reject) => {
    try {
      if (isCanceled) {
        reject({isCanceled})
      } else {
        resolve(promise)
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
