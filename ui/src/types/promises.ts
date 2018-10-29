export interface WrappedCancelablePromise<T> {
  promise: Promise<T>
  cancel: () => void
}
