class Deferred<T = any> {
  public promise: Promise<T>
  public resolve: (...rest: T[]) => void
  public reject: (error: Error) => void

  constructor() {
    this.promise = new Promise((resolve, reject) => {
      this.resolve = resolve
      this.reject = reject
    })
  }
}

export default Deferred
