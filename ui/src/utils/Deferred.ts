class Deferred {
  public promise: Promise<any>
  public resolve: (...rest: any[]) => void
  public reject: (error: Error) => void

  constructor() {
    this.promise = new Promise((resolve, reject) => {
      this.resolve = resolve
      this.reject = reject
    })
  }
}

export default Deferred
