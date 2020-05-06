import memoizeOne from 'memoize-one'

/*
  Stores a cache of memoized functions which can be looked up by an arbitrary
  key. The memoized functions are instatiated lazily.

  Example:

  ```
  const f = () => {
    console.log('f called')

    return 2
  }

  const cache = new MemoizedFunctionCache()
  const fMemoized = cache.get('myKey', f)

  fMemoized() // logs "f called", returns 2
  fMemoized() // returns 2
  ```

*/
export class MemoizedFunctionCache {
  private memoizedFunctions: {
    [key: string]: {memoized: Function; original: Function}
  } = {}

  public get<T extends (...args: any[]) => any>(key: string, fn: T): T {
    if (!this.memoizedFunctions[key]) {
      this.memoizedFunctions[key] = {
        memoized: memoizeOne(fn),
        original: fn,
      }
    }

    const {memoized, original} = this.memoizedFunctions[key]

    if (original !== fn) {
      throw new Error(`expected ${fn} but found ${original}`)
    }

    return memoized as T
  }
}
