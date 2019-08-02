// APIs
import {
  findBuckets,
  findKeys,
  findValues,
  FindBucketsOptions,
  FindKeysOptions,
  FindValuesOptions,
} from 'src/timeMachine/apis/queryBuilder'

// Types
import {CancelBox} from 'src/types'

type CancelableQuery = CancelBox<string[]>

class QueryBuilderFetcher {
  private findBucketsQuery: CancelableQuery
  private findKeysQueries: CancelableQuery[] = []
  private findValuesQueries: CancelableQuery[] = []
  private findKeysCache: {[key: string]: string[]} = {}
  private findValuesCache: {[key: string]: string[]} = {}
  private findBucketsCache: {[key: string]: string[]} = {}

  public async findBuckets(options: FindBucketsOptions): Promise<string[]> {
    this.cancelFindBuckets()

    const cacheKey = JSON.stringify(options)
    const cachedResult = this.findBucketsCache[cacheKey]

    if (cachedResult) {
      return Promise.resolve(cachedResult)
    }

    const pendingResult = findBuckets(options)

    pendingResult.promise
      .then(result => {
        this.findBucketsCache[cacheKey] = result
      })
      .catch(() => {})

    return pendingResult.promise
  }

  public cancelFindBuckets(): void {
    if (this.findBucketsQuery) {
      this.findBucketsQuery.cancel()
    }
  }

  public async findKeys(
    index: number,
    options: FindKeysOptions
  ): Promise<string[]> {
    this.cancelFindKeys(index)

    const cacheKey = JSON.stringify(options)
    const cachedResult = this.findKeysCache[cacheKey]

    if (cachedResult) {
      return Promise.resolve(cachedResult)
    }

    const pendingResult = findKeys(options)

    this.findKeysQueries[index] = pendingResult

    pendingResult.promise
      .then(result => {
        this.findKeysCache[cacheKey] = result
      })
      .catch(() => {})

    return pendingResult.promise
  }

  public cancelFindKeys(index: number): void {
    if (this.findKeysQueries[index]) {
      this.findKeysQueries[index].cancel()
    }
  }

  public async findValues(
    index: number,
    options: FindValuesOptions
  ): Promise<string[]> {
    this.cancelFindValues(index)

    const cacheKey = JSON.stringify(options)
    const cachedResult = this.findValuesCache[cacheKey]

    if (cachedResult) {
      return Promise.resolve(cachedResult)
    }

    const pendingResult = findValues(options)

    this.findValuesQueries[index] = pendingResult

    pendingResult.promise
      .then(result => {
        this.findValuesCache[cacheKey] = result
      })
      .catch(() => {})

    return pendingResult.promise
  }

  public cancelFindValues(index: number): void {
    if (this.findValuesQueries[index]) {
      this.findValuesQueries[index].cancel()
    }
  }

  public clearCache(): void {
    this.findBucketsCache = {}
    this.findKeysCache = {}
    this.findValuesCache = {}
  }
}

export const queryBuilderFetcher = new QueryBuilderFetcher()
