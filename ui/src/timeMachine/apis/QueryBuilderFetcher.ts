// APIs
import {
  findBuckets,
  findKeys,
  findValues,
} from 'src/timeMachine/apis/queryBuilder'

// Types
import {BuilderConfig} from 'src/types/v2'
import {WrappedCancelablePromise} from 'src/types/promises'

type CancelableQuery = WrappedCancelablePromise<string[]>

class QueryBuilderFetcher {
  private findBucketsQuery: CancelableQuery
  private findKeysQueries: CancelableQuery[] = []
  private findValuesQueries: CancelableQuery[] = []
  private findKeysCache: {[key: string]: string[]} = {}
  private findValuesCache: {[key: string]: string[]} = {}
  private findBucketsCache: {[key: string]: string[]} = {}

  public async findBuckets(url: string, orgID: string): Promise<string[]> {
    this.cancelFindBuckets()

    const cacheKey = JSON.stringify([...arguments])
    const cachedResult = this.findBucketsCache[cacheKey]

    if (cachedResult) {
      return Promise.resolve(cachedResult)
    }

    const pendingResult = findBuckets(url, orgID)

    pendingResult.promise.then(result => {
      this.findBucketsCache[cacheKey] = result
    })

    return pendingResult.promise
  }

  public cancelFindBuckets(): void {
    if (this.findBucketsQuery) {
      this.findBucketsQuery.cancel()
    }
  }

  public async findKeys(
    index: number,
    url: string,
    orgID: string,
    bucket: string,
    tagsSelections: BuilderConfig['tags'],
    searchTerm: string = ''
  ): Promise<string[]> {
    this.cancelFindKeys(index)

    const cacheKey = JSON.stringify([...arguments].slice(1))
    const cachedResult = this.findKeysCache[cacheKey]

    if (cachedResult) {
      return Promise.resolve(cachedResult)
    }

    const pendingResult = findKeys(
      url,
      orgID,
      bucket,
      tagsSelections,
      searchTerm
    )

    this.findKeysQueries[index] = pendingResult

    pendingResult.promise.then(result => {
      this.findKeysCache[cacheKey] = result
    })

    return pendingResult.promise
  }

  public cancelFindKeys(index: number): void {
    if (this.findKeysQueries[index]) {
      this.findKeysQueries[index].cancel()
    }
  }

  public async findValues(
    index: number,
    url: string,
    orgID: string,
    bucket: string,
    tagsSelections: BuilderConfig['tags'],
    key: string,
    searchTerm: string = ''
  ): Promise<string[]> {
    this.cancelFindValues(index)

    const cacheKey = JSON.stringify([...arguments].slice(1))
    const cachedResult = this.findValuesCache[cacheKey]

    if (cachedResult) {
      return Promise.resolve(cachedResult)
    }

    const pendingResult = findValues(
      url,
      orgID,
      bucket,
      tagsSelections,
      key,
      searchTerm
    )

    this.findValuesQueries[index] = pendingResult

    pendingResult.promise.then(result => {
      this.findValuesCache[cacheKey] = result
    })

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
