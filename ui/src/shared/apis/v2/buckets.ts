import {Bucket, Source} from 'src/api'
import {bucketsAPI} from 'src/utils/api'

export const getBuckets = async (source: Source): Promise<Bucket[]> => {
  try {
    const {data} = await bucketsAPI.sourcesSourceIDBucketsGet(source.id, null)

    return data.buckets
  } catch (error) {
    console.error(error)
    throw error
  }
}
