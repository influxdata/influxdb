import AJAX from 'src/utils/ajax'
import {Bucket} from 'src/types/v2/buckets'

export const getBuckets = async (url: string): Promise<Bucket[]> => {
  try {
    const {data} = await AJAX({url})
    return data
  } catch (error) {
    console.error(error)
    throw error
  }
}
