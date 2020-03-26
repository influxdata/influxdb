import AJAX from 'src/utils/ajax'
import {get} from 'lodash'
import {Bucket} from 'src/types'

const baseURL = '/api/v2/experimental/sampledata'

export const getDemoDataBuckets = async (): Promise<Bucket[]> => {
  try {
    const {data} = await AJAX({
      method: 'GET',
      url: `${baseURL}/buckets`,
    })

    // if sampledata endpoints are not available in a cluster
    // gateway responds with a list of links where 'buckets' field is a string
    const buckets = get(data, 'buckets', false)
    if (!buckets || !Array.isArray(buckets)) {
      throw new Error('Could not reach demodata endpoint')
    }

    return buckets.filter(b => b.type == 'user') as Bucket[] // remove returned _tasks and _monitoring buckets
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const getDemoDataBucketMembership = async (
  bucketID: string,
  userID: string
) => {
  try {
    const response = await AJAX({
      method: 'POST',
      url: `${baseURL}/buckets/${bucketID}/members`,
      data: {userID},
    })

    if (response.status === '200') {
      // a failed or successful membership POST to sampledata should return 204
      throw new Error('Could not reach demodata endpoint')
    }
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const deleteDemoDataBucketMembership = async (
  bucketID: string,
  userID: string
) => {
  try {
    const response = await AJAX({
      method: 'DELETE',
      url: `${baseURL}/buckets/${bucketID}/members/${userID}`,
    })

    if (response.status === '200') {
      // a failed or successful membership DELETE to sampledata should return 204
      throw new Error('Could not reach demodata endpoint')
    }
  } catch (error) {
    console.error(error)
    throw error
  }
}
