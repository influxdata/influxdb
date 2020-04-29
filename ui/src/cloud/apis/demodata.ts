// Libraries
import {get} from 'lodash'
import {getBuckets} from 'src/client'
import AJAX from 'src/utils/ajax'

//Utils
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

//Types
import {Bucket, DemoBucket} from 'src/types'
import {LIMIT} from 'src/resources/constants'

const baseURL = '/api/v2/experimental/sampledata'

export const getDemoDataBuckets = async (): Promise<Bucket[]> => {
  const {data} = await AJAX({
    method: 'GET',
    url: `${baseURL}/buckets`,
  })

  // if sampledata endpoints are not available in a cluster
  // gateway responds with a list of links where 'buckets' field is a string
  const buckets = get(data, 'buckets', false)
  if (!Array.isArray(buckets)) {
    throw new Error('Could not reach demodata endpoint')
  }

  return buckets.filter(b => b.type == 'user') as Bucket[] // remove returned _tasks and _monitoring buckets
}

export const getDemoDataBucketMembership = async (
  bucketID: string,
  userID: string
) => {
  const response = await AJAX({
    method: 'POST',
    url: `${baseURL}/buckets/${bucketID}/members`,
    data: {userID},
  })

  if (response.status === '200') {
    // a failed or successful membership POST to sampledata should return 204
    throw new Error('Could not reach demodata endpoint')
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

export const fetchDemoDataBuckets = async (): Promise<Bucket[]> => {
  if (!isFlagEnabled('demodata')) return []

  try {
    // FindBuckets paginates before filtering for authed buckets until #6591 is resolved,
    // so UI needs to make getBuckets request with demodata orgID parameter
    const demoBuckets = await getDemoDataBuckets()

    const demodataOrgID = get(demoBuckets, '[0].orgID') as string

    if (!demodataOrgID) {
      throw new Error('Could not get demodata orgID')
    }

    const resp = await getBuckets({
      query: {orgID: demodataOrgID, limit: LIMIT},
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    return resp.data.buckets.map(b => ({
      ...b,
      type: 'demodata' as 'demodata',
      labels: [],
    })) as Array<DemoBucket>
  } catch (error) {
    console.error(error)
    return [] // demodata bucket fetching errors should not effect regular bucket fetching
  }
}
