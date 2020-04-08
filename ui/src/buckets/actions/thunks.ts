// Libraries
import {normalize} from 'normalizr'
import {Dispatch} from 'react'

// API
import * as api from 'src/client'

// Schemas
import {bucketSchema, arrayOfBuckets} from 'src/schemas'

// Types
import {
  RemoteDataState,
  GetState,
  GenBucket,
  Bucket,
  BucketEntities,
  Label,
  ResourceType,
  OwnBucket,
  BucketWithLabel,
} from 'src/types'

// Utils
import {getErrorMessage} from 'src/utils/api'
import {getOrg} from 'src/organizations/selectors'
import {getStatus} from 'src/resources/selectors'

// Actions
import {
  editBucket,
  addBucket,
  setBuckets,
  removeBucket,
  Action as BucketAction,
} from 'src/buckets/actions/creators'
import {notify, Action as NotifyAction} from 'src/shared/actions/notifications'
import {checkBucketLimits} from 'src/cloud/actions/limits'
import {fetchDemoDataBuckets} from 'src/cloud/apis/demodata'

// Constants
import {
  getBucketsFailed,
  bucketCreateFailed,
  bucketUpdateFailed,
  bucketDeleteFailed,
  bucketUpdateSuccess,
  bucketRenameSuccess,
  bucketRenameFailed,
  addBucketLabelFailed,
  removeBucketLabelFailed,
} from 'src/shared/copy/notifications'
import {LIMIT} from 'src/resources/constants'
import {isSystemBucket} from 'src/buckets/selectors'

type Action = BucketAction | NotifyAction

export const getBuckets = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const state = getState()
    if (getStatus(state, ResourceType.Buckets) === RemoteDataState.NotStarted) {
      dispatch(setBuckets(RemoteDataState.Loading))
    }
    const org = getOrg(state)

    const resp = await api.getBuckets({
      query: {orgID: org.id, limit: LIMIT},
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const demoDataBuckets = await fetchDemoDataBuckets()

    const ownBuckets: Array<BucketWithLabel> = resp.data.buckets

    for (let i = 0; i < ownBuckets.length; i++) {
      if (!isSystemBucket(ownBuckets[i].type)) {
        const labels = await getLabelsForBucketHelper(ownBuckets[i].id)
        ownBuckets[i].labels = labels
      }
    }

    const buckets = normalize<Bucket, BucketEntities, string[]>(
      [...ownBuckets, ...demoDataBuckets],
      arrayOfBuckets
    )

    dispatch(setBuckets(RemoteDataState.Done, buckets))
  } catch (error) {
    console.error(error)
    dispatch(setBuckets(RemoteDataState.Error))
    dispatch(notify(getBucketsFailed()))
  }
}

const getLabelsForBucketHelper = async (bucketID: string): Promise<api.Labels> => {
  try {
    const r = await api.getBucketsLabels({bucketID})
    if (r.status != 200) {
      throw new Error(r.data.message)
    }
    return r.data.labels
  } catch (e) {
    console.error(e)
  }
}

export const createBucket = (bucket: OwnBucket) => async (
  dispatch: Dispatch<Action | ReturnType<typeof checkBucketLimits>>,
  getState: GetState
) => {
  try {
    const org = getOrg(getState())

    const resp = await api.postBucket({data: {...bucket, orgID: org.id}})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const b: BucketWithLabel = resp.data
    b.labels = []

    const newBucket = normalize<Bucket, BucketEntities, string>(b, bucketSchema)

    dispatch(addBucket(newBucket))
    dispatch(checkBucketLimits())
  } catch (error) {
    console.error(error)
    const message = getErrorMessage(error)
    dispatch(notify(bucketCreateFailed(message)))
  }
}

export const updateBucket = (bucket: OwnBucket) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const data = denormalizeBucket(bucket)

    const resp = await api.patchBucket({
      bucketID: bucket.id,
      data,
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const b: BucketWithLabel = resp.data
    b.labels = []

    const newBucket = normalize<Bucket, BucketEntities, string>(b, bucketSchema)

    dispatch(editBucket(newBucket))
    dispatch(notify(bucketUpdateSuccess(bucket.name)))
  } catch (error) {
    console.error(error)
    const message = getErrorMessage(error)
    dispatch(notify(bucketUpdateFailed(message)))
  }
}

export const renameBucket = (originalName: string, bucket: OwnBucket) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const data = denormalizeBucket(bucket)

    const resp = await api.patchBucket({
      bucketID: bucket.id,
      data,
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const newBucket = normalize<Bucket, BucketEntities, string>(
      resp.data,
      bucketSchema
    )

    dispatch(editBucket(newBucket))
    dispatch(notify(bucketRenameSuccess(bucket.name)))
  } catch (error) {
    console.error(error)
    dispatch(notify(bucketRenameFailed(originalName)))
  }
}

export const deleteBucket = (id: string, name: string) => async (
  dispatch: Dispatch<Action | ReturnType<typeof checkBucketLimits>>
) => {
  try {
    const resp = await api.deleteBucket({bucketID: id})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeBucket(id))
    dispatch(checkBucketLimits())
  } catch (error) {
    console.error(error)
    dispatch(notify(bucketDeleteFailed(name)))
  }
}

export const addBucketLabel = (bucketID: string, label: Label) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const postResp = await api.postBucketsLabel({
      bucketID,
      data: {labelID: label.id},
    })

    if (postResp.status !== 201) {
      throw new Error(postResp.data.message)
    }

    const resp = await api.getBucket({bucketID})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const labels = await getLabelsForBucketHelper(bucketID)

    const bucket: BucketWithLabel = resp.data
    bucket.labels = labels

    const newBucket = normalize<Bucket, BucketEntities, string>(
      bucket,
      bucketSchema
    )

    dispatch(editBucket(newBucket))
  } catch (error) {
    console.error(error)
    dispatch(notify(addBucketLabelFailed()))
  }
}

export const deleteBucketLabel = (bucketID: string, label: Label) => async (
  dispatch: Dispatch<Action>
): Promise<void> => {
  try {
    const deleteResp = await api.deleteBucketsLabel({
      bucketID,
      labelID: label.id,
    })
    if (deleteResp.status !== 204) {
      throw new Error(deleteResp.data.message)
    }

    const resp = await api.getBucket({bucketID})
    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const labels = await getLabelsForBucketHelper(bucketID)

    const bucket: BucketWithLabel = resp.data
    bucket.labels = labels

    const newBucket = normalize<Bucket, BucketEntities, string>(
      resp.data,
      bucketSchema
    )

    dispatch(editBucket(newBucket))
  } catch (error) {
    console.error(error)
    dispatch(notify(removeBucketLabelFailed()))
  }
}

const denormalizeBucket = (bucket: OwnBucket): GenBucket => {
  return {
    ...bucket,
  }
}
