import {Bucket as IBucket} from 'src/client'

export type GenBucket = IBucket

export interface Bucket extends Omit<IBucket, 'labels'> {
  labels?: string[]
  readableRetention: string
}

export type RetentionRule = IBucket['retentionRules'][0]
