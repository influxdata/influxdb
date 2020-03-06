import {Bucket as IBucket} from 'src/client'

export type GenBucket = IBucket

export interface Bucket extends Omit<IBucket, 'labels'> {
  labels?: string[]
}

export type BucketRetentionRules = IBucket['retentionRules']

export enum RetentionRuleTypes {
  Expire = 'expire',
  Forever = 'forever',
}

export interface RetentionRule {
  type: RetentionRuleTypes
  everySeconds: number
}

export interface BucketLinks {
  org: string
  self: string
}
