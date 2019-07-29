import {Bucket} from 'src/client'

export type BucketRetentionRules = Bucket['retentionRules']

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

export {Bucket} from 'src/client'
