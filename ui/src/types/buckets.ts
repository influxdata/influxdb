import {IBucket} from '@influxdata/influx'

export interface Bucket extends IBucket {}

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
