export {Bucket as GenBucket} from 'src/client'

import {Bucket as GenBucket, Labels} from 'src/client'

export interface OwnBucket extends GenBucket {
  labels?: string[]
  readableRetention: string
}

export interface DemoBucket extends Omit<OwnBucket, 'type'> {
  type: 'demodata'
}

export interface BucketWithLabel extends GenBucket {
  labels?: Labels
}

export type Bucket = DemoBucket | OwnBucket

export type RetentionRule = GenBucket['retentionRules'][0]
