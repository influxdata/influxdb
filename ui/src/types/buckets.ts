export {Bucket as GenBucket} from 'src/client'

import {Bucket as GenBucket} from 'src/client'

export interface OwnBucket extends Omit<GenBucket, 'labels'> {
  labels?: string[]
  readableRetention: string
}

export interface DemoBucket extends Omit<OwnBucket, 'type'> {
  type: 'demodata'
}

export type Bucket = DemoBucket | OwnBucket

export type RetentionRule = GenBucket['retentionRules'][0]

export type LineProtocolTab = 'Upload File' | 'Enter Manually'

export enum WritePrecision {
  Ms = 'ms',
  S = 's',
  Us = 'us',
  Ns = 'ns',
}
