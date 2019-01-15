export interface Bucket {
  id: string
  name: string
  organization: string
  orgID: string
  rp?: string
  retentionRules: RetentionRule[]
  links: BucketLinks
}

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
