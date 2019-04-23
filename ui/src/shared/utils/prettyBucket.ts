import {Bucket} from 'src/types'

import {BucketRetentionRules} from '@influxdata/influx'
import {PrettyBucket} from 'src/buckets/components/BucketRow'

// Utils
import {ruleToString} from 'src/utils/formatting'

export const prettyBuckets = (buckets: Bucket[]): PrettyBucket[] => {
  return buckets.map(b => {
    const expire = b.retentionRules.find(
      rule => rule.type === BucketRetentionRules.TypeEnum.Expire
    )

    if (!expire) {
      return {
        ...b,
        ruleString: 'forever',
      }
    }

    return {
      ...b,
      ruleString: ruleToString(expire.everySeconds),
    }
  })
}
