import {Bucket} from 'src/types'

import {PrettyBucket} from 'src/buckets/components/BucketCard'

// Utils
import {ruleToString} from 'src/utils/formatting'

export const prettyBuckets = (buckets: Bucket[]): PrettyBucket[] => {
  return buckets.map(b => {
    const expire = b.retentionRules.find(rule => rule.type === 'expire')

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
