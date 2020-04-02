import {RetentionRule} from 'src/types'

// Utils
import {ruleToString} from 'src/utils/formatting'

export const humanizeRetentionTime = (
  retentionRules: RetentionRule[]
): string => {
  const expire = retentionRules.find(rule => rule.type === 'expire')

  if (!expire) {
    return 'forever'
  }

  return ruleToString(expire.everySeconds)
}
