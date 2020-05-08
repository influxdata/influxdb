import {WebsiteMonitoringBucket} from 'src/cloud/constants'
import {reportError} from 'src/shared/utils/errors'
import {CLOUD} from 'src/shared/constants'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

export const isDemoDataAvailabilityError = (errorMessage: string): boolean => {
  if (!CLOUD || isFlagEnabled('demodata')) {
    return false
  }
  const findBucketErrorMessage =
    'failed to initialize execute state: could not find bucket /"'

  if (errorMessage.startsWith(findBucketErrorMessage)) {
    const missingBucket = errorMessage
      .replace(findBucketErrorMessage, '')
      .replace('/"', '')

    if (missingBucket === WebsiteMonitoringBucket) {
      reportError(
        {
          name: 'demodata switched off but user had demodata dashboard',
          message: errorMessage,
        },
        {
          context: {},
          name: 'demodata switched off but user had demodata dashboard',
        }
      )
      return true
    }
  }
  return false
}
