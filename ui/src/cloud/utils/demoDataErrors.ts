import {WebsiteMonitoringBucket} from 'src/cloud/constants'
import {CLOUD} from 'src/shared/constants'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

export const isDemoDataAvailabilityError = (
  errorCode: string,
  errorMessage: string
): boolean => {
  if (!CLOUD || isFlagEnabled('demodata')) {
    return false
  }

  if (errorCode === 'not found') {
    if (errorMessage.includes(WebsiteMonitoringBucket)) {
      return true
    }
  }
  return false
}
