import {CLOUD_FLAGS, OSS_FLAGS} from 'src/shared/selectors/flags'

describe("getting the user's feature flags", () => {
  beforeEach(() => {
    jest.resetModules()
  })

  it('gets the OSS flags by default if CLOUD is not enabled', () => {
    jest.mock('src/shared/constants/index', () => ({
      CLOUD: false,
      CLOUD_BILLING_VISIBLE: true,
    }))
    const {getUserFlags} = require('src/shared/utils/featureFlag')

    const flags = getUserFlags()
    expect(flags).toEqual(OSS_FLAGS)
  })

  it('gets the cloud flags if CLOUD is enabled', () => {
    jest.mock('src/shared/constants/index', () => ({
      CLOUD: true,
      CLOUD_BILLING_VISIBLE: false,
    }))
    const {getUserFlags} = require('src/shared/utils/featureFlag')

    const flags = getUserFlags()
    expect(flags).toEqual(CLOUD_FLAGS)
  })
})

// todo: test the dadgum isFlagEnabled function
