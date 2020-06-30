// Libraries
import {FC, useEffect} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router-dom'

// APIs
import {postSignout} from 'src/client'

// Constants
import {CLOUD, CLOUD_URL, CLOUD_LOGOUT_PATH} from 'src/shared/constants'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {reset} from 'src/shared/actions/flags'

interface DispatchProps {
  resetFeatureFlags: typeof reset
}

type Props = DispatchProps & WithRouterProps
const Logout: FC<Props> = ({router, resetFeatureFlags}) => {
  const handleSignOut = async () => {
    if (CLOUD) {
      window.location.href = `${CLOUD_URL}${CLOUD_LOGOUT_PATH}`
      return
    } else {
      const resp = await postSignout({})

      if (resp.status !== 204) {
        throw new Error(resp.data.message)
      }

      router.push(`/signin`)
    }
  }

  useEffect(() => {
    resetFeatureFlags()
    handleSignOut()
  }, [])
  return null
}

const mdtp = {
  resetFeatureFlags: reset,
}

export default ErrorHandling(
  connect<{}, DispatchProps>(null, mdtp)(withRouter<WithRouterProps>(Logout))
)
