// Libraries
import {FC, useEffect} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// APIs
import {postSignout} from 'src/client'

// Constants
import {CLOUD, CLOUD_URL, CLOUD_LOGOUT_PATH} from 'src/shared/constants'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

const Logout: FC<WithRouterProps> = ({router}) => {
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
    handleSignOut()
  }, [])
  return null
}

export default ErrorHandling(withRouter<WithRouterProps>(Logout))
