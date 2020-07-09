// Libraries
import {FC, useEffect} from 'react'
import {useDispatch} from 'react-redux'
import {RouteComponentProps} from 'react-router-dom'

// APIs
import {postSignout} from 'src/client'

// Constants
import {CLOUD, CLOUD_URL, CLOUD_LOGOUT_PATH} from 'src/shared/constants'

// Components
import {reset} from 'src/shared/actions/flags'

type Props = RouteComponentProps

const Logout: FC<Props> = ({history}) => {
  const dispatch = useDispatch()

  useEffect(() => {
    const handleSignOut = async () => {
      if (CLOUD) {
        window.location.href = `${CLOUD_URL}${CLOUD_LOGOUT_PATH}`
        return
      } else {
        const resp = await postSignout({})

        if (resp.status !== 204) {
          throw new Error(resp.data.message)
        }

        history.push(`/signin`)
      }
    }
    dispatch(reset())
    handleSignOut()
  }, [dispatch, history])

  return null
}

export default Logout
