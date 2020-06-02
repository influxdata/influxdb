import {useState} from 'react'

type NotificationState = 'visible' | 'hidden'

interface Actions {
  hide: () => void
  show: () => void
}

// useNotify controls state for @influxdata/clockface Notification component
const useNotify = (
  initialState: NotificationState = 'hidden'
): [boolean, Actions] => {
  const [notify, setNotify] = useState<boolean>(initialState == 'visible')

  const actions = {
    hide: () => setNotify(false),
    show: () => setNotify(true),
  }

  return [notify, actions]
}

export {useNotify, NotificationState}

/* Example usage
  const [notify, {hide, show}] = useNotify()

  const showNotification = () => {
    show()
  }

  <Notification
    onDismiss={hide}
    onTimeout={hide}
    visible={notify === "visible"}
    duration={2000}
  >
    Alert text here!
  </Notification>
*/
