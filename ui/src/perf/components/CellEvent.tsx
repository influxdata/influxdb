import {FC, useEffect} from 'react'
import {useSelector} from 'react-redux'
import {useParams} from 'react-router-dom'

// Selectors
import {getOrg} from 'src/organizations/selectors'

// Utils
import {event} from 'src/cloud/utils/reporting'

// Types
import {AppState} from 'src/types'

interface Props {
  id: string
  type: string
}

const getState = (s: AppState) => {
  return {
    scroll: s.perf.dashboard.scroll,
    mountID: s.perf.dashboard.mountID,
    orgID: getOrg(s).id,
  }
}

const CellEvent: FC<Props> = ({id, type}) => {
  const params = useParams<{dashboardID?: string}>()
  const dashboardID = params?.dashboardID

  const {mountID, orgID, scroll} = useSelector(getState)

  useEffect(() => {
    if (scroll === 'scrolled') {
      return
    }

    const hasIDs = dashboardID && id && orgID && mountID

    if (!hasIDs) {
      return
    }

    event('Cell Visualized', {dashboardID, cellID: id, orgID, mountID, type})
  }, [dashboardID, id, orgID, mountID, type, scroll])

  return null
}

export default CellEvent
