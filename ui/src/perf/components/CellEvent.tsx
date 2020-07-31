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
    renderID: s.perf.dashboard.renderID,
    orgID: getOrg(s).id,
  }
}

const CellEvent: FC<Props> = ({id, type}) => {
  const params = useParams<{dashboardID?: string}>()
  const dashboardID = params?.dashboardID

  const {renderID, orgID, scroll} = useSelector(getState)

  useEffect(() => {
    if (scroll === 'scrolled') {
      return
    }

    const hasIDs = dashboardID && id && orgID && renderID

    if (!hasIDs) {
      return
    }

    event('Cell Visualized', {dashboardID, cellID: id, orgID, renderID, type})
  }, [dashboardID, id, orgID, renderID, type, scroll])

  return null
}

export default CellEvent
