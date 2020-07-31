import {FC, useEffect} from 'react'
import {useSelector} from 'react-redux'
import {useParams} from 'react-router-dom'

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
  }
}

const CellEvent: FC<Props> = ({id, type}) => {
  const params = useParams<{dashboardID?: string}>()
  const dashboardID = params?.dashboardID

  const {renderID, scroll} = useSelector(getState)

  useEffect(() => {
    if (scroll === 'scrolled') {
      return
    }

    const hasIDs = dashboardID && id && renderID

    if (!hasIDs) {
      return
    }

    const tags = {dashboardID, cellID: id, type}
    const fields = {renderID}

    event('Cell Visualized', tags, fields)
  }, [dashboardID, id, renderID, type, scroll])

  return null
}

export default CellEvent
