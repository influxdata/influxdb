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

const getState = (cellID: string) => (state: AppState) => {
  const {perf} = state
  const {dashboard, cells} = perf
  const {scroll, renderID} = dashboard

  return {
    scroll,
    renderID,
    cellMountStartMs: cells.byID[cellID]?.mountStartMs,
  }
}

const CellEvent: FC<Props> = ({id, type}) => {
  const params = useParams<{dashboardID?: string}>()
  const dashboardID = params?.dashboardID

  const {renderID, scroll, cellMountStartMs} = useSelector(getState(id))

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

  useEffect(() => {
    if (!cellMountStartMs) {
      return
    }

    const hasIDs = dashboardID && id && renderID
    if (!hasIDs) {
      return
    }

    const visRenderedMs = new Date().getTime()
    const timeToAppearMs = visRenderedMs - cellMountStartMs

    const tags = {dashboardID, cellID: id, type}
    const fields = {timeToAppearMs, renderID}

    event('Cell Render Cycle', tags, fields)
  }, [cellMountStartMs, dashboardID, id, renderID, type])

  return null
}

export default CellEvent
