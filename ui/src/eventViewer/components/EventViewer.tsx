// Libraries
import {useEffect, useReducer, FC, ReactElement} from 'react'

// Utils
import {useMountedLayoutEffect} from 'src/shared/utils/useMountedEffect'
import {
  reducer,
  INITIAL_STATE,
  loadNextRows,
  refresh,
} from 'src/eventViewer/components/EventViewer.reducer'

// Types
import {LoadRows, EventViewerChildProps} from 'src/eventViewer/types'

interface Props {
  loadRows: LoadRows
  children: (props: EventViewerChildProps) => ReactElement
}

const EventViewer: FC<Props> = ({loadRows, children}) => {
  const [state, dispatch] = useReducer(reducer, INITIAL_STATE)

  useEffect(() => {
    loadNextRows(state, dispatch, loadRows, Date.now())
  }, [])

  useMountedLayoutEffect(() => {
    refresh(state, dispatch, loadRows)
  }, [loadRows])

  return children({state, dispatch, loadRows})
}

export default EventViewer
