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
import {State} from 'src/eventViewer/components/EventViewer.reducer'

interface Props {
  loadRows: LoadRows
  children: (props: EventViewerChildProps) => ReactElement
  initialState?: Partial<State>
}

const EventViewer: FC<Props> = ({loadRows, children, initialState}) => {
  const [state, dispatch] = useReducer(reducer, {
    ...INITIAL_STATE,
    ...initialState,
  })

  useEffect(() => {
    loadNextRows(state, dispatch, loadRows, Date.now())
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  useMountedLayoutEffect(() => {
    refresh(state, dispatch, loadRows)
  }, [loadRows])

  return children({
    state,
    dispatch,
    loadRows,
  })
}

export default EventViewer
