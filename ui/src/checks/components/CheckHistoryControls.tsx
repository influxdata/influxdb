// Libraries
import React, {FC} from 'react'

// Components
import SearchBar from 'src/alerting/components/SearchBar'

// Types
import {EventViewerChildProps} from 'src/eventViewer/types'

// Constants
import {EXAMPLE_STATUS_SEARCHES} from 'src/alerting/constants/history'

interface Props {
  eventViewerProps: EventViewerChildProps
}

const CheckHistoryControls: FC<Props> = ({eventViewerProps}) => {
  return (
    <SearchBar
      {...eventViewerProps}
      placeholder="Search statuses..."
      exampleSearches={EXAMPLE_STATUS_SEARCHES}
    />
  )
}

export default CheckHistoryControls
