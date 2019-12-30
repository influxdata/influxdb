// Libraries
import {useEffect, FC} from 'react'

// Utils
import {updateQueryParams} from 'src/shared/utils/queryParams'

// Constants
import {
  HISTORY_TYPE_QUERY_PARAM,
  SEARCH_QUERY_PARAM,
} from 'src/alerting/constants/history'

// Types
import {AlertHistoryType} from 'src/types'

interface Props {
  searchInput: string
  historyType: AlertHistoryType
}

const AlertHistoryQueryParams: FC<Props> = ({searchInput, historyType}) => {
  useEffect(() => {
    updateQueryParams({
      [SEARCH_QUERY_PARAM]: searchInput || null,
      [HISTORY_TYPE_QUERY_PARAM]: historyType || null,
    })
  }, [searchInput, historyType])

  return null
}

export default AlertHistoryQueryParams
