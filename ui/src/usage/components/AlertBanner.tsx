import React, {FC} from 'react'

// Components
import AlertStatusLimits from './AlertStatusLimits'
import AlertStatusCancelled from './AlertStatusCancelled'

// Types
import {UsageLimitStatus} from 'src/types'

interface Props {
  limitStatuses: UsageLimitStatus
  isOperator: boolean
  accountType: string
  className?: string
}

const AlertBanner: FC<Props> = props => {
  const {
    limitStatuses: {read, write, cardinality},
    isOperator,
    accountType,
    className,
  } = props

  if (accountType === 'cancelled') {
    return <AlertStatusCancelled isOperator={isOperator} />
  } else {
    return (
      <div className={className}>
        <AlertStatusLimits
          statuses={[
            {
              name: 'reads',
              status: read.status,
            },
            {
              name: 'writes',
              status: write.status,
            },
            {
              name: 'cardinality',
              status: cardinality.status,
            },
          ]}
          isOperator={isOperator}
          accountType={accountType}
        />
      </div>
    )
  }
}

export default AlertBanner
