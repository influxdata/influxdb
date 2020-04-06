import React, {Component} from 'react'

import AlertStatusLimits from './AlertStatusLimtis'
import AlertStatusCancelled from './AlertStatusCancelled'

class AlertBanner extends Component {
  render() {
    return this.alertStatus()
  }

  alertStatus() {
    const {
      limitStatuses: {read, write, cardinality},
      isOperator,
      accountType,
      className,
    } = this.props

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
}

export default AlertBanner
