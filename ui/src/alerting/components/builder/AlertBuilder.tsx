// Libraries
import React, {FC} from 'react'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import CheckMetaCard from 'src/alerting/components/builder/CheckMetaCard'
import CheckMessageCard from 'src/alerting/components/builder/CheckMessageCard'
import CheckConditionsCard from 'src/alerting/components/builder/CheckConditionsCard'
import CheckMatchingRulesCard from 'src/alerting/components/builder/CheckMatchingRulesCard'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

const AlertBuilder: FC = () => {
  return (
    <div className="query-builder alert-builder" data-testid="query-builder">
      <div className="query-builder--cards">
        <FancyScrollbar>
          <div className="builder-card--list alert-builder--list">
            <CheckMetaCard />
            <CheckMessageCard />
            <CheckConditionsCard />
            {isFlagEnabled('matchingNotificationRules') && (
              <CheckMatchingRulesCard />
            )}
          </div>
        </FancyScrollbar>
      </div>
    </div>
  )
}

export default AlertBuilder
