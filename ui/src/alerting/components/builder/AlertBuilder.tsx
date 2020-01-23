// Libraries
import React, {FC} from 'react'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import CheckMetaCard from 'src/checks/components/CheckMetaCard'
import CheckMessageCard from 'src/checks/components/CheckMessageCard'
import CheckConditionsCard from 'src/checks/components/CheckConditionsCard'
import CheckMatchingRulesCard from 'src/checks/components/CheckMatchingRulesCard'
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
