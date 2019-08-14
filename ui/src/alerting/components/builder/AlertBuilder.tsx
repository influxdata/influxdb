// Libraries
import React, {FC} from 'react'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'
import CheckMetaCard from 'src/alerting/components/builder/CheckMetaCard'
import CheckConditionsCard from 'src/alerting/components/builder/CheckConditionsCard'
import CheckMatchingRulesCard from 'src/alerting/components/builder/CheckMatchingRulesCard'

const AlertBuilder: FC = () => {
  return (
    <div className="query-builder" data-testid="query-builder">
      <div className="query-builder--cards">
        <FancyScrollbar>
          <div className="builder-card--list">
            <BuilderCard
              testID="builder-meta"
              widthPixels={340}
              className="alert-builder--card"
            >
              <BuilderCard.Header title="Meta" />
              <BuilderCard.Body addPadding={true} autoHideScrollbars={true}>
                <CheckMetaCard />
              </BuilderCard.Body>
            </BuilderCard>
            <BuilderCard
              testID="builder-conditions"
              widthPixels={510}
              className="alert-builder--card"
            >
              <BuilderCard.Header title="Conditions" />
              <BuilderCard.Body addPadding={true}>
                <CheckConditionsCard />
              </BuilderCard.Body>
            </BuilderCard>
            <BuilderCard
              testID="builder-matching-rules"
              widthPixels={420}
              className="alert-builder--card"
            >
              <BuilderCard.Header title="Matching Notification Rules" />
              <BuilderCard.Body addPadding={true}>
                <CheckMatchingRulesCard />
              </BuilderCard.Body>
            </BuilderCard>
          </div>
        </FancyScrollbar>
      </div>
    </div>
  )
}

export default AlertBuilder
