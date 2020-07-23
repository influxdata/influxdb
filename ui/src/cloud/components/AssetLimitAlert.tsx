// Libraries
import React, {FC} from 'react'

// Components
import {
  FlexBox,
  JustifyContent,
  Gradients,
  InfluxColors,
  GradientBox,
  Panel,
  Heading,
  HeadingElement,
  AlignItems,
} from '@influxdata/clockface'
import CloudUpgradeButton from 'src/shared/components/CloudUpgradeButton'

// Constants
import {CLOUD} from 'src/shared/constants'

// Types
import {LimitStatus} from 'src/cloud/actions/limits'

interface Props {
  limitStatus: LimitStatus
  resourceName: string
  className?: string
}

const AssetLimitAlert: FC<Props> = ({limitStatus, resourceName, className}) => {
  if (CLOUD && limitStatus === LimitStatus.EXCEEDED) {
    return (
      <GradientBox
        borderGradient={Gradients.MiyazakiSky}
        borderColor={InfluxColors.Raven}
        className={className}
      >
        <Panel
          backgroundColor={InfluxColors.Raven}
          className="asset-alert"
        >
          <Panel.Header>
            <Heading element={HeadingElement.H4}>
              Need more {resourceName}?
            </Heading>
          </Panel.Header>
          <Panel.Body
            className="asset-alert--contents"
          >
            <FlexBox
              justifyContent={JustifyContent.FlexEnd}
              alignItems={AlignItems.FlexEnd}
              stretchToFitHeight={true}
            >
              <CloudUpgradeButton buttonText={`Get more ${resourceName}`} />
            </FlexBox>
          </Panel.Body>
        </Panel>
      </GradientBox>

    )
  }

  return null
}


export default AssetLimitAlert