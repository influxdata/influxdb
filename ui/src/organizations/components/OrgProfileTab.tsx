// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {
  Button,
  ComponentSize,
  Panel,
  IconFont,
  FlexBox,
  AlignItems,
  FlexDirection,
  Gradients,
  InfluxColors,
  JustifyContent,
} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'
import OverlayLink from 'src/overlays/components/OverlayLink'

type Props = {}

@ErrorHandling
class OrgProfileTab extends PureComponent<Props> {
  public render() {
    return (
      <Panel size={ComponentSize.Small} backgroundColor={InfluxColors.Onyx}>
        <Panel.Header>
          <Panel.Title>Organization Profile</Panel.Title>
        </Panel.Header>
        <Panel.Body>
            <Panel
              gradient={Gradients.DocScott}
              size={ComponentSize.ExtraSmall}
            >
              <Panel.Header>
                <Panel.Title>Danger Zone!</Panel.Title>
              </Panel.Header>
              <Panel.Body>
                <FlexBox
                  stretchToFitWidth={true}
                  alignItems={AlignItems.Center}
                  direction={FlexDirection.Row}
                  justifyContent={JustifyContent.SpaceBetween}
                >
                  <div>
                    <h5 style={{marginBottom: '0'}}>Rename Organization</h5>
                    <p style={{marginTop: '2px'}}>
                      This action can have wide-reaching unintended
                      consequences.
                    </p>
                  </div>
                  <OverlayLink overlayID="rename-organization">
                    {onClick => (
                      <Button
                        text="Rename"
                        icon={IconFont.Pencil}
                        onClick={onClick}
                      />
                    )}
                  </OverlayLink>
                </FlexBox>
              </Panel.Body>
            </Panel>
        </Panel.Body>
      </Panel>
    )
  }
}

export default OrgProfileTab
