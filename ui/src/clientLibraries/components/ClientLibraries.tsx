// Libraries
import _ from 'lodash'
import React, {FunctionComponent} from 'react'

// Components
import {Grid, SelectableCard} from '@influxdata/clockface'
import {ResponsiveGridSizer} from 'src/clockface'
import OverlayLink from 'src/overlays/components/OverlayLink'

// Mocks
import {clientLibraries} from 'src/clientLibraries/constants'

interface Props {}

const ClientLibraries: FunctionComponent<Props> = () => {
  const clientLibrariesCount = clientLibraries.length

  return (
    <Grid>
      <Grid.Row>
        <Grid.Column>
          <ResponsiveGridSizer columns={clientLibrariesCount}>
            {clientLibraries.map(cl => {
              return (
                <OverlayLink overlayID={`${cl.id}-client`}>
                  {onClick => (
                    <SelectableCard
                      key={cl.id}
                      id={cl.id}
                      formName="client-libraries-cards"
                      label={cl.name}
                      testID={`client-libraries-cards--${cl.id}`}
                      selected={false}
                      onClick={onClick}
                      image={<img src={cl.logoUrl} />}
                    />
                  )}
                </OverlayLink>
              )
            })}
          </ResponsiveGridSizer>
        </Grid.Column>
      </Grid.Row>
    </Grid>
  )
}

export default ClientLibraries
