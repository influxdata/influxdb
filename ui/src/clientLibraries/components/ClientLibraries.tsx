// Libraries
import _ from 'lodash'
import React, {FunctionComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Grid, SelectableCard} from '@influxdata/clockface'
import {ResponsiveGridSizer} from 'src/clockface'

// Mocks
import {clientLibraries} from 'src/clientLibraries/constants'

interface OwnProps {
  orgID: string
}

type Props = OwnProps & WithRouterProps

const ClientLibraries: FunctionComponent<Props> = ({orgID, router}) => {
  const clientLibrariesCount = clientLibraries.length

  return (
    <Grid>
      <Grid.Row>
        <Grid.Column>
          <ResponsiveGridSizer columns={clientLibrariesCount}>
            {clientLibraries.map(cl => {
              const handleClick = (): void => {
                router.push(
                  `/orgs/${orgID}/load-data/client-libraries/${cl.id}`
                )
              }

              return (
                <SelectableCard
                  key={cl.id}
                  id={cl.id}
                  formName="client-libraries-cards"
                  label={cl.name}
                  testID={`client-libraries-cards--${cl.id}`}
                  selected={false}
                  onClick={handleClick}
                  image={<img src={cl.logoUrl} />}
                />
              )
            })}
          </ResponsiveGridSizer>
        </Grid.Column>
      </Grid.Row>
    </Grid>
  )
}

export default withRouter<OwnProps>(ClientLibraries)
