// Libraries
import _ from 'lodash'
import React, {FunctionComponent, createElement} from 'react'
import {withRouter, WithRouterProps, Link} from 'react-router-dom'

// Components
import {
  Grid,
  SelectableCard,
  SquareGrid,
  ComponentSize,
} from '@influxdata/clockface'
import CodeSnippet from 'src/shared/components/CodeSnippet'

// Mocks
import {clientLibraries} from 'src/clientLibraries/constants'

interface OwnProps {
  orgID: string
}

type Props = OwnProps & WithRouterProps

const ClientLibraries: FunctionComponent<Props> = ({orgID, router}) => {
  return (
    <Grid>
      <Grid.Row>
        <Grid.Column>
          <p>
            Use the following URL when initializing each Client Library. The
            Token can be generated on the
            <Link to={`/orgs/${orgID}/load-data/tokens`}>&nbsp;Tokens tab</Link>
            .
          </p>
          <CodeSnippet copyText={window.location.origin} label="Client URL" />
        </Grid.Column>
      </Grid.Row>
      <Grid.Row>
        <Grid.Column>
          <SquareGrid cardSize="200px" gutter={ComponentSize.Small}>
            {clientLibraries.map(cl => {
              const handleClick = (): void => {
                router.push(
                  `/orgs/${orgID}/load-data/client-libraries/${cl.id}`
                )
              }

              return (
                <SquareGrid.Card key={cl.id}>
                  <SelectableCard
                    id={cl.id}
                    formName="client-libraries-cards"
                    label={cl.name}
                    testID={`client-libraries-cards--${cl.id}`}
                    selected={false}
                    onClick={handleClick}
                  >
                    {createElement(cl.image)}
                  </SelectableCard>
                </SquareGrid.Card>
              )
            })}
          </SquareGrid>
        </Grid.Column>
      </Grid.Row>
    </Grid>
  )
}

export default withRouter<OwnProps>(ClientLibraries)
