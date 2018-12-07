// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Panel, Button, Spinner} from 'src/clockface'
import ResourceFetcher from 'src/shared/components/resource_fetcher'
import TokenList from 'src/me/components/account/TokensList'

// APIs
import {getAuthorizations} from 'src/authorizations/apis'

// Types
import {Authorization} from 'src/api'
import {AppState} from 'src/types/v2'

interface StateProps {
  authorizationsLink: string
}

export class Tokens extends PureComponent<StateProps> {
  public render() {
    const {authorizationsLink} = this.props

    return (
      <Panel>
        <Panel.Header title="Tokens">
          <Button text="Create Token" />
        </Panel.Header>
        <Panel.Body>
          <ResourceFetcher<Authorization[]>
            link={authorizationsLink}
            fetcher={getAuthorizations}
          >
            {(auths, loading) => (
              <Spinner loading={loading}>
                <TokenList auths={auths} />
              </Spinner>
            )}
          </ResourceFetcher>
        </Panel.Body>
      </Panel>
    )
  }
}

const mstp = ({links}: AppState) => {
  return {authorizationsLink: links.authorizations}
}

export default connect<StateProps>(mstp)(Tokens)
