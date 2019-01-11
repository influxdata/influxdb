// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Styles
import 'src/me/containers/MePage.scss'

// Components
import {Page} from 'src/pageLayout'
import Resources from 'src/me/components/Resources'
import Header from 'src/me/components/UserPageHeader'
import Docs from 'src/me/components/Docs'

// Types
import {MeState, AppState} from 'src/types/v2'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

import {Panel} from 'src/clockface'

interface StateProps {
  me: MeState
}

@ErrorHandling
export class MePage extends PureComponent<StateProps> {
  public render() {
    const {me} = this.props

    return (
      <Page className="user-page" titleTag="My Account">
        <Header title={`Howdy, ${me.name}!`} />
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-8">
            <Panel>
              <Panel.Header title="Getting Started" />
              <Panel.Body>
                <span>Put Getting Started Stuff Here</span>
              </Panel.Body>
            </Panel>
            <Docs />
          </div>
          <div className="col-xs-4">
            <Resources me={me} />
          </div>
        </Page.Contents>
      </Page>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const {me} = state

  return {me}
}

export default connect<StateProps>(
  mstp,
  null
)(MePage)
