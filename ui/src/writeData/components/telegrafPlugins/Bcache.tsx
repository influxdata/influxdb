// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TelegrafPluginPage from 'src/writeData/components/pageTemplates/TelegrafPluginPage'

// Types
import {AppState} from 'src/types'

// Selectors
import {getOrg} from 'src/organizations/selectors'

interface StateProps {
  org: string
}

type Props = StateProps

const Bcache: FunctionComponent<Props> = () => {
  return (
    <TelegrafPluginPage title="Bcache">
      <p>Docs go here</p>
    </TelegrafPluginPage>
  )
}

const mstp = (state: AppState) => {
  return {
    org: getOrg(state).id,
  }
}

export {Bcache}
export default connect<StateProps, {}, Props>(mstp, null)(Bcache)
