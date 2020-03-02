// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
import TemplatedCodeSnippet from 'src/shared/components/TemplatedCodeSnippet'

// Constants
import {clientGoLibrary} from 'src/clientLibraries/constants'

// Types
import {AppState} from 'src/types'

// Selectors
import {getOrg} from 'src/organizations/selectors'

interface StateProps {
  org: string
}

type Props = StateProps

const ClientGoOverlay: FunctionComponent<Props> = props => {
  const {
    name,
    url,
    initializeClientCodeSnippet,
    writeDataCodeSnippet,
  } = clientGoLibrary
  const {org} = props
  const server = window.location.origin

  return (
    <ClientLibraryOverlay title={`${name} Client Library`}>
      <p>
        For more detailed and up to date information check out the{' '}
        <a href={url} target="_blank">
          GitHub Repository
        </a>
      </p>
      <h5>Initialize the Client</h5>
      <TemplatedCodeSnippet
        template={initializeClientCodeSnippet}
        label="Go Code"
        defaults={{
          token: 'myToken',
          server: 'myHTTPInfluxAddress',
        }}
        values={{
          server,
        }}
      />
      <h5>Write Data</h5>
      <TemplatedCodeSnippet
        template={writeDataCodeSnippet}
        label="Go Code"
        defaults={{
          bucket: 'my-awesome-bucket',
          org: 'my-very-awesome-org',
        }}
        values={{
          org,
        }}
      />
    </ClientLibraryOverlay>
  )
}

const mstp = (state: AppState): StateProps => {
  return {
    org: getOrg(state).id,
  }
}

export {ClientGoOverlay}
export default connect<StateProps, {}, Props>(
  mstp,
  null
)(ClientGoOverlay)
