// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import ClientLibraryOverlay from 'src/clientLibraries/components/ClientLibraryOverlay'
import TemplatedCodeSnippet from 'src/shared/components/TemplatedCodeSnippet'

// Constants
import {clientArduinoLibrary} from 'src/clientLibraries/constants'

// Types
import {AppState} from 'src/types'

// Selectors
import {getOrg} from 'src/organizations/selectors'

interface StateProps {
  org: string
}

type Props = StateProps

const ClientArduinoOverlay: FunctionComponent<Props> = props => {
  const {
    name,
    url,
    installingLibraryManagerCodeSnippet,
    installingManualCodeSnippet,
    initializeClientCodeSnippet,
    writingDataPointCodeSnippet,
    executeQueryCodeSnippet,
  } = clientArduinoLibrary
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
      <h5>Install Library</h5>
      <p>Library Manager</p>
      <TemplatedCodeSnippet
        template={installingLibraryManagerCodeSnippet}
        label="Guide"
      />
      <p>Manual Installation</p>
      <TemplatedCodeSnippet
        template={installingManualCodeSnippet}
        label="Guide"
        defaults={{
          url: 'url',
        }}
        values={{
          url,
        }}
      />
      <h5>Initialize the Client</h5>
      <TemplatedCodeSnippet
        template={initializeClientCodeSnippet}
        label="Arduino Code"
        defaults={{
          server: 'basepath',
          token: 'token',
          org: 'orgID',
          bucket: 'bucketID',
        }}
        values={{
          server,
          org,
        }}
      />
      <h5>Write Data</h5>
      <TemplatedCodeSnippet
        template={writingDataPointCodeSnippet}
        label="Arduino Code"
      />
      <h5>Execute a Flux query</h5>
      <TemplatedCodeSnippet
        template={executeQueryCodeSnippet}
        label="Arduino Code"
      />
    </ClientLibraryOverlay>
  )
}

const mstp = (state: AppState): StateProps => {
  return {
    org: getOrg(state).id,
  }
}

export {ClientArduinoOverlay}
export default connect<StateProps, {}, Props>(mstp, null)(ClientArduinoOverlay)
