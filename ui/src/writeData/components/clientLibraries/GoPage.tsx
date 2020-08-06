// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import ClientLibraryPage from 'src/writeData/components/clientLibraries/ClientLibraryPage'
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

const ClientGoPage: FunctionComponent<Props> = props => {
  const {
    name,
    url,
    initializeClientCodeSnippet,
    writingDataLineProtocolCodeSnippet,
    writingDataPointCodeSnippet,
    executeQueryCodeSnippet,
  } = clientGoLibrary
  const {org} = props
  const server = window.location.origin

  return (
    <ClientLibraryPage title={`${name} Client Library`}>
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
      <p>Option 1: Use InfluxDB Line Protocol to write data</p>
      <TemplatedCodeSnippet
        template={writingDataLineProtocolCodeSnippet}
        label="Go Code"
      />
      <p>Option 2: Use a Data Point to write data</p>
      <TemplatedCodeSnippet
        template={writingDataPointCodeSnippet}
        label="Go Code"
      />
      <h5>Execute a Flux query</h5>
      <TemplatedCodeSnippet
        template={executeQueryCodeSnippet}
        label="Go Code"
      />
    </ClientLibraryPage>
  )
}

const mstp = (state: AppState) => {
  return {
    org: getOrg(state).id,
  }
}

export {ClientGoPage}
export default connect<StateProps, {}, Props>(mstp, null)(ClientGoPage)
