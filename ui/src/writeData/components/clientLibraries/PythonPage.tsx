// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import ClientLibraryPage from 'src/writeData/components/clientLibraries/ClientLibraryPage'
import TemplatedCodeSnippet from 'src/shared/components/TemplatedCodeSnippet'

// Constants
import {clientPythonLibrary} from 'src/clientLibraries/constants'

// Selectors
import {getOrg} from 'src/organizations/selectors'

// Types
import {AppState} from 'src/types'

interface StateProps {
  org: string
}

type Props = StateProps

const ClientPythonPage: FunctionComponent<Props> = props => {
  const {
    name,
    url,
    initializePackageCodeSnippet,
    initializeClientCodeSnippet,
    executeQueryCodeSnippet,
    writingDataLineProtocolCodeSnippet,
    writingDataPointCodeSnippet,
    writingDataBatchCodeSnippet,
  } = clientPythonLibrary
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
      <h5>Install Package</h5>
      <TemplatedCodeSnippet
        template={initializePackageCodeSnippet}
        label="Code"
      />
      <h5>Initialize the Client</h5>
      <TemplatedCodeSnippet
        template={initializeClientCodeSnippet}
        label="Python Code"
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
        label="Python Code"
      />
      <p>Option 2: Use a Data Point to write data</p>
      <TemplatedCodeSnippet
        template={writingDataPointCodeSnippet}
        label="Python Code"
      />
      <p>Option 3: Use a Batch Sequence to write data</p>
      <TemplatedCodeSnippet
        template={writingDataBatchCodeSnippet}
        label="Python Code"
      />
      <h5>Execute a Flux query</h5>
      <TemplatedCodeSnippet
        template={executeQueryCodeSnippet}
        label="Python Code"
      />
    </ClientLibraryPage>
  )
}

const mstp = (state: AppState) => {
  const {id} = getOrg(state)

  return {
    org: id,
  }
}

export {ClientPythonPage}
export default connect<StateProps, {}, Props>(mstp, null)(ClientPythonPage)
