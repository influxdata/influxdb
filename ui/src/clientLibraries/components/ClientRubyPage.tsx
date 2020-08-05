// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import ClientLibraryPageTemplate from 'src/clientLibraries/components/ClientLibraryPageTemplate'
import TemplatedCodeSnippet from 'src/shared/components/TemplatedCodeSnippet'

// Constants
import {clientRubyLibrary} from 'src/clientLibraries/constants'

// Selectors
import {getOrg} from 'src/organizations/selectors'

// Types
import {AppState} from 'src/types'

interface StateProps {
  org: string
}

type Props = StateProps

const ClientRubyOverlay: FunctionComponent<Props> = props => {
  const {
    name,
    url,
    initializeGemCodeSnippet,
    initializeClientCodeSnippet,
    executeQueryCodeSnippet,
    writingDataLineProtocolCodeSnippet,
    writingDataPointCodeSnippet,
    writingDataHashCodeSnippet,
    writingDataBatchCodeSnippet,
  } = clientRubyLibrary
  const {org} = props
  const server = window.location.origin

  return (
    <ClientLibraryPageTemplate title={`${name} Client Library`}>
      <p>
        For more detailed and up to date information check out the{' '}
        <a href={url} target="_blank">
          GitHub Repository
        </a>
      </p>
      <h5>Install the Gem</h5>
      <TemplatedCodeSnippet template={initializeGemCodeSnippet} label="Code" />
      <h5>Initialize the Client</h5>
      <TemplatedCodeSnippet
        template={initializeClientCodeSnippet}
        label="Ruby Code"
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
        label="Ruby Code"
      />
      <p>Option 2: Use a Data Point to write data</p>
      <TemplatedCodeSnippet
        template={writingDataPointCodeSnippet}
        label="Ruby Code"
      />
      <p>Option 3: Use a Hash to write data</p>
      <TemplatedCodeSnippet
        template={writingDataHashCodeSnippet}
        label="Ruby Code"
      />
      <p>Option 4: Use a Batch Sequence to write data</p>
      <TemplatedCodeSnippet
        template={writingDataBatchCodeSnippet}
        label="Ruby Code"
      />
      <h5>Execute a Flux query</h5>
      <TemplatedCodeSnippet
        template={executeQueryCodeSnippet}
        label="Ruby Code"
      />
    </ClientLibraryPageTemplate>
  )
}

const mstp = (state: AppState) => {
  const {id} = getOrg(state)

  return {
    org: id,
  }
}

export {ClientRubyOverlay}
export default connect<StateProps, {}, Props>(mstp, null)(ClientRubyOverlay)
