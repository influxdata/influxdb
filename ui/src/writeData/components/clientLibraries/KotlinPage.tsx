// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import ClientLibraryPageTemplate from 'src/clientLibraries/components/ClientLibraryPageTemplate'
import TemplatedCodeSnippet from 'src/shared/components/TemplatedCodeSnippet'

// Constants
import {clientKotlinLibrary} from 'src/clientLibraries/constants'

// Types
import {AppState} from 'src/types'

// Selectors
import {getOrg} from 'src/organizations/selectors'

interface StateProps {
  org: string
}

type Props = StateProps

const ClientKotlinPage: FunctionComponent<Props> = props => {
  const {
    name,
    url,
    buildWithMavenCodeSnippet,
    buildWithGradleCodeSnippet,
    initializeClientCodeSnippet,
    executeQueryCodeSnippet,
  } = clientKotlinLibrary
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
      <h5>Add Dependency</h5>
      <p>Build with Maven</p>
      <TemplatedCodeSnippet template={buildWithMavenCodeSnippet} label="Code" />
      <p>Build with Gradle</p>
      <TemplatedCodeSnippet
        template={buildWithGradleCodeSnippet}
        label="Code"
      />
      <h5>Initialize the Client</h5>
      <TemplatedCodeSnippet
        template={initializeClientCodeSnippet}
        label="Kotlin Code"
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
      <h5>Execute a Flux query</h5>
      <TemplatedCodeSnippet
        template={executeQueryCodeSnippet}
        label="Kotlin Code"
      />
    </ClientLibraryPageTemplate>
  )
}

const mstp = (state: AppState) => {
  return {
    org: getOrg(state).id,
  }
}

export {ClientKotlinPage}
export default connect<StateProps, {}, Props>(mstp, null)(ClientKotlinPage)
