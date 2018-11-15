// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {Page} from 'src/pageLayout'
import CreateOrgOverlay from 'src/organizations/components/CreateOrgOverlay'
import OrganizationsIndexContents from 'src/organizations/components/OrganizationsIndexContents'
import {
  Button,
  IconFont,
  ComponentColor,
  OverlayTechnology,
  Input,
} from 'src/clockface'

// Actions
import {createOrg, deleteOrg} from 'src/organizations/actions'

// Types
import {Organization, Links} from 'src/types/v2'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface StateProps {
  links: Links
  orgs: Organization[]
}

interface DispatchProps {
  onCreateOrg: typeof createOrg
  onDeleteOrg: typeof deleteOrg
}

interface State {
  modalState: ModalState
  searchTerm: string
}

enum ModalState {
  Open = 'open',
  Closed = 'closed',
}

type Props = StateProps & DispatchProps & WithRouterProps

@ErrorHandling
class OrganizationsIndex extends PureComponent<Props, State> {
  public state: State = {
    modalState: ModalState.Closed,
    searchTerm: '',
  }

  public render() {
    const {links, onCreateOrg, onDeleteOrg} = this.props
    const {modalState, searchTerm} = this.state

    return (
      <>
        <Page>
          <Page.Header fullWidth={false}>
            <Page.Header.Left>
              <Page.Title title="Organizations" />
            </Page.Header.Left>
            <Page.Header.Right>
              <Input
                value={searchTerm}
                placeholder="Filter organizations by name..."
                onChange={this.handleChangeSearchTerm}
              />

              <Button
                color={ComponentColor.Primary}
                onClick={this.handleOpenModal}
                icon={IconFont.Plus}
                text="Create Organization"
                titleText="Create a new Organization"
              />
            </Page.Header.Right>
          </Page.Header>
          <Page.Contents fullWidth={false} scrollable={true}>
            <OrganizationsIndexContents
              orgs={this.filteredOrgs}
              onDeleteOrg={onDeleteOrg}
            />
          </Page.Contents>
        </Page>
        <OverlayTechnology visible={modalState === ModalState.Open}>
          <CreateOrgOverlay
            link={links.orgs}
            onCloseModal={this.handleCloseModal}
            onCreateOrg={onCreateOrg}
          />
        </OverlayTechnology>
      </>
    )
  }

  private get filteredOrgs(): Organization[] {
    const {orgs} = this.props
    const {searchTerm} = this.state

    const filteredOrgs = orgs.filter(org =>
      org.name.toLowerCase().includes(searchTerm.toLowerCase())
    )

    return filteredOrgs
  }

  private handleOpenModal = (): void => {
    this.setState({modalState: ModalState.Open})
  }

  private handleCloseModal = (): void => {
    this.setState({modalState: ModalState.Closed})
  }

  private handleChangeSearchTerm = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }
}

const mstp = (state): StateProps => {
  const {orgs, links} = state

  return {
    orgs,
    links,
  }
}

const mdtp: DispatchProps = {
  onCreateOrg: createOrg,
  onDeleteOrg: deleteOrg,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(OrganizationsIndex)
