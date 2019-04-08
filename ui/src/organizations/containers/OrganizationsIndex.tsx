// Libraries
import React, {PureComponent} from 'react'
import {WithRouterProps, withRouter} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {Page} from 'src/pageLayout'
import OrganizationsIndexContents from 'src/organizations/components/OrganizationsIndexContents'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import {Button, IconFont, ComponentColor} from '@influxdata/clockface'

// Actions
import {deleteOrg} from 'src/organizations/actions/orgs'

// Types
import {Organization, Links, AppState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import FilterList from 'src/shared/components/Filter'

interface StateProps {
  links: Links
  orgs: Organization[]
}

interface DispatchProps {
  onDeleteOrg: typeof deleteOrg
}

interface State {
  searchTerm: string
}

type Props = StateProps & DispatchProps & WithRouterProps

@ErrorHandling
class OrganizationsIndex extends PureComponent<Props, State> {
  public state: State = {
    searchTerm: '',
  }

  public render() {
    const {onDeleteOrg, orgs, children} = this.props
    const {searchTerm} = this.state

    return (
      <>
        <Page titleTag="Organizations">
          <Page.Header fullWidth={false}>
            <Page.Header.Left>
              <Page.Title title="Organizations" />
            </Page.Header.Left>
            <Page.Header.Right>
              <SearchWidget
                placeholderText="Filter organizations by name..."
                onSearch={this.handleChangeSearchTerm}
              />
              <Button
                color={ComponentColor.Primary}
                onClick={this.handleOpenModal}
                icon={IconFont.Plus}
                text="Create Organization"
                titleText="Create a new Organization"
                testID="create-org-button"
              />
            </Page.Header.Right>
          </Page.Header>
          <Page.Contents fullWidth={false} scrollable={true}>
            <FilterList<Organization>
              searchKeys={['name']}
              searchTerm={searchTerm}
              list={orgs}
            >
              {filteredOrgs => (
                <OrganizationsIndexContents
                  orgs={filteredOrgs}
                  onDeleteOrg={onDeleteOrg}
                  searchTerm={searchTerm}
                />
              )}
            </FilterList>
          </Page.Contents>
        </Page>
        {children}
      </>
    )
  }

  private handleChangeSearchTerm = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private handleOpenModal = () => {
    this.props.router.push(`${this.props.location.pathname}/new`)
  }
}

const mstp = (state: AppState): StateProps => {
  const {orgs, links} = state

  return {
    orgs: orgs.items,
    links,
  }
}

const mdtp: DispatchProps = {
  onDeleteOrg: deleteOrg,
}

export default withRouter(
  connect<StateProps, DispatchProps>(
    mstp,
    mdtp
  )(OrganizationsIndex)
)
