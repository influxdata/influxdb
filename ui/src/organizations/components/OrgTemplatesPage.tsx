// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import FilterList from 'src/shared/components/Filter'
import TemplatesHeader from 'src/templates/components/TemplatesHeader'
import OrgTemplatesList from 'src/organizations/components/OrgTemplatesList'
import {ErrorHandling} from 'src/shared/decorators/errors'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'

// Types
import {TemplateSummary} from '@influxdata/influx'

interface Props {
  templates: TemplateSummary[]
  orgName: string
  orgID: string
  onImport: () => void
}

interface State {
  searchTerm: string
}

@ErrorHandling
class OrgTemplatesPage extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    const {templates, onImport} = this.props
    const {searchTerm} = this.state

    return (
      <>
        <TemplatesHeader
          onCreateTemplate={onImport}
          onImportTemplate={onImport}
          showOrgDropdown={false}
          isFullPage={false}
          filterComponent={() => this.filterComponent}
        />
        <FilterList<TemplateSummary>
          searchTerm={searchTerm}
          searchKeys={['meta.name', 'labels[].name']}
          list={templates}
        >
          {ts => (
            <OrgTemplatesList
              searchTerm={searchTerm}
              templates={ts}
              onFilterChange={this.setSearchTerm}
              onImport={onImport}
            />
          )}
        </FilterList>
      </>
    )
  }

  private get filterComponent(): JSX.Element {
    const {searchTerm} = this.state

    return (
      <SearchWidget
        placeholderText="Filter templates..."
        onSearch={this.setSearchTerm}
        searchTerm={searchTerm}
      />
    )
  }

  private setSearchTerm = (searchTerm: string) => {
    this.setState({searchTerm})
  }
}

export default OrgTemplatesPage
