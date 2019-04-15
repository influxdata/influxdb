// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Components
import FilterList from 'src/shared/components/Filter'
import TemplatesHeader from 'src/templates/components/TemplatesHeader'
import TemplatesList from 'src/templates/components/TemplatesList'
import {ErrorHandling} from 'src/shared/decorators/errors'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import GetLabels from 'src/labels/components/GetLabels'

// Types
import {TemplateSummary, AppState} from 'src/types'

interface OwnProps {
  onImport: () => void
}

interface StateProps {
  templates: TemplateSummary[]
}

type Props = OwnProps & StateProps

interface State {
  searchTerm: string
}

@ErrorHandling
class TemplatesPage extends PureComponent<Props, State> {
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
          onImportTemplate={onImport}
          showOrgDropdown={false}
          isFullPage={false}
          filterComponent={() => this.filterComponent}
        />
        <GetLabels>
          <FilterList<TemplateSummary>
            searchTerm={searchTerm}
            searchKeys={['meta.name', 'labels[].name']}
            list={templates}
          >
            {ts => (
              <TemplatesList
                searchTerm={searchTerm}
                templates={ts}
                onFilterChange={this.setSearchTerm}
                onImport={onImport}
              />
            )}
          </FilterList>
        </GetLabels>
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
const mstp = ({templates}: AppState): StateProps => ({
  templates: templates.items,
})

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(TemplatesPage)
