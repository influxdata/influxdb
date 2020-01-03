// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Components
import FilterList from 'src/shared/components/Filter'
import TemplatesList from 'src/templates/components/TemplatesList'
import StaticTemplatesList from 'src/templates/components/StaticTemplatesList'
import {ErrorHandling} from 'src/shared/decorators/errors'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import GetResources from 'src/shared/components/GetResources'
import SettingsTabbedPageHeader from 'src/settings/components/SettingsTabbedPageHeader'

// Types
import {TemplateSummary, AppState, ResourceType} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'
import {
  Sort,
  Button,
  ComponentColor,
  IconFont,
  SelectGroup,
  FlexBox,
  FlexDirection,
  ComponentSize,
} from '@influxdata/clockface'

import {staticTemplates as statics} from 'src/templates/constants/defaultTemplates'

interface StaticTemplate {
  name: string
  template: TemplateSummary
}

const staticTemplates: StaticTemplate[] = _.map(statics, (template, name) => ({
  name,
  template,
}))

interface OwnProps {
  onImport: () => void
}

interface StateProps {
  templates: TemplateSummary[]
}

type Props = OwnProps & StateProps

interface State {
  searchTerm: string
  sortKey: SortKey
  sortDirection: Sort
  sortType: SortTypes
  activeTab: string
}

type SortKey = 'meta.name'

@ErrorHandling
class TemplatesPage extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      searchTerm: '',
      sortKey: 'meta.name',
      sortDirection: Sort.Ascending,
      sortType: SortTypes.String,
      activeTab: 'static-templates',
    }
  }

  public render() {
    const {onImport} = this.props
    const {activeTab} = this.state

    return (
      <>
        <SettingsTabbedPageHeader>
          <FlexBox direction={FlexDirection.Row} margin={ComponentSize.Small}>
            {this.filterComponent}
            <SelectGroup>
              <SelectGroup.Option
                name="template-type"
                id="static-templates"
                active={activeTab === 'static-templates'}
                value="static-templates"
                onClick={this.handleClickTab}
                titleText="Static Templates"
              >
                Static Templates
              </SelectGroup.Option>
              <SelectGroup.Option
                name="template-type"
                id="user-templates"
                active={activeTab === 'user-templates'}
                value="user-templates"
                onClick={this.handleClickTab}
                titleText="User Templates"
              >
                User Templates
              </SelectGroup.Option>
            </SelectGroup>
          </FlexBox>
          <Button
            text="Import Template"
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
            onClick={onImport}
          />
        </SettingsTabbedPageHeader>
        {this.templatesList}
      </>
    )
  }

  private handleClickTab = val => {
    this.setState({activeTab: val})
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    const sortType = SortTypes.String
    this.setState({sortKey, sortDirection: nextSort, sortType})
  }

  private get templatesList(): JSX.Element {
    const {templates, onImport} = this.props
    const {searchTerm, sortKey, sortDirection, sortType, activeTab} = this.state

    if (activeTab === 'static-templates') {
      return (
        <FilterList<StaticTemplate>
          searchTerm={searchTerm}
          searchKeys={['template.meta.name', 'labels[].name']}
          list={staticTemplates}
        >
          {ts => {
            return (
              <StaticTemplatesList
                searchTerm={searchTerm}
                templates={ts}
                onFilterChange={this.setSearchTerm}
                onImport={onImport}
                sortKey={sortKey}
                sortDirection={sortDirection}
                sortType={sortType}
                onClickColumn={this.handleClickColumn}
              />
            )
          }}
        </FilterList>
      )
    }

    if (activeTab === 'user-templates') {
      return (
        <GetResources resources={[ResourceType.Labels]}>
          <FilterList<TemplateSummary>
            searchTerm={searchTerm}
            searchKeys={['meta.name', 'labels[].name']}
            list={templates}
          >
            {ts => {
              return (
                <TemplatesList
                  searchTerm={searchTerm}
                  templates={ts}
                  onFilterChange={this.setSearchTerm}
                  onImport={onImport}
                  sortKey={sortKey}
                  sortDirection={sortDirection}
                  sortType={sortType}
                  onClickColumn={this.handleClickColumn}
                />
              )
            }}
          </FilterList>
        </GetResources>
      )
    }
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
