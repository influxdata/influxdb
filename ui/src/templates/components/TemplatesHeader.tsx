// Libraries
import React, {PureComponent} from 'react'
import {Page} from 'src/pageLayout'

// Components
import {Tabs, ComponentSpacer, Alignment, Stack} from 'src/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'

import 'src/tasks/components/TasksPage.scss'

interface Props {
  onCreateTemplate: () => void
  onImportTemplate: () => void
  showOrgDropdown?: boolean
  isFullPage?: boolean
  filterComponent: () => JSX.Element
}

export default class TemplatesHeader extends PureComponent<Props> {
  public static defaultProps: {
    showOrgDropdown: boolean
    isFullPage: boolean
  } = {
    showOrgDropdown: true,
    isFullPage: true,
  }

  public render() {
    const {
      onCreateTemplate,
      onImportTemplate,
      isFullPage,
      filterComponent,
    } = this.props

    if (isFullPage) {
      return (
        <Page.Header fullWidth={false}>
          <Page.Header.Left>
            <Page.Title title={this.pageTitle} />
          </Page.Header.Left>
          <Page.Header.Right>
            <AddResourceDropdown
              onSelectNew={onCreateTemplate}
              onSelectImport={onImportTemplate}
              resourceName="Template"
            />
          </Page.Header.Right>
        </Page.Header>
      )
    }

    return (
      <Tabs.TabContentsHeader>
        {filterComponent()}
        <ComponentSpacer align={Alignment.Right} stackChildren={Stack.Columns}>
          <AddResourceDropdown
            onSelectNew={onCreateTemplate}
            onSelectImport={onImportTemplate}
            resourceName="Template"
          />
        </ComponentSpacer>
      </Tabs.TabContentsHeader>
    )
  }

  private get pageTitle() {
    const {showOrgDropdown} = this.props

    if (showOrgDropdown) {
      return 'Templates'
    }

    return ''
  }
}
