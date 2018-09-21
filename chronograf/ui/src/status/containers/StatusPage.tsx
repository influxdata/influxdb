// Libraries
import React, {Component} from 'react'

// Components
import {Page, PageHeader, PageContents} from 'src/page_layout'

// Types
import {Source, Cell} from 'src/types/v2'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  source: Source
  params: {
    sourceID: string
  }
}

@ErrorHandling
class StatusPage extends Component<Props> {
  public render() {
    return (
      <Page>
        <PageHeader fullWidth={true}>
          <PageHeader.Left>
            <h1 className="page--title">Status Page</h1>
          </PageHeader.Left>
          <PageHeader.Right />
        </PageHeader>
        <PageContents fullWidth={true} scrollable={true}>
          <div className="dashboard container-fluid full-width">
            {JSON.stringify(this.cells)}
          </div>
        </PageContents>
      </Page>
    )
  }

  private get cells(): Cell[] {
    return [
      {
        id: 'news-feed',
        viewID: 'news-feed',
        x: 0,
        y: 0,
        w: 8.5,
        h: 10,
        links: {
          self: '',
          view: '',
          copy: '',
        },
      },
      {
        id: 'getting-started',
        viewID: 'getting-started',
        x: 8.5,
        y: 0,
        w: 3.5,
        h: 10,
        links: {
          self: '',
          view: '',
          copy: '',
        },
      },
    ]
  }
}

export default StatusPage
