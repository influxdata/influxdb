// Libraries
import React, {Component} from 'react'

// Components
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'

// Types
import {Source, Cell} from 'src/types/v2'

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
      <div className="page">
        <PageHeader
          titleText="Status"
          fullWidth={true}
          sourceIndicator={true}
        />
        <FancyScrollbar className="page-contents">
          <div className="dashboard container-fluid full-width">
            {JSON.stringify(this.cells)}
          </div>
        </FancyScrollbar>
      </div>
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
