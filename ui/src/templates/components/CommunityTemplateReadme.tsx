// Libraries
import React, {Component} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {MarkdownRenderer} from 'src/shared/components/views/MarkdownRenderer'

import {AppState} from 'src/types'
import {getTemplateNameFromUrl} from 'src/templates/utils'
import {fetchAndSetReadme} from 'src/templates/actions/thunks'

interface OwnProps {
  url: string
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

const cloudImageRenderer =
  "We don't support images in markdown for security purposes"

class CommunityTemplateReadmeUnconnected extends Component<Props> {
  componentDidMount = () => {
    if (!this.props.readme) {
      this.props.fetchAndSetReadme(this.props.name, this.props.directory)
    }
  }

  render = () => {
    const {readme} = this.props

    if (!readme) {
      return null
    }

    return (
      <MarkdownRenderer
        text={readme}
        cloudRenderers={{
          image: cloudImageRenderer,
        }}
        className="markdown-format community-templates--readme"
      />
    )
  }
}

const mstp = (state: AppState, props: any) => {
  const templateDetails = getTemplateNameFromUrl(props.url)
  return {
    directory: templateDetails.directory,
    name: templateDetails.name,
    readme:
      state.resources.templates.communityTemplateReadmeCollection[
        templateDetails.name
      ],
  }
}

const mdtp = {
  fetchAndSetReadme,
}

const connector = connect(mstp, mdtp)

export const CommunityTemplateReadme = connector(
  CommunityTemplateReadmeUnconnected
)
