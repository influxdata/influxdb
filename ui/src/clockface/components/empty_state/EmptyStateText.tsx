// Libraries
import React, {SFC, Fragment} from 'react'
import _ from 'lodash'
import uuid from 'uuid'

interface Props {
  text: string
  highlightWords?: string | string[]
}

const highlighter = (
  text: string,
  highlightWords: string | string[]
): JSX.Element[] => {
  const splitString = text.replace(/[\\][n]/g, 'LINEBREAK').split(' ')

  return splitString.map(word => {
    if (_.includes(highlightWords, word)) {
      return <em key={uuid.v4()}>{`${word}`}</em>
    }

    if (word === 'LINEBREAK') {
      return <br key={uuid.v4()} />
    }

    if (word === 'SPACECHAR') {
      return <Fragment key={uuid.v4()}>&nbsp;</Fragment>
    }

    return <Fragment key={uuid.v4()}>{`${word} `}</Fragment>
  })
}

const EmptyStateText: SFC<Props> = ({text, highlightWords}) => (
  <h4 className="empty-state--text">{highlighter(text, highlightWords)}</h4>
)

export default EmptyStateText
