import React, {SFC} from 'react'

interface Props {
  title: string
  altText?: string
}

const PageTitle: SFC<Props> = ({title, altText}) => (
  <h1 className="page--title" title={altText}>
    {title}
  </h1>
)

export default PageTitle
