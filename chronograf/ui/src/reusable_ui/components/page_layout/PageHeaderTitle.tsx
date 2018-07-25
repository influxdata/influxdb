import React, {SFC} from 'react'

interface Props {
  title: string
}

const PageTitle: SFC<Props> = ({title}) => (
  <h1 className="page-header--title">{title}</h1>
)

export default PageTitle
