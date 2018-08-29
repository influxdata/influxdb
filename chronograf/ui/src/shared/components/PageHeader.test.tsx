import React from 'react'
import {shallow} from 'enzyme'

import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'

describe('PageHeader', () => {
  it('should throw an error if neither titleText nor titleComponents is supplied', () => {
    expect(() => shallow(<PageHeader />)).toThrow(
      'PageHeader requires either titleText or titleComponents prop'
    )
  })
})
