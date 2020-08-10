import {mocked} from 'ts-jest/utils'
jest.mock('src/client')

import {postTemplatesApply as postTemplatesApplyMock} from 'src/client'

import {reviewTemplate} from 'src/templates/api/'

describe('templates api calls', () => {
  it('reviews a template successfully', async () => {
    const orgID = '1234'
    const templateUrl = 'http://example.com'

    mocked(postTemplatesApplyMock).mockImplementation(() => {
      return Promise.resolve({
        status: 200,
        data: {
          foo: 'hi!'
        }
      })
    })

    const summary = await reviewTemplate(orgID, templateUrl)
    const [mockArguments] = mocked(postTemplatesApplyMock).mock.calls[0]

    expect(mockArguments.data.dryRun).toBeTruthy()
    expect(mockArguments.data.orgID).toBe(orgID)
    expect(mockArguments.data.remotes[0].url).toBe(templateUrl)
  })

  it ('reviews a template unsuccessfully', async () => {
    const orgID = '1234'
    const templateUrl = 'http://example.com'

    mocked(postTemplatesApplyMock).mockImplementation(() => {
      return Promise.resolve({
        status: 404,
        data: {
          message: 'whoops'
        }
      })
    })

    try {
      await reviewTemplate(orgID, templateUrl)
    } catch (error) {
      expect(error.message).toBe('whoops')
    }

  })

//   it('installTemplate', () => {
//     mocked(postTemplatesApplyMock).mockImplementation(() => {
//       return Promise.resolve({
//         status: 200,
//         data: {
//           foo: 'hi!'
//         }
//       })

        // // TODO: test for dry-run being false
//     })

//   })

//   it('fetchStacks', () => {

//   })

//   it('deleteStack', () => {

//   })

//   it('updateStackName', () => {

//   })
})
