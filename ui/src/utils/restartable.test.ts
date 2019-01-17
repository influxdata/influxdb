import {restartable} from 'src/utils/restartable'
import {CancellationError} from 'src/types/promises'

describe('restartable', () => {
  test('with three concurrent promises', async () => {
    let n = 0

    const f = () => new Promise(res => setTimeout(() => res(n++), 50))
    const restartableF = restartable(f)

    const successMock = jest.fn()
    const errorMock = jest.fn()

    const g = async () => {
      try {
        const result = await restartableF()

        successMock(result)
      } catch (e) {
        if (e instanceof CancellationError) {
          errorMock()
        }
      }
    }

    g()
    g()
    g()

    await new Promise(res => setTimeout(res, 200))

    expect(successMock).toHaveBeenCalledTimes(1)
    expect(successMock.mock.calls[0][0]).toEqual(2)

    expect(errorMock).toHaveBeenCalledTimes(2)
  })

  test('with two sequential promises', async () => {
    let n = 0

    const f = () => new Promise(res => setTimeout(() => res(n++), 50))
    const restartableF = restartable(f)

    const successMock = jest.fn()
    const errorMock = jest.fn()

    const g = async () => {
      try {
        const result = await restartableF()

        successMock(result)
      } catch (e) {
        if (e instanceof CancellationError) {
          errorMock()
        }
      }
    }

    g()

    await new Promise(res => setTimeout(res, 200))

    g()

    await new Promise(res => setTimeout(res, 200))

    expect(successMock).toHaveBeenCalledTimes(2)
    expect(successMock.mock.calls[0][0]).toEqual(0)
    expect(successMock.mock.calls[1][0]).toEqual(1)

    expect(errorMock).toHaveBeenCalledTimes(0)
  })
})
